// Package simsync tracks which clients are viewing which resources,
// and fans out update messages to just those clients when a resource changes.
//
// It does not store your data. Your database remains the source of truth.
// simsync only keeps viewer presence (who looks at what) plus pub/sub
// delivery, so the Go server stays stateless when backed by Redis.
//
// Typical flow:
//
//  1. Track when serving a page/fragment: client X is viewing resource R.
//  2. Subscribe to the client's private channel while its SSE connection is open.
//  3. Invalidate after you changed R in your storage: every current viewer gets msg.
//  4. UntrackAll when the SSE connection closes.
//
// Key layout (prefix defaults to "simsync"):
//
//	prefix:dep:<resource>   hash  clientID -> last-seen unix seconds
//	prefix:client:<clientID> set   of resource keys this client views
//	prefix:stream:<clientID> pub/sub channel for this client's updates
package simsync

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// StreamMessage is the unit delivered to viewers of a resource.
// Resource names the changed resource, Value carries the new state
// (or event data) as raw JSON. Use NewStreamMessage to build one,
// DecodeValue (or a handler Registry) to read Value back.
type StreamMessage struct {
	Resource string          `json:"resource"`
	Value    json.RawMessage `json:"value,omitempty"`
}

// NewStreamMessage builds a StreamMessage by JSON-marshaling v.
// Use it for markup-style updates where the payload must decode into
// the Markup[T] type registered for the resource. For event-style
// updates (Event profile, client refetches), use NewEventMessage instead.
func NewStreamMessage[T any](r string, v T) (StreamMessage, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return StreamMessage{}, err
	}
	return StreamMessage{Resource: r, Value: raw}, nil
}

// NewEventMessage builds a StreamMessage with no payload, for event-style
// updates where the client refetches itself (see htmx Event profile).
// It never fails, so there is no error return — unlike NewStreamMessage.
func NewEventMessage(resource string) StreamMessage {
	return StreamMessage{Resource: resource}
}

// DecodeValue JSON-unmarshals m.Value into T.
// The caller must know the payload type for the resource.
func DecodeValue[T any](m StreamMessage) (T, error) {
	var v T
	err := json.Unmarshal(m.Value, &v)
	return v, err
}

// MarshalBinary encodes m as JSON so backends like go-redis can
// Publish a StreamMessage directly (go-redis uses BinaryMarshaler
// for the message payload). The subscriber side decodes with json.Unmarshal.
func (m StreamMessage) MarshalBinary() (data []byte, err error) {
	return json.Marshal(m)
}

// PubSub delivers messages to per-client channels.
// Subscribe returns a channel that closes when the subscription ends;
// the returned cleanup func must be called when the client disconnects.
type PubSub interface {
	Subscribe(ctx context.Context, channel string) (<-chan StreamMessage, func())
	Publish(ctx context.Context, channel string, msg StreamMessage) error
}

// Storer is the storage surface the Engine needs. It is intentionally
// Redis-shaped (hash + set ops): a custom backend must emulate hash/set
// semantics even if the underlying store is different.
type Storer interface {
	HSet(ctx context.Context, key string, values ...any) error
	SAdd(ctx context.Context, key string, members ...any) error
	HKeys(ctx context.Context, key string) ([]string, error)
	SMembers(ctx context.Context, key string) ([]string, error)
	HDel(ctx context.Context, key string, fields ...string) error
	Del(ctx context.Context, keys ...string) error
	Expire(ctx context.Context, key string, expiration time.Duration) error
}

// Backend combines viewer storage with pub/sub delivery.
// Swap it to change where state lives (e.g. Redis vs in-memory).
type Backend interface {
	Storer
	PubSub
}

// Engine tracks viewers and fans out invalidations through a Backend.
// All viewer state lives in the Backend, so the Go process itself
// holds no viewer state and any replica can Track/Invalidate.
type Engine struct {
	// Backend is exported for custom handlers that need direct access
	// (e.g. Subscribe to a channel). Prefer SubscribeClient when you
	// just need the calling client's own update stream.
	Backend    Backend
	prefix     string
	defaultTTL time.Duration
}

// New builds an Engine over b.
// ttl is dead-viewer garbage collection: Track refreshes it on every view.
// Pass 0 (or negative) to default to 1 hour. If a client stays connected
// without re-Tracking longer than ttl, its keys can expire while the SSE
// stream is still open and it will silently stop receiving updates — keep
// ttl longer than your longest idle view, or re-Track as a heartbeat.
// n namespaces all keys/channels (prefix). Empty defaults to "simsync".
func New(b Backend, ttl time.Duration, n string) *Engine {
	if ttl <= 0 {
		ttl = 1 * time.Hour
	}
	if n == "" {
		n = "simsync"
	}
	return &Engine{
		prefix:     n,
		defaultTTL: ttl,
		Backend:    b,
	}
}

// Track marks clientID as currently viewing resource.
// Call it when serving the page/fragment that depends on the resource.
//
// It writes two entries: resource -> client (hash) and client -> resource
// (set, reverse index for cleanup), then refreshes both TTLs. The hash value
// is last-seen unix seconds — informational only (debugging), the viewer
// list is derived from hash keys. Expire failures are returned, not ignored,
// because without TTL dead viewers never get garbage-collected.
func (e *Engine) Track(ctx context.Context, clientID string, resource string) error {
	resourceKey := e.createResourceKey(resource)

	err := e.Backend.HSet(ctx, resourceKey, clientID, time.Now().Unix())
	if err != nil {
		return fmt.Errorf("failed to track dependency: %w", err)
	}

	clientLookupKey := e.createClientLookupKey(clientID)
	err = e.Backend.SAdd(ctx, clientLookupKey, resourceKey)
	if err != nil {
		return fmt.Errorf("failed to track client reverse index: %w", err)
	}

	if err := e.Backend.Expire(ctx, resourceKey, e.defaultTTL); err != nil {
		return fmt.Errorf("failed to set ttl on resource key: %w", err)
	}
	if err := e.Backend.Expire(ctx, clientLookupKey, e.defaultTTL); err != nil {
		return fmt.Errorf("failed to set ttl on client lookup key: %w", err)
	}
	return nil
}

// UntrackAll removes clientID from every resource it was viewing and deletes
// its reverse index. Call it when the client's update stream disconnects
// (SSE close). Use a fresh context (e.g. context.Background()), not the
// request context — it is already cancelled at that point.
//
// Cleanup continues past individual HDel failures; all errors are joined
// and returned together. Missing client (no resources) returns nil.
func (e *Engine) UntrackAll(ctx context.Context, clientID string) error {
	clientLookupKey := e.createClientLookupKey(clientID)

	// 1. Find all resources this specific client was looking at
	resources, err := e.Backend.SMembers(ctx, clientLookupKey)
	if err != nil {
		return fmt.Errorf("failed to fetch client resources: %w", err)
	}
	if len(resources) == 0 {
		return nil
	}

	// 2. Remove this client from all of those resource hashes
	var errs []error
	for _, resourceKey := range resources {
		if err := e.Backend.HDel(ctx, resourceKey, clientID); err != nil {
			errs = append(errs, fmt.Errorf("failed to untrack %q from %q: %w", clientID, resourceKey, err))
		}
	}

	// 3. Delete the client's reverse index key
	if err := e.Backend.Del(ctx, clientLookupKey); err != nil {
		errs = append(errs, fmt.Errorf("failed to delete client lookup key: %w", err))
	}
	return errors.Join(errs...)
}

// Invalidate fans out msg to every client currently tracking msg.Resource.
// Call it after you changed the resource in your source of truth.
//
// Delivery is at-most-once with no persistence or replay: clients that are
// not tracking right now get nothing, and there is no backlog for late
// joiners. No current viewers is a successful no-op.
//
// Fan-out is one Publish per viewer (O(viewers)), executed sequentially.
// A failure for one client does not stop the rest; all publish errors are
// joined and returned together, so an error means partial delivery.
func (e *Engine) Invalidate(ctx context.Context, msg StreamMessage) error {
	resourceKey := e.createResourceKey(msg.Resource)

	// 1. Get everyone watching this specific resource
	clientIDs, err := e.Backend.HKeys(ctx, resourceKey)
	if err != nil {
		return fmt.Errorf("failed to fetch dependency viewers: %w", err)
	}

	if len(clientIDs) == 0 {
		return nil // No one online cares right now, safely skip broadcasting
	}

	// 2. Publish the message to each viewer's private update channel
	var errs []error
	for _, clientID := range clientIDs {
		userChannel := e.GetUserChannel(clientID)
		if err := e.Backend.Publish(ctx, userChannel, msg); err != nil {
			errs = append(errs, fmt.Errorf("failed to publish to %q: %w", clientID, err))
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("failed to broadcast invalidations: %w", errors.Join(errs...))
	}

	return nil
}

// GetUserChannel returns the backend pub/sub channel name for a client's
// private update stream. Prefer SubscribeClient unless you need the name
// itself (custom subscribe logic).
func (e *Engine) GetUserChannel(clientID string) string {
	return fmt.Sprintf("%s:stream:%s", e.prefix, clientID)
}

// SubscribeClient subscribes to the calling client's private update channel.
// It is shorthand for Subscribe(GetUserChannel(clientID)).
// Call the returned cleanup func when the connection closes.
func (e *Engine) SubscribeClient(ctx context.Context, clientID string) (<-chan StreamMessage, func()) {
	return e.Backend.Subscribe(ctx, e.GetUserChannel(clientID))
}

func (e *Engine) createResourceKey(resource string) string {
	return fmt.Sprintf("%s:dep:%s", e.prefix, resource)
}

func (e *Engine) createClientLookupKey(clientID string) string {
	return fmt.Sprintf("%s:client:%s", e.prefix, clientID)
}
