package simsync

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

// structure

// here we use HashSet so each resource key creates separate kv store
// prefix:dep:dependency - actual resource_key
// resource_key -> clientID - storing that this client is actively using this resource

// prefix:client:clientID - clientLookupKey
// clientLookupKey -> resource_key, so we keep track of what exactly client is looking at, easier to then clean everything up

// pub/sub
// prefix:stream:clientID - channel topic, which we can send to, and if client is active then its subscribed to it (in SSE handler)

type StreamMessage struct {
	Resource string `json:"resource"`
	Value    string `json:"value"`
}

// r - resource
//
// v - new value
func NewStreamMessage(r, v string) StreamMessage {
	return StreamMessage{
		Resource: r,
		Value:    v,
	}
}

func (m StreamMessage) MarshalBinary() (data []byte, err error) {
	return json.Marshal(m)
}

type PubSub interface {
	Subscribe(ctx context.Context, channel string) (<-chan StreamMessage, func())
	Publish(ctx context.Context, channel string, msg StreamMessage) error
}
type Storer interface {
	HSet(ctx context.Context, key string, values ...any) error
	SAdd(ctx context.Context, key string, members ...any) error
	HKeys(ctx context.Context, key string) ([]string, error)
	SMembers(ctx context.Context, key string) ([]string, error)
	HDel(ctx context.Context, key string, fields ...string) error
	Del(ctx context.Context, keys ...string) error
	Expire(ctx context.Context, key string, expiration time.Duration) error
}

type Backend interface {
	Storer
	PubSub
}

type Engine struct {
	Backend    Backend
	prefix     string
	defaultTTL time.Duration
}

// ttl will default to 1 hour if passed 0
//
// namespace will default to "simsync" if provided empty string
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

// Track registers that a specific client/user is actively viewing a resource.
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

	e.Backend.Expire(ctx, resourceKey, e.defaultTTL)
	e.Backend.Expire(ctx, clientLookupKey, e.defaultTTL)
	return nil
}

// UntrackAll completely scrubs a client from all dependencies when they close their connection.
func (e *Engine) UntrackAll(ctx context.Context, clientID string) error {
	clientLookupKey := e.createClientLookupKey(clientID)

	// 1. Find all resources this specific client was looking at
	resources, err := e.Backend.SMembers(ctx, clientLookupKey)
	if err != nil || len(resources) == 0 {
		return err
	}

	// 2. Remove this client from all of those dependency hashes
	for _, resourceKey := range resources {
		err = e.Backend.HDel(ctx, resourceKey, clientID)
	}

	// 3. Delete the client's inverse index key
	err = e.Backend.Del(ctx, clientLookupKey)
	return err
}

// Invalidate finds everyone currently tracking a dependency and triggers an HTMX update event.
func (e *Engine) Invalidate(ctx context.Context, msg StreamMessage) error {
	resourceKey := e.createResourceKey(msg.Resource)

	// 1. Get everyone watching this specific dependency
	clientIDs, err := e.Backend.HKeys(ctx, resourceKey)
	if err != nil {
		return fmt.Errorf("failed to fetch dependency viewers: %w", err)
	}

	if len(clientIDs) == 0 {
		return nil // No one online cares right now, safely skip broadcasting
	}

	// 2. Broadcast the HTMX event name to each active user's private SSE update channel
	for _, clientID := range clientIDs {
		userChannel := e.GetUserChannel(clientID)
		err = e.Backend.Publish(ctx, userChannel, msg)
	}

	if err != nil {
		return fmt.Errorf("failed to broadcast invalidations: %w", err)
	}

	return nil
}

// GetUserChannel returns the correct Redis Pub/Sub topic name for a specific client.
// Use this inside your main HTTP SSE handler to subscribe the connection to the right channel.
func (e *Engine) GetUserChannel(clientID string) string {
	return fmt.Sprintf("%s:stream:%s", e.prefix, clientID)
}

func (e *Engine) createResourceKey(resource string) string {
	return fmt.Sprintf("%s:dep:%s", e.prefix, resource)
}

func (e *Engine) createClientLookupKey(clientID string) string {
	return fmt.Sprintf("%s:client:%s", e.prefix, clientID)
}
