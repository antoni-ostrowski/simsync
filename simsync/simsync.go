package simsync

import (
	"context"
	"encoding"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// structure

// here we use HashSet so each resource key creates separate kv store
// prefix:dep:dependency - actual resource_key
// resource_key -> clientID - storing that this client is actively using this resource

// prefix:client:clientID - clientLookupKey
// clientLookupKey -> resource_key, so we keep track of what exactly client is looking at, easier to then clean everything up

// pub/sub
// prefix:stream:clientID - channel topic, which we can send to, and if client is active then its subscribed to it (in SSE handler)

type Engine struct {
	redis      *redis.Client
	prefix     string
	defaultTTL time.Duration
}

type Config struct {
	RedisClient *redis.Client
	Namespace   string
	DefaultTTL  time.Duration
}

func New(cfg Config) *Engine {
	if cfg.Namespace == "" {
		cfg.Namespace = "simsync"
	}
	if cfg.DefaultTTL == 0 {
		cfg.DefaultTTL = time.Hour
	}

	return &Engine{
		redis:      cfg.RedisClient,
		prefix:     cfg.Namespace,
		defaultTTL: cfg.DefaultTTL,
	}
}

type Frontend interface {
	Method() error
}

// Track registers that a specific client/user is actively viewing a resource.
func (e *Engine) Track(ctx context.Context, clientID string, resource string) error {
	resourceKey := e.createResourceKey(resource)

	err := e.redis.HSet(ctx, resourceKey, clientID, time.Now().Unix()).Err()
	if err != nil {
		return fmt.Errorf("failed to track dependency: %w", err)
	}

	clientLookupKey := e.createClientLookupKey(clientID)
	err = e.redis.SAdd(ctx, clientLookupKey, resourceKey).Err()
	if err != nil {
		return fmt.Errorf("failed to track client reverse index: %w", err)
	}

	e.redis.Expire(ctx, resourceKey, e.defaultTTL)
	e.redis.Expire(ctx, clientLookupKey, e.defaultTTL)
	return nil
}

// UntrackAll completely scrubs a client from all dependencies when they close their connection.
func (e *Engine) UntrackAll(ctx context.Context, clientID string) error {
	clientLookupKey := e.createClientLookupKey(clientID)

	// 1. Find all resources this specific client was looking at
	resources, err := e.redis.SMembers(ctx, clientLookupKey).Result()
	if err != nil || len(resources) == 0 {
		return err
	}

	// 2. Remove this client from all of those dependency hashes
	pipe := e.redis.Pipeline()
	for _, resourceKey := range resources {
		pipe.HDel(ctx, resourceKey, clientID)
	}

	// 3. Delete the client's inverse index key
	pipe.Del(ctx, clientLookupKey)

	_, err = pipe.Exec(ctx)
	return err
}

// Invalidate finds everyone currently tracking a dependency and triggers an HTMX update event.
func (e *Engine) Invalidate(ctx context.Context, resource string, msg encoding.BinaryMarshaler) error {
	resourceKey := e.createResourceKey(resource)

	// 1. Get everyone watching this specific dependency
	clientIDs, err := e.redis.HKeys(ctx, resourceKey).Result()
	if err != nil {
		return fmt.Errorf("failed to fetch dependency viewers: %w", err)
	}

	if len(clientIDs) == 0 {
		return nil // No one online cares right now, safely skip broadcasting
	}

	// 2. Broadcast the HTMX event name to each active user's private SSE update channel
	pipe := e.redis.Pipeline()
	for _, clientID := range clientIDs {
		userChannel := e.GetUserChannel(clientID)
		pipe.Publish(ctx, userChannel, msg)
	}

	_, err = pipe.Exec(ctx)
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
