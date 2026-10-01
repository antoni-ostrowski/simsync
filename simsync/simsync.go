package simsync

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

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

// Track registers that a specific client/user is actively viewing a dependency resource.
func (e *Engine) Track(ctx context.Context, clientID string, dependency string) error {
	key := fmt.Sprintf("%s:dep:%s", e.prefix, dependency)

	// Store client ID with the current unix timestamp
	err := e.redis.HSet(ctx, key, clientID, time.Now().Unix()).Err()
	if err != nil {
		return fmt.Errorf("failed to track dependency: %w", err)
	}

	// Also maintain an inverse lookup index so we know exactly what a client is looking at
	// This makes disconnecting/cleaning up incredibly fast!
	clientLookupKey := fmt.Sprintf("%s:client:%s", e.prefix, clientID)
	err = e.redis.SAdd(ctx, clientLookupKey, dependency).Err()
	if err != nil {
		return fmt.Errorf("failed to track client reverse index: %w", err)
	}

	// Set safety expirations so stale data cleans up automatically over time
	e.redis.Expire(ctx, key, e.defaultTTL)
	e.redis.Expire(ctx, clientLookupKey, e.defaultTTL)
	return nil
}

// UntrackAll completely scrubs a client from all dependencies when they close their connection.
func (e *Engine) UntrackAll(ctx context.Context, clientID string) error {
	clientLookupKey := fmt.Sprintf("%s:client:%s", e.prefix, clientID)

	// 1. Find all dependencies this specific client was looking at
	dependencies, err := e.redis.SMembers(ctx, clientLookupKey).Result()
	if err != nil || len(dependencies) == 0 {
		return err
	}

	// 2. Remove this client from all of those dependency hashes
	pipe := e.redis.Pipeline()
	for _, dep := range dependencies {
		depKey := fmt.Sprintf("%s:dep:%s", e.prefix, dep)
		pipe.HDel(ctx, depKey, clientID)
	}

	// 3. Delete the client's inverse index key
	pipe.Del(ctx, clientLookupKey)

	_, err = pipe.Exec(ctx)
	return err
}

// Invalidate finds everyone currently tracking a dependency and triggers an HTMX update event.
func (e *Engine) Invalidate(ctx context.Context, dependency string, htmxEventName string) error {
	depKey := fmt.Sprintf("%s:dep:%s", e.prefix, dependency)

	// 1. Get everyone watching this specific dependency
	clientIDs, err := e.redis.HKeys(ctx, depKey).Result()
	if err != nil {
		return fmt.Errorf("failed to fetch dependency viewers: %w", err)
	}

	if len(clientIDs) == 0 {
		return nil // No one online cares right now, safely skip broadcasting
	}

	// 2. Broadcast the HTMX event name to each active user's private SSE update channel
	pipe := e.redis.Pipeline()
	for _, clientID := range clientIDs {
		userChannel := fmt.Sprintf("%s:stream:%s", e.prefix, clientID)
		pipe.Publish(ctx, userChannel, htmxEventName)
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
