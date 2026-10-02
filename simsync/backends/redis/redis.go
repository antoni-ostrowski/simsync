package redis

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/antoni-ostrowski/simsync/simsync"
	"github.com/redis/go-redis/v9"
)

type RedisBackend struct {
	rdb *redis.Client
}

func NewRedisBackend(rdb *redis.Client) *RedisBackend {
	return &RedisBackend{rdb}
}

func (r RedisBackend) HSet(ctx context.Context, key string, values ...any) error {
	return r.rdb.HSet(ctx, key, values).Err()
}
func (r RedisBackend) SAdd(ctx context.Context, key string, members ...any) error {
	return r.rdb.SAdd(ctx, key, members).Err()
}
func (r RedisBackend) HKeys(ctx context.Context, key string) ([]string, error) {
	return r.rdb.HKeys(ctx, key).Result()
}
func (r RedisBackend) SMembers(ctx context.Context, key string) ([]string, error) {
	return r.rdb.SMembers(ctx, key).Result()
}
func (r RedisBackend) HDel(ctx context.Context, key string, fields ...string) error {
	return r.rdb.HDel(ctx, key, fields...).Err()
}
func (r RedisBackend) Del(ctx context.Context, keys ...string) error {
	return r.rdb.Del(ctx, keys...).Err()
}
func (r RedisBackend) Expire(ctx context.Context, key string, expiration time.Duration) error {
	return r.rdb.Expire(ctx, key, expiration).Err()
}

func (r RedisBackend) Subscribe(ctx context.Context, channel string) (<-chan simsync.StreamMessage, func()) {
	pubsub := r.rdb.Subscribe(ctx, channel)
	out := make(chan simsync.StreamMessage, 20)
	go func() {
		defer close(out)
		for redisMsg := range pubsub.Channel() {
			var coreMsg simsync.StreamMessage

			if err := json.Unmarshal([]byte(redisMsg.Payload), &coreMsg); err != nil {
				slog.Error("failed to unmarshal pub/sub message", "error", err.Error(), "channel", redisMsg.Channel)
				continue
			}

			out <- coreMsg

		}

	}()

	cleanup := func() {
		pubsub.Close()
	}
	return out, cleanup
}

func (r RedisBackend) Publish(ctx context.Context, channel string, msg simsync.StreamMessage) error {
	err := r.rdb.Publish(ctx, channel, msg).Err()
	if err != nil {
		slog.Error("failed to publish message", "error", err.Error(), "channel", channel)
	}
	return err
}
