package cache

import (
	"context"
	"time"
)

// Client interface to communicate with cache storage.
type Client interface {
	Set(ctx context.Context, key string, value interface{}) error
	HSet(ctx context.Context, key string, values interface{}) error
	Get(ctx context.Context, key string) (interface{}, error)
	HGetAll(ctx context.Context, key string) ([]interface{}, error)
	Expire(ctx context.Context, key string, expiration time.Duration) error
	Ping(ctx context.Context) error
}
