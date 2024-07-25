package redis

import (
	"context"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/pkg/errors"
	"go.uber.org/zap"

	"github.com/polshe-v/microservices_common/pkg/cache"
	"github.com/polshe-v/microservices_common/pkg/closer"
	"github.com/polshe-v/microservices_common/pkg/logger"
)

var _ cache.Client = (*redisClient)(nil)

type handler func(ctx context.Context, conn redis.Conn) error

type redisClient struct {
	pool              *redis.Pool
	connectionTimeout time.Duration
}

// NewClient creates client for Redis communication.
func NewClient(pool *redis.Pool, connectionTimeout time.Duration) *redisClient {
	return &redisClient{
		pool:              pool,
		connectionTimeout: connectionTimeout,
	}
}

func (c *redisClient) Set(ctx context.Context, key string, value interface{}) error {
	err := c.execute(ctx, func(_ context.Context, conn redis.Conn) error {
		_, err := conn.Do("SET", redis.Args{key}.Add(value)...)
		if err != nil {
			return err
		}
		return nil
	})

	if err != nil {
		return err
	}

	return nil
}

func (c *redisClient) HSet(ctx context.Context, key string, values interface{}) error {
	err := c.execute(ctx, func(_ context.Context, conn redis.Conn) error {
		_, err := conn.Do("HSET", redis.Args{key}.AddFlat(values)...)
		if err != nil {
			return err
		}
		return nil
	})

	if err != nil {
		return err
	}

	return nil
}

func (c *redisClient) Get(ctx context.Context, key string) (interface{}, error) {
	var value interface{}
	err := c.execute(ctx, func(_ context.Context, conn redis.Conn) error {
		var errEx error
		value, errEx = conn.Do("GET", key)
		if errEx != nil {
			return errEx
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	return value, nil
}

func (c *redisClient) HGetAll(ctx context.Context, key string) ([]interface{}, error) {
	var values []interface{}
	err := c.execute(ctx, func(_ context.Context, conn redis.Conn) error {
		var errEx error
		values, errEx = redis.Values(conn.Do("HGETALL", key))
		if errEx != nil {
			return errEx
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	return values, nil
}

func (c *redisClient) Expire(ctx context.Context, key string, expiration time.Duration) error {
	err := c.execute(ctx, func(_ context.Context, conn redis.Conn) error {
		_, err := conn.Do("EXPIRE", key, int(expiration.Seconds()))
		if err != nil {
			return err
		}
		return nil
	})

	if err != nil {
		return err
	}

	return nil
}

func (c *redisClient) Ping(ctx context.Context) error {
	err := c.execute(ctx, func(_ context.Context, conn redis.Conn) error {
		_, err := conn.Do("PING")
		if err != nil {
			return err
		}
		return nil
	})

	if err != nil {
		return err
	}

	return nil
}

func (c *redisClient) execute(ctx context.Context, handler handler) error {
	conn, err := c.getConnect(ctx)
	if err != nil {
		return err
	}

	closer.Add(func() error {
		err = conn.Close()
		if err != nil {
			logger.Error("failed to close redis connection: ", zap.Error(err))
		}
		return nil
	})

	err = handler(ctx, conn)
	if err != nil {
		return err
	}

	return nil
}

func (c *redisClient) getConnect(ctx context.Context) (redis.Conn, error) {
	getConnTimeoutCtx, cancel := context.WithTimeout(ctx, c.connectionTimeout)
	closer.Add(func() error {
		cancel()
		return nil
	})

	conn, err := c.pool.GetContext(getConnTimeoutCtx)
	if err != nil {
		_ = conn.Close()
		return nil, errors.Errorf("failed to connect to redis: %v", err)
	}

	return conn, nil
}
