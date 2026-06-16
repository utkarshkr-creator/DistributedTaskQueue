package redis

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

type RedisStore struct {
	rdb     *redis.Client
	holdTTL time.Duration
}

func NewRedisStore(ctx context.Context, cfg Config, holdTTL time.Duration) (*RedisStore, error) {
	rdb := redis.NewClient(&redis.Options{
		Addr:     cfg.Address,
		Password: cfg.Password,
		DB:       cfg.DB,
		Protocol: 2,
	})
	if err := rdb.Ping(ctx).Err(); err != nil {
		rdb.Close()
		return nil, fmt.Errorf("failed to connect to Redis: %w", err)
	}
	fmt.Printf("Connected to Redis at %s, DB: %d\n", cfg.Address, cfg.DB)
	return &RedisStore{
		rdb:     rdb,
		holdTTL: holdTTL,
	}, nil
}

func (s *RedisStore) Close() error {
	return s.rdb.Close()
}

func (s *RedisStore) EnqueueTask(ctx context.Context, queueName string, taskId string) error {
	err := s.rdb.LPush(ctx, queueName, taskId).Err()
	if err != nil {
		return fmt.Errorf("failed to enqueue task %s: %w", taskId, err)
	}
	return nil
}

func (s *RedisStore) ConsumeTask(ctx context.Context, queueName string) (string, error) {
	inflightKey := queueName + ":inflight"
	result, err := s.rdb.BLMove(ctx, queueName, inflightKey, "RIGHT", "LEFT", 0).Result()
	if err == redis.Nil {
		return "", nil // No task available
	}
	if err != nil {
		return "", fmt.Errorf("failed to consume task vis brpoplpush: %w", err)
	}
	return result, nil
}

func (s *RedisStore) AcknowledgeTask(ctx context.Context, queueName string, taskId string) error {
	inflightKey := queueName + ":inflight"
	err := s.rdb.LRem(ctx, inflightKey, 0, taskId).Err()
	if err != nil {
		return fmt.Errorf("failed to acknowledge task %s: %w", taskId, err)
	}
	return nil
}
