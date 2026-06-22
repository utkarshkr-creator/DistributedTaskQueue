package redis

import (
	"context"
	"distributed-queue/pkg/config"
	"distributed-queue/pkg/redis/metrics"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

type RedisStore struct {
	rdb     *redis.Client
	holdTTL time.Duration
}

func NewRedisStore(ctx context.Context, cfg config.RedisConfig, holdTTL time.Duration) (*RedisStore, error) {
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
	slog.Info("connected to Redis", "addr", cfg.Address, "db", cfg.DB)
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
		metrics.StoreErrors.WithLabelValues(queueName, "EnqueueTask").Inc()
		return fmt.Errorf("[PRODUCER] failed to enqueue task %s: %w", taskId, err)
	}
	metrics.TasksEnqueued.WithLabelValues(queueName).Inc()
	return nil
}

// ConsumeTask blocks until a task is available, moves it from queueName into
// the inflight list, and records the dequeue time so the janitor can detect
// stuck tasks. If recording the timestamp fails, the task is intentionally
// pushed back onto the queue rather than left untracked in inflight — an
// untracked inflight task is invisible to SweepAbondonedTasks and would
// never be retried or DLQ'd.
func (s *RedisStore) ConsumeTask(ctx context.Context, queueName string) (string, error) {
	inflightKey := queueName + ":inflight"
	timeHashKey := queueName + ":timestamps"

	taskId, err := s.rdb.BLMove(ctx, queueName, inflightKey, "RIGHT", "LEFT", 0).Result()
	if err == redis.Nil {
		return "", nil // No task available
	}
	if err != nil {
		metrics.StoreErrors.WithLabelValues(queueName, "ConsumeTask").Inc()
		return "", fmt.Errorf("[CONSUMER] failed to consume task via blmove: %w", err)
	}

	// Use Redis server time instead of local time to avoid clock skew
	// between machines.
	redisTime, err := s.rdb.Time(ctx).Result()
	if err != nil {
		metrics.StoreErrors.WithLabelValues(queueName, "ConsumeTask").Inc()
		slog.Error("[CONSUMER] failed to get Redis server time, returning task without inflight tracking", "taskId", taskId, "error", err)
		metrics.TasksConsumed.WithLabelValues(queueName).Inc()
		return taskId, nil
	}
	now := redisTime.Unix()

	if err := s.rdb.HSet(ctx, timeHashKey, taskId, strconv.FormatInt(now, 10)).Err(); err != nil {
		metrics.StoreErrors.WithLabelValues(queueName, "ConsumeTask").Inc()
		slog.Error("[CONSUMER] failed to record inflight timestamp, returning task to queue", "taskId", taskId, "error", err)
		pipe := s.rdb.TxPipeline()
		pipe.LRem(ctx, inflightKey, 1, taskId)
		pipe.LPush(ctx, queueName, taskId)
		if _, rerr := pipe.Exec(ctx); rerr != nil {
			// Worst case: task stays in inflight untracked. Surface this loudly
			// since it will require manual intervention or rely on a
			// time-based reconciliation job to recover.
			metrics.StoreErrors.WithLabelValues(queueName, "ConsumeTask").Inc()
			return "", fmt.Errorf("[CONSUMER] failed to record timestamp AND failed to requeue task %s, task is now untracked in inflight: %w", taskId, rerr)
		}
		return "", nil
	}

	metrics.TasksConsumed.WithLabelValues(queueName).Inc()
	return taskId, nil
}

func (s *RedisStore) AcknowledgeTask(ctx context.Context, queueName string, taskId string) error {
	inflightKey := queueName + ":inflight"
	timeHashKey := queueName + ":timestamps"
	retryHashKey := queueName + ":retries"

	pipe := s.rdb.TxPipeline()
	removed := pipe.LRem(ctx, inflightKey, 1, taskId)
	pipe.HDel(ctx, timeHashKey, taskId)
	pipe.HDel(ctx, retryHashKey, taskId)
	if _, err := pipe.Exec(ctx); err != nil {
		metrics.StoreErrors.WithLabelValues(queueName, "AcknowledgeTask").Inc()
		return fmt.Errorf("[ACK] failed to acknowledge task %s: %w", taskId, err)
	}
	if removed.Val() == 0 {
		// Not necessarily an error (e.g. ack arrived after a sweep already
		// moved this task back to the main queue) but worth knowing about.
		slog.Warn("[ACK] task was not found in inflight list when acknowledged", "taskId", taskId)
	}
	metrics.TasksAcknowledged.WithLabelValues(queueName).Inc()
	return nil
}

// GetAndIncrementRetryCount returns the number of prior retry attempts for
// taskId (0 if this is the first failure) and atomically increments the
// counter for next time. Callers use the returned value to decide whether
// to retry with backoff or give up and move the task to the DLQ.
func (s *RedisStore) GetAndIncrementRetryCount(ctx context.Context, queueName, taskId string) (int, error) {
	retryHashKey := queueName + ":retries"

	prior, err := s.rdb.HGet(ctx, retryHashKey, taskId).Result()
	if err != nil && err != redis.Nil {
		metrics.StoreErrors.WithLabelValues(queueName, "GetAndIncrementRetryCount").Inc()
		return 0, fmt.Errorf("[RETRY] failed to read retry count for task %s: %w", taskId, err)
	}
	count := 0
	if err == nil {
		count, err = strconv.Atoi(prior)
		if err != nil {
			slog.Warn("[RETRY] unparseable retry count, resetting to 0", "taskId", taskId, "value", prior)
			count = 0
		}
	}

	if err := s.rdb.HSet(ctx, retryHashKey, taskId, strconv.Itoa(count+1)).Err(); err != nil {
		metrics.StoreErrors.WithLabelValues(queueName, "GetAndIncrementRetryCount").Inc()
		return count, fmt.Errorf("[RETRY] failed to persist incremented retry count for task %s: %w", taskId, err)
	}
	return count, nil
}

func (s *RedisStore) SweepAbondonedTasks(ctx context.Context, queueName string) error {
	inflightKey := queueName + ":inflight"
	timeHashKey := queueName + ":timestamps"

	timestamps, err := s.rdb.HGetAll(ctx, timeHashKey).Result()
	if err != nil {
		metrics.StoreErrors.WithLabelValues(queueName, "SweepAbondonedTasks").Inc()
		return fmt.Errorf("[JANITOR] failed to get task timestamps: %w", err)
	}
	redisTime, err := s.rdb.Time(ctx).Result()
	if err != nil {
		metrics.StoreErrors.WithLabelValues(queueName, "SweepAbondonedTasks").Inc()
		return fmt.Errorf("[JANITOR] failed to get Redis server time: %w", err)
	}
	now := redisTime.Unix()
	for taskId, tsStr := range timestamps {
		startTime, err := strconv.ParseInt(tsStr, 10, 64)
		if err != nil {
			slog.Warn("[JANITOR] skipping task with unparseable timestamp", "taskId", taskId, "timestamp", tsStr)
			continue
		}
		if now-startTime > int64(s.holdTTL.Seconds()) {
			slog.Warn("[JANITOR] task held too long, moving back to queue", "taskId", taskId, "heldSeconds", now-startTime)
			pipe := s.rdb.TxPipeline()
			removed := pipe.LRem(ctx, inflightKey, 1, taskId)
			pipe.HDel(ctx, timeHashKey, taskId)
			pipe.LPush(ctx, queueName, taskId)
			if _, err := pipe.Exec(ctx); err != nil {
				metrics.StoreErrors.WithLabelValues(queueName, "SweepAbondonedTasks").Inc()
				return fmt.Errorf("[JANITOR] failed to sweep abandoned task %s: %w", taskId, err)
			}
			if removed.Val() == 0 {
				// We pushed a duplicate into the main queue even though the
				// task wasn't actually in inflight (e.g. already acked
				// concurrently). Flag it — this is how double-processing
				// happens.
				slog.Warn("[JANITOR] requeued task that was not present in inflight list, possible duplicate", "taskId", taskId)
			}
			metrics.TasksSwept.WithLabelValues(queueName).Inc()
		}
	}
	return nil
}

func (s *RedisStore) RetryTaskWithBackoff(ctx context.Context, queueName string, taskId string, currentRetry int) error {
	inflightKey := queueName + ":inflight"
	timeHashKey := queueName + ":timestamps"
	delayedKey := queueName + ":delayed"

	redisTime, err := s.rdb.Time(ctx).Result()
	if err != nil {
		metrics.StoreErrors.WithLabelValues(queueName, "RetryTaskWithBackoff").Inc()
		return fmt.Errorf("[PRODUCER] failed to get central clock time: %w", err)
	}

	now := redisTime.Unix()

	delayInSec := int64(1 << uint(currentRetry))
	executionTimestamp := now + delayInSec

	pipe := s.rdb.TxPipeline()
	removed := pipe.LRem(ctx, inflightKey, 1, taskId)
	pipe.HDel(ctx, timeHashKey, taskId)
	pipe.ZAdd(ctx, delayedKey, redis.Z{
		Score:  float64(executionTimestamp),
		Member: taskId,
	})

	if _, err = pipe.Exec(ctx); err != nil {
		metrics.StoreErrors.WithLabelValues(queueName, "RetryTaskWithBackoff").Inc()
		return fmt.Errorf("[PRODUCER] failed to move task %s to retry queue: %w", taskId, err)
	}
	if removed.Val() == 0 {
		slog.Warn("[PRODUCER] retried task was not present in inflight list", "taskId", taskId)
	}

	metrics.TasksRetried.WithLabelValues(queueName).Inc()
	slog.Info("[PRODUCER] task moved to retry queue with backoff", "taskId", taskId, "retryCount", currentRetry, "delaySeconds", delayInSec)
	return nil
}

func (s *RedisStore) MoveToDeadLetterQueue(ctx context.Context, queueName, taskId string) error {
	inflightKey := queueName + ":inflight"
	timeHashKey := queueName + ":timestamps"
	retryHashKey := queueName + ":retries"
	dlqKey := queueName + ":dlq"

	pipe := s.rdb.TxPipeline()
	removed := pipe.LRem(ctx, inflightKey, 1, taskId)
	pipe.HDel(ctx, timeHashKey, taskId)
	pipe.HDel(ctx, retryHashKey, taskId)
	pipe.LPush(ctx, dlqKey, taskId)

	if _, err := pipe.Exec(ctx); err != nil {
		metrics.StoreErrors.WithLabelValues(queueName, "MoveToDeadLetterQueue").Inc()
		return fmt.Errorf("[DLQ] failed to move task %s to dead letter queue: %w", taskId, err)
	}
	if removed.Val() == 0 {
		slog.Warn("[DLQ] dead-lettered task was not present in inflight list", "taskId", taskId)
	}
	metrics.TasksDeadLettered.WithLabelValues(queueName).Inc()
	slog.Info("[DLQ] task moved to dead letter queue after max retries", "taskId", taskId)
	return nil
}

// CollectDepths reads the current size of each queue-related Redis
// structure and updates the corresponding Prometheus gauges. Intended to be
// called periodically (e.g. every few seconds) by a dedicated goroutine —
// gauges reflect a snapshot of Redis state, not a discrete code event, so
// they can't be updated inline at the point of an operation.
func (s *RedisStore) CollectDepths(ctx context.Context, queueName string) error {
	inflightKey := queueName + ":inflight"
	delayedKey := queueName + ":delayed"
	dlqKey := queueName + ":dlq"

	queueLen, err := s.rdb.LLen(ctx, queueName).Result()
	if err != nil {
		metrics.StoreErrors.WithLabelValues(queueName, "CollectDepths").Inc()
		return fmt.Errorf("[METRICS] failed to read queue depth: %w", err)
	}
	inflightLen, err := s.rdb.LLen(ctx, inflightKey).Result()
	if err != nil {
		metrics.StoreErrors.WithLabelValues(queueName, "CollectDepths").Inc()
		return fmt.Errorf("[METRICS] failed to read inflight depth: %w", err)
	}
	delayedLen, err := s.rdb.ZCard(ctx, delayedKey).Result()
	if err != nil {
		metrics.StoreErrors.WithLabelValues(queueName, "CollectDepths").Inc()
		return fmt.Errorf("[METRICS] failed to read delayed depth: %w", err)
	}
	dlqLen, err := s.rdb.LLen(ctx, dlqKey).Result()
	if err != nil {
		metrics.StoreErrors.WithLabelValues(queueName, "CollectDepths").Inc()
		return fmt.Errorf("[METRICS] failed to read DLQ depth: %w", err)
	}

	metrics.QueueDepth.WithLabelValues(queueName).Set(float64(queueLen))
	metrics.InflightDepth.WithLabelValues(queueName).Set(float64(inflightLen))
	metrics.DelayedDepth.WithLabelValues(queueName).Set(float64(delayedLen))
	metrics.DLQDepth.WithLabelValues(queueName).Set(float64(dlqLen))
	return nil
}

// PolledDelayedTasks checks the delayed ZSET for any tasks whose execution
// timestamp has passed and moves them back onto the main queue.
func (s *RedisStore) PolledDelayedTasks(ctx context.Context, queueName string) error {
	delayedKey := queueName + ":delayed"

	redisTime, err := s.rdb.Time(ctx).Result()
	if err != nil {
		metrics.StoreErrors.WithLabelValues(queueName, "PolledDelayedTasks").Inc()
		return fmt.Errorf("[PRODUCER] failed to get central clock time: %w", err)
	}
	now := redisTime.Unix()

	// find all tasks scored between 0 and current time (inclusive)
	opt := redis.ZRangeArgs{
		Key:     delayedKey,
		Start:   "0",
		Stop:    strconv.FormatInt(now, 10),
		ByScore: true,
	}

	maturedTasks, err := s.rdb.ZRangeArgs(ctx, opt).Result()
	if err != nil {
		metrics.StoreErrors.WithLabelValues(queueName, "PolledDelayedTasks").Inc()
		return fmt.Errorf("[PRODUCER] failed to poll delayed tasks: %w", err)
	}

	movedCount := 0
	for _, taskId := range maturedTasks {
		// ZRangeArgs (read) and ZRem (delete) are separate round trips, so a
		// concurrent producer instance polling at the same time can observe
		// the same matured taskId before either of us deletes it. ZRem
		// itself is atomic and returns how many members it actually removed,
		// so we use that as a compare-and-delete guard: only the caller that
		// wins the ZRem gets to push the task onto the main queue. This is
		// what actually prevents duplicate pops — the inclusivity of the
		// ZRangeArgs score bound is unrelated to this race.
		removed, err := s.rdb.ZRem(ctx, delayedKey, taskId).Result()
		if err != nil {
			metrics.StoreErrors.WithLabelValues(queueName, "PolledDelayedTasks").Inc()
			return fmt.Errorf("[PRODUCER] failed to remove matured task %s from delayed set: %w", taskId, err)
		}
		if removed == 0 {
			// Another producer instance already claimed this task this cycle.
			slog.Info("[PRODUCER] matured task already claimed by another poller, skipping", "taskId", taskId)
			continue
		}

		if err := s.rdb.LPush(ctx, queueName, taskId).Err(); err != nil {
			metrics.StoreErrors.WithLabelValues(queueName, "PolledDelayedTasks").Inc()
			// We've already removed the task from the delayed set, so on
			// failure here it would be lost rather than just duplicated.
			// Put it back into the delayed set at its original due time so
			// the next poll cycle picks it up again instead of dropping it.
			if rerr := s.rdb.ZAdd(ctx, delayedKey, redis.Z{Score: float64(now), Member: taskId}).Err(); rerr != nil {
				return fmt.Errorf("[PRODUCER] failed to push matured task %s to main queue AND failed to restore it to delayed set, task is lost: push_err=%v restore_err=%w", taskId, err, rerr)
			}
			return fmt.Errorf("[PRODUCER] failed to push matured task %s to main queue, restored to delayed set: %w", taskId, err)
		}

		slog.Info("[PRODUCER] task ready to be retried, moved back to main queue", "taskId", taskId)
		movedCount++
	}

	slog.Info("[PRODUCER] polled delayed tasks", "movedCount", movedCount, "candidateCount", len(maturedTasks))
	return nil
}
