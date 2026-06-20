package main

import (
	"context"
	"distributed-queue/pkg/redis"
	"errors"
	"log/slog"
	"math/rand"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"
)

const (
	queueName  = "queue:video_processing"
	maxRetries = 5

	holdTTL          = 20 * time.Second
	janitorInterval  = 5 * time.Second
	delayedPollEvery = 2 * time.Second
)

// simulateProcessing stands in for real task work. It fails roughly 20% of
// the time so the retry/DLQ path is actually exercised end to end. Replace
// this with real processing logic; keep the ctx-cancellation check if real
// work is itself cancellable.
func simulateProcessing(ctx context.Context, taskId string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(2 * time.Second):
	}
	if rand.Intn(5) == 0 {
		return errors.New("simulated processing failure")
	}
	return nil
}

func runWorker(ctx context.Context, store *redis.RedisStore, wg *sync.WaitGroup) {
	defer wg.Done()
	slog.Info("worker thread started, waiting for tasks...")

	for {
		if ctx.Err() != nil {
			slog.Info("worker thread received shutdown signal, exiting...")
			return
		}

		// BLMove blocks until a task arrives or ctx is cancelled, so this
		// call itself is the idle wait — no extra polling sleep needed.
		taskId, err := store.ConsumeTask(ctx, queueName)
		if err != nil {
			if ctx.Err() != nil {
				// Cancellation during the blocking call surfaces as an error
				// here; treat it as shutdown, not a real failure.
				slog.Info("worker thread received shutdown signal, exiting...")
				return
			}
			slog.Error("failed to consume task", "error", err)
			time.Sleep(1 * time.Second)
			continue
		}
		if taskId == "" {
			continue
		}

		slog.Info("task consumed", "taskId", taskId)
		procErr := simulateProcessing(ctx, taskId)

		if procErr == nil {
			slog.Info("task processing completed", "taskId", taskId)
			if err := store.AcknowledgeTask(ctx, queueName, taskId); err != nil {
				// We stop immediately on shutdown per design, so a failed ack
				// here (including due to ctx cancellation) is left for the
				// janitor to recover on the next sweep after holdTTL expires.
				slog.Error("failed to acknowledge task, will be recovered by janitor", "taskId", taskId, "error", err)
			} else {
				slog.Info("task acknowledged", "taskId", taskId)
			}
			continue
		}

		// Processing failed. Decide retry vs dead-letter.
		slog.Warn("task processing failed", "taskId", taskId, "error", procErr)

		retryCount, rcErr := store.GetAndIncrementRetryCount(ctx, queueName, taskId)
		if rcErr != nil {
			slog.Error("failed to read/increment retry count, leaving task for janitor to recover", "taskId", taskId, "error", rcErr)
			continue
		}

		if retryCount >= maxRetries {
			if err := store.MoveToDeadLetterQueue(ctx, queueName, taskId); err != nil {
				slog.Error("failed to move task to DLQ, leaving for janitor to recover", "taskId", taskId, "error", err)
			} else {
				slog.Warn("task exceeded max retries, moved to DLQ", "taskId", taskId, "retryCount", retryCount)
			}
			continue
		}

		if err := store.RetryTaskWithBackoff(ctx, queueName, taskId, retryCount); err != nil {
			slog.Error("failed to schedule retry, leaving for janitor to recover", "taskId", taskId, "error", err)
		}
	}
}

func runProducer(ctx context.Context, store *redis.RedisStore, wg *sync.WaitGroup) {
	defer wg.Done()
	slog.Info("producer thread started, generating tasks...")

	taskCounter := 1
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			slog.Info("producer thread received shutdown signal, exiting...")
			return
		case <-ticker.C:
			taskId := "task_" + strconv.Itoa(taskCounter)
			if err := store.EnqueueTask(ctx, queueName, taskId); err != nil {
				slog.Error("failed to enqueue task", "taskId", taskId, "error", err)
				continue
			}
			slog.Info("task enqueued", "taskId", taskId)
			taskCounter++
		}
	}
}

func runJanitor(ctx context.Context, store *redis.RedisStore, wg *sync.WaitGroup) {
	defer wg.Done()
	slog.Info("janitor thread started, monitoring for abandoned tasks...")

	ticker := time.NewTicker(janitorInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			slog.Info("janitor thread received shutdown signal, exiting...")
			return
		case <-ticker.C:
			if err := store.SweepAbondonedTasks(ctx, queueName); err != nil {
				slog.Error("failed to sweep abandoned tasks", "error", err)
			} else {
				slog.Info("janitor sweep completed")
			}
		}
	}
}

// runDelayedPoller periodically moves matured retry-delayed tasks back onto
// the main queue. Without this, tasks scheduled via RetryTaskWithBackoff
// would sit in the delayed set forever and never be retried.
func runDelayedPoller(ctx context.Context, store *redis.RedisStore, wg *sync.WaitGroup) {
	defer wg.Done()
	slog.Info("delayed-task poller started")

	ticker := time.NewTicker(delayedPollEvery)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			slog.Info("delayed-task poller received shutdown signal, exiting...")
			return
		case <-ticker.C:
			if err := store.PolledDelayedTasks(ctx, queueName); err != nil {
				slog.Error("failed to poll delayed tasks", "error", err)
			}
		}
	}
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	cfg, err := redis.LoadConfig()
	if err != nil {
		slog.Error("failed to load configuration", "error", err)
		return
	}
	store, err := redis.NewRedisStore(ctx, *cfg, holdTTL)
	if err != nil {
		slog.Error("failed to initialize Redis store", "error", err)
		return
	}

	var wg sync.WaitGroup
	wg.Add(4)
	go runWorker(ctx, store, &wg)
	go runProducer(ctx, store, &wg)
	go runJanitor(ctx, store, &wg)
	go runDelayedPoller(ctx, store, &wg)

	<-ctx.Done()
	slog.Info("shutdown signal received, waiting for all threads to exit...")
	wg.Wait()

	slog.Info("all threads exited, closing connection pool...")
	if err := store.Close(); err != nil {
		slog.Error("error closing redis store", "error", err)
	}
	slog.Info("shutdown complete")
}
