package main

import (
	"context"
	"distributed-queue/pkg/config"
	"distributed-queue/pkg/redis"
	"distributed-queue/pkg/redis/metrics"
	"distributed-queue/pkg/repository"
	"encoding/json"
	"errors"
	"log/slog"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// simulateProcessing stands in for real task work. Replace with real
// processing logic; keep the ctx-cancellation check if real work is itself
// cancellable.
func simulateProcessing(ctx context.Context, taskId string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(2 * time.Second):
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if rand.Intn(5) == 0 {
		return errors.New("simulated processing failure")
	}
	return nil
}

// runWorker continuously consumes tasks from Redis, checks idempotency in Postgres,
// processes them, and updates Postgres and Redis accordingly. It handles retries
// and dead-lettering based on the Postgres job row's retry counter.
func runWorker(ctx context.Context, queueName string, store *redis.RedisStore, jobRepo *repository.JobRepository, wg *sync.WaitGroup) {
	defer wg.Done()
	slog.Info("worker thread started, waiting for tasks...")

	for {
		if ctx.Err() != nil {
			slog.Info("worker thread received shutdown signal, exiting...")
			return
		}

		taskId, err := store.ConsumeTask(ctx, queueName)
		if err != nil {
			if ctx.Err() != nil {
				slog.Info("worker thread received shutdown signal, exiting...")
				return
			}
			slog.Error("failed to consume task", "error", err)
			//TODO: Implement exponential backoff here instead of busy-looping on Redis errors
			time.Sleep(1 * time.Second)
			continue
		}
		if taskId == "" {
			continue
		}

		slog.Info("task consumed, running idempotency check", "taskId", taskId)

		status, err := jobRepo.TryStartJob(ctx, queueName, taskId)
		if err != nil {
			if errors.Is(err, repository.ErrDuplicateJob) {
				slog.Warn("[IDEMPOTENCY] task already completed, dropping from Redis", "taskId", taskId)
				metrics.TasksAcknowledged.WithLabelValues(queueName).Inc()
				if err := store.AcknowledgeTask(ctx, queueName, taskId); err != nil {
					slog.Error("failed to clean up duplicate task from Redis", "taskId", taskId, "error", err)
				}
				continue
			}
			if errors.Is(err, repository.ErrJobNotFound) {
				slog.Error("[IDEMPOTENCY] task in Redis but no DB row exists - data inconsistency, moving to DLQ", "taskId", taskId)
				metrics.TasksOrphaned.WithLabelValues(queueName).Inc()
				if err := store.MoveToDeadLetterQueue(ctx, queueName, taskId); err != nil {
					slog.Error("failed to move orphaned task to DLQ", "taskId", taskId, "error", err)
				}
				continue
			}
			// Any other error (DB down, timeout, etc): task stays claimed in
			// Redis inflight. We deliberately do NOT retry/DLQ here — we
			// leave it for the janitor to sweep back to the main queue after
			// holdTTL, since we don't know if the DB write actually landed.
			slog.Error("idempotency check failed, leaving task for janitor to recover", "taskId", taskId, "error", err)
			continue
		}
		slog.Info("idempotency check passed", "taskId", taskId, "status", status)

		processingStart := time.Now()
		procErr := simulateProcessing(ctx, taskId)
		metrics.ProcessingDuration.WithLabelValues(queueName).Observe(time.Since(processingStart).Seconds())

		if procErr == nil {
			slog.Info("task processing completed, syncing to postgres", "taskId", taskId)

			if err := jobRepo.CompleteJob(ctx, queueName, taskId); err != nil {
				slog.Error("failed to write job completion to postgres, leaving for janitor to recover", "taskId", taskId, "error", err)
				//TODO: Implement exponential backoff here instead of busy-looping on DB errors
				time.Sleep(500 * time.Millisecond)
				continue
			}

			// Check for ready children (DAG orchestration)
			children, err := jobRepo.CheckAndEnqueueChildren(ctx, queueName, taskId)
			if err != nil {
				slog.Error("failed to check for ready children", "taskId", taskId, "error", err)
			} else if len(children) > 0 {
				slog.Info("enqueuing ready children", "parentTask", taskId, "childCount", len(children))
				for _, childID := range children {
					if err := store.EnqueueTask(ctx, queueName, childID); err != nil {
						slog.Error("failed to enqueue child task", "childId", childID, "error", err)
						metrics.ChildEnqueueFailures.WithLabelValues(queueName).Inc()
					} else {
						metrics.ChildrenEnqueued.WithLabelValues(queueName).Inc()
					}
				}
			}

			if err := store.AcknowledgeTask(ctx, queueName, taskId); err != nil {
				slog.Error("failed to acknowledge task, will be recovered by janitor", "taskId", taskId, "error", err)
				//TODO: Implement exponential backoff here instead of busy-looping on Redis errors
				time.Sleep(500 * time.Millisecond) // avoid busy-looping on Redis errors
			} else {
				slog.Info("task acknowledged and synchronized everywhere", "taskId", taskId)
			}
			continue
		}

		// Processing failed. Decide retry vs dead-letter using the DB's
		// per-job retry counter as the source of truth.
		metrics.ProcessingFailures.WithLabelValues(queueName).Inc()
		slog.Warn("task processing failed, recording failure in postgres", "taskId", taskId, "error", procErr)

		currentRetry, maxRetry, dbErr := jobRepo.RecordFailure(ctx, queueName, taskId, procErr.Error())
		if dbErr != nil {
			slog.Error("failed to record failure in postgres, leaving for janitor to recover", "taskId", taskId, "error", dbErr)
			//TODO: Implement exponential backoff here instead of busy-looping on DB errors
			time.Sleep(500 * time.Millisecond)
			continue
		}

		if currentRetry >= maxRetry {
			slog.Error("task exceeded max retries, moving to DLQ", "taskId", taskId, "retries", currentRetry, "maxRetries", maxRetry)

			// Cancel all dependent children since this parent will never complete
			if err := jobRepo.MarkAsCancelledCascading(ctx, queueName, taskId); err != nil {
				slog.Error("failed to cascade cancellation to children", "taskId", taskId, "error", err)
			}

			if err := store.MoveToDeadLetterQueue(ctx, queueName, taskId); err != nil {
				slog.Error("failed to move task to DLQ, leaving for janitor to recover", "taskId", taskId, "error", err)
			}
			continue
		}

		slog.Warn("scheduling backoff retry", "taskId", taskId, "attempt", currentRetry, "max", maxRetry)
		if err := store.RetryTaskWithBackoff(ctx, queueName, taskId, currentRetry); err != nil {
			slog.Error("failed to schedule retry, leaving for janitor to recover", "taskId", taskId, "error", err)
		}
	}
}

// ranProducer generates tasks at a fixed interval, creating a workflow and jobs in Postgres, then enqueuing them to Redis.
// It simulates a task producer for demonstration purposes.
func runProducer(ctx context.Context, queueName string, store *redis.RedisStore, jobRepo *repository.JobRepository, wg *sync.WaitGroup) {
	defer wg.Done()
	slog.Info("producer thread started, generating tasks...")

	// Create one workflow for all demo tasks
	workflowID, err := jobRepo.CreateWorkflow(ctx, "demo-workflow")
	if err != nil {
		slog.Error("failed to create workflow, producer cannot start", "error", err)
		return
	}

	taskCounter := 1
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			slog.Info("producer thread received shutdown signal, exiting...")
			return
		case <-ticker.C:
			taskID := uuid.New()
			task := repository.Task{
				ID:         taskID,
				WorkflowID: workflowID,
				ParentIDs:  []uuid.UUID{}, // root task, no parents
				TaskType:   "VIDEO_PROCESS",
				Payload:    json.RawMessage(`{}`),
			}

			// WAL: Write DB row FIRST
			if err := jobRepo.CreateJob(ctx, queueName, task); err != nil {
				slog.Error("failed to create job in DB, skipping", "taskId", taskID, "error", err)
				continue
			}

			// THEN enqueue to Redis
			if err := store.EnqueueTask(ctx, queueName, taskID.String()); err != nil {
				slog.Error("job created in DB but Redis enqueue failed - orphaned", "taskId", taskID, "error", err)
				continue
			}

			slog.Info("task enqueued", "taskId", taskID, "counter", taskCounter)
			taskCounter++
		}
	}
}

// ranJanitor periodically checks for tasks that have been inflight longer than holdTTL and moves them back to the main queue for retry.
func runJanitor(ctx context.Context, queueName string, interval time.Duration, store *redis.RedisStore, wg *sync.WaitGroup) {
	defer wg.Done()
	slog.Info("janitor thread started, monitoring for abandoned tasks...")

	ticker := time.NewTicker(interval)
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

// runDelayedPoller periodically checks the delayed queue for tasks whose delay has expired and moves them to the main queue.
func runDelayedPoller(ctx context.Context, queueName string, interval time.Duration, store *redis.RedisStore, wg *sync.WaitGroup) {
	defer wg.Done()
	slog.Info("delayed-task poller started")

	ticker := time.NewTicker(interval)
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

// runDepthCollector periodically snapshots queue/inflight/delayed/DLQ sizes
// into Prometheus gauges.
func runDepthCollector(ctx context.Context, queueName string, interval time.Duration, store *redis.RedisStore, wg *sync.WaitGroup) {
	defer wg.Done()
	slog.Info("depth collector started")

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			slog.Info("depth collector received shutdown signal, exiting...")
			return
		case <-ticker.C:
			if err := store.CollectDepths(ctx, queueName); err != nil {
				slog.Error("failed to collect queue depths", "error", err)
			}
		}
	}
}

// runMetricsServer serves Prometheus metrics on metricsAddr until ctx is
// cancelled, then shuts the HTTP server down gracefully.
func runMetricsServer(ctx context.Context, metricsAddr string, wg *sync.WaitGroup) {
	defer wg.Done()

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	srv := &http.Server{Addr: metricsAddr, Handler: mux}

	serverErr := make(chan error, 1)
	go func() {
		slog.Info("metrics server started", "addr", metricsAddr)
		serverErr <- srv.ListenAndServe()
	}()

	select {
	case <-ctx.Done():
		slog.Info("metrics server received shutdown signal, shutting down...")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			slog.Error("metrics server shutdown error", "error", err)
		}
	case err := <-serverErr:
		if err != nil && err != http.ErrServerClosed {
			slog.Error("metrics server failed", "error", err)
		}
	}
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	cfg, err := config.Load()
	if err != nil {
		slog.Error("failed to load configuration", "error", err)
		return
	}

	store, err := redis.NewRedisStore(ctx, cfg.Redis, cfg.Queue.HoldTTL)
	if err != nil {
		slog.Error("failed to initialize Redis store", "error", err)
		return
	}

	pgCfg := repository.PostgresConfig{ConnString: cfg.Postgres.ConnString}
	pgPool, err := repository.NewPostgresPool(ctx, pgCfg)
	if err != nil {
		slog.Error("failed to initialize PostgreSQL connection pool", "error", err)
		store.Close()
		return
	}

	jobRepo := repository.NewJobRepository(pgPool)

	defer func() {
		slog.Info("shutting down, closing database connections...")
		pgPool.Close()
		store.Close()
	}()

	var wg sync.WaitGroup
	wg.Add(6)
	go runWorker(ctx, cfg.Queue.Name, store, jobRepo, &wg)
	go runProducer(ctx, cfg.Queue.Name, store, jobRepo, &wg)
	go runJanitor(ctx, cfg.Queue.Name, cfg.Queue.JanitorInterval, store, &wg)
	go runDelayedPoller(ctx, cfg.Queue.Name, cfg.Queue.DelayedPollEvery, store, &wg)
	go runDepthCollector(ctx, cfg.Queue.Name, cfg.Queue.DepthPollEvery, store, &wg)
	go runMetricsServer(ctx, cfg.Metrics.Addr, &wg)

	<-ctx.Done()
	slog.Info("shutdown signal received, waiting for all threads to exit...")
	wg.Wait()

	slog.Info("all threads exited, shutdown complete")
}
