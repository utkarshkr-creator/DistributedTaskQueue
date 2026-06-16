package main

import (
	"context"
	"distributed-queue/pkg/redis"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"
)

const queueName = "queue:video_processing"

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	cfg, err := redis.LoadConfig()
	if err != nil {
		slog.Error("Failed to load configuration", "error", err)
		return
	}
	store, err := redis.NewRedisStore(ctx, *cfg, 20*time.Second)
	if err != nil {
		slog.Error("Failed to initialize Redis store", "error", err)
		return
	}

	defer func() {
		slog.Info("Shutting down connection pool gracefully...")
		store.Close()
	}()
	workerDone := make(chan struct{})
	go func() {
		defer close(workerDone)
		slog.Info("Worker thread successfully started, waiting for tasks...")
		for {
			select {
			case <-ctx.Done():
				slog.Info("Worker thread received shutdown signal, exiting...")
				return
			default:
				tasks, err := store.ConsumeTask(ctx, queueName)
				if err != nil {
					slog.Error("Failed to consume task", "error", err)
					time.Sleep(1 * time.Second) // Backoff before retrying
					continue
				}
				if tasks == "" {
					slog.Info("No tasks available, worker is idle...")
					time.Sleep(1 * time.Second) // Sleep briefly before checking again
					continue
				}
				slog.Info("Task consumed successfully", "taskId", tasks)
				// Simulate task processing
				time.Sleep(2 * time.Second)
				slog.Info("Task processing completed", "taskId", tasks)
				err = store.AcknowledgeTask(ctx, queueName, tasks)
				if err != nil {
					slog.Error("Failed to acknowledge task", "error", err)
					continue
				}
				slog.Info("Task acknowledged successfully", "taskId", tasks)
			}
		}
	}()

	go func() {
		slog.Info("Producer thread successfully started, generating tasks...")
		taskCounter := 1
		for {
			select {
			case <-ctx.Done():
				slog.Info("Producer thread received shutdown signal, exiting...")
				return
			default:
				taskId := "task_" + strconv.Itoa(taskCounter)
				err := store.EnqueueTask(ctx, queueName, taskId)
				if err != nil {
					slog.Error("Failed to enqueue task", "error", err)
					time.Sleep(1 * time.Second) // Backoff before retrying
					continue
				}
				slog.Info("Task enqueued successfully", "taskId", taskId)
				taskCounter++
				time.Sleep(1 * time.Second) // Simulate time between task generation
			}
		}
	}()

	<-ctx.Done()
	slog.Info("Main thread received shutdown signal, waiting for worker and producer to finish...")
	<-workerDone
	slog.Info("All threads have exited, shutting down application.")
}
