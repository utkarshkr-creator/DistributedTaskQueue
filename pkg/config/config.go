package config

import (
	"os"
	"strconv"
	"time"
)

type Config struct {
	Redis    RedisConfig
	Postgres PostgresConfig
	Metrics  MetricsConfig
	Queue    QueueConfig
}

type RedisConfig struct {
	Address  string
	Password string
	DB       int
}

type PostgresConfig struct {
	ConnString string
}

type MetricsConfig struct {
	Addr string
}

type QueueConfig struct {
	Name             string
	HoldTTL          time.Duration
	JanitorInterval  time.Duration
	DelayedPollEvery time.Duration
	DepthPollEvery   time.Duration
}

func getEnv(key, fallback string) string {
	if value, exists := os.LookupEnv(key); exists && value != "" {
		return value
	}
	return fallback
}

func Load() (*Config, error) {
	dbStr := getEnv("REDIS_DB", "0")
	db, _ := strconv.Atoi(dbStr)

	return &Config{
		Redis: RedisConfig{
			Address:  getEnv("REDIS_ADDRESS", "localhost:6379"),
			Password: getEnv("REDIS_PASSWORD", ""),
			DB:       db,
		},
		Postgres: PostgresConfig{
			ConnString: getEnv("POSTGRES_CONN_STRING", "postgres://postgres:password@localhost:5432/task_queue?sslmode=disable"),
		},
		Metrics: MetricsConfig{
			Addr: getEnv("METRICS_ADDR", ":9090"),
		},
		Queue: QueueConfig{
			Name:             getEnv("QUEUE_NAME", "queue:video_processing"),
			HoldTTL:          20 * time.Second,
			JanitorInterval:  5 * time.Second,
			DelayedPollEvery: 2 * time.Second,
			DepthPollEvery:   5 * time.Second,
		},
	}, nil
}
