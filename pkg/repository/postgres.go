package repository

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5/pgxpool"
)

type PostgresConfig struct {
	ConnString string // e.g., "postgres://user:pass@localhost:5432/dbname"
}

// it will establishes a high-performance thread-safe connection pool
func NewPostgresPool(ctx context.Context, cfg PostgresConfig) (*pgxpool.Pool, error) {
	pool, err := pgxpool.New(ctx, cfg.ConnString)
	if err != nil {
		return nil, fmt.Errorf("failed to create pgx pool: %w", err)
	}

	/// Verify database connection instantly on startup (Fail-Fast)
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("failed to ping postgres database: %w", err)
	}
	slog.Info("successfully connected to postgres database")
	return pool, nil
}
