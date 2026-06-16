package redis

import (
	"os"
	"strconv"
)

// Config holds the raw settings for Redis
type Config struct {
	Address  string
	Password string
	DB       int
}

// LoadConfig reads from the environment and returns a Config struct
func LoadConfig() (*Config, error) {
	addr, exists := os.LookupEnv("REDIS_ADDRESS")
	if !exists {
		addr = "localhost:6379" // Default value
	}

	pw := os.Getenv("REDIS_PASSWORD") // Returns empty string if missing

	dbStr := os.Getenv("REDIS_DB")
	db, _ := strconv.Atoi(dbStr) // Defaults to 0 if empty or invalid

	return &Config{
		Address:  addr,
		Password: pw,
		DB:       db,
	}, nil
}
