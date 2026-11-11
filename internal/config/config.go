package config

import (
	"os"
	"strconv"
	"time"

	"github.com/joho/godotenv"
	"github.com/rs/zerolog/log"
	"github.com/tharunn0/ingestd/internal/dlq"
)

const (
	defaultAuthSecretKey   = "default-dev-secret-key"
	defaultListenAddr      = ":3000"
	defaultQueueCapacity   = 10000
	defaultWorkerCount     = 16
	defaultShutdownTimeout = 15 * time.Second
)

type Config struct {
	AuthSecretKey   []byte
	ListenAddr      string
	QueueCapacity   int
	WorkerCount     int
	DLQPath         string
	ShutdownTimeout time.Duration
}

func Init() Config {
	_ = godotenv.Load()

	return Config{
		AuthSecretKey:   []byte(stringEnv("INGESTD_AUTH_SECRET_KEY", defaultAuthSecretKey)),
		ListenAddr:      stringEnv("INGESTD_LISTEN_ADDR", defaultListenAddr),
		QueueCapacity:   positiveIntEnv("INGESTD_QUEUE_CAPACITY", defaultQueueCapacity),
		WorkerCount:     positiveIntEnv("INGESTD_WORKER_COUNT", defaultWorkerCount),
		DLQPath:         stringEnv("INGESTD_DLQ_PATH", dlq.DefaultPath),
		ShutdownTimeout: positiveDurationEnv("INGESTD_SHUTDOWN_TIMEOUT", defaultShutdownTimeout),
	}
}

func stringEnv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func positiveIntEnv(key string, fallback int) int {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback
	}

	value, err := strconv.Atoi(raw)
	if err != nil || value <= 0 {
		log.Warn().Str("key", key).Str("value", raw).Int("default", fallback).Msg("invalid config value, using default")
		return fallback
	}
	return value
}

func positiveDurationEnv(key string, fallback time.Duration) time.Duration {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback
	}

	value, err := time.ParseDuration(raw)
	if err != nil || value <= 0 {
		log.Warn().Str("key", key).Str("value", raw).Dur("default", fallback).Msg("invalid config value, using default")
		return fallback
	}
	return value
}
