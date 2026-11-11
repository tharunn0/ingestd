package config_test

import (
	"os"
	"testing"
	"time"

	"github.com/tharunn0/ingestd/internal/config"
)

func TestConfigInit_Default(t *testing.T) {
	os.Unsetenv("INGESTD_AUTH_SECRET_KEY")

	cfg := config.Init()
	if string(cfg.AuthSecretKey) != "default-dev-secret-key" {
		t.Fatalf("expected default secret key, got %s", string(cfg.AuthSecretKey))
	}
}

func TestConfigInit_FromEnv(t *testing.T) {
	expected := "custom-secret-key"
	t.Setenv("INGESTD_AUTH_SECRET_KEY", expected)

	cfg := config.Init()
	if string(cfg.AuthSecretKey) != expected {
		t.Fatalf("expected %s, got %s", expected, string(cfg.AuthSecretKey))
	}
}

var optionalKeys = []string{
	"INGESTD_LISTEN_ADDR",
	"INGESTD_QUEUE_CAPACITY",
	"INGESTD_WORKER_COUNT",
	"INGESTD_DLQ_PATH",
	"INGESTD_SHUTDOWN_TIMEOUT",
	"INGESTD_WEBHOOK_URL",
	"INGESTD_WEBHOOK_TIMEOUT",
	"INGESTD_MAX_ATTEMPTS",
	"INGESTD_RETRY_BACKOFF",
}

func TestConfigInit_RuntimeDefaults(t *testing.T) {
	for _, key := range optionalKeys {
		t.Setenv(key, "")
	}

	cfg := config.Init()

	if cfg.ListenAddr != ":3000" {
		t.Errorf("expected default listen addr ':3000', got %q", cfg.ListenAddr)
	}
	if cfg.QueueCapacity != 10000 {
		t.Errorf("expected default queue capacity 10000, got %d", cfg.QueueCapacity)
	}
	if cfg.WorkerCount != 16 {
		t.Errorf("expected default worker count 16, got %d", cfg.WorkerCount)
	}
	if cfg.DLQPath != "dlq_failed_events.jsonl" {
		t.Errorf("expected default dlq path, got %q", cfg.DLQPath)
	}
	if cfg.ShutdownTimeout != 15*time.Second {
		t.Errorf("expected default shutdown timeout 15s, got %s", cfg.ShutdownTimeout)
	}
	if cfg.WebhookURL != "http://localhost:9000/webhook" {
		t.Errorf("expected default webhook url, got %q", cfg.WebhookURL)
	}
	if cfg.WebhookTimeout != 5*time.Second {
		t.Errorf("expected default webhook timeout 5s, got %s", cfg.WebhookTimeout)
	}
	if cfg.MaxAttempts != 3 {
		t.Errorf("expected default max attempts 3, got %d", cfg.MaxAttempts)
	}
	if cfg.RetryBackoff != 200*time.Millisecond {
		t.Errorf("expected default retry backoff 200ms, got %s", cfg.RetryBackoff)
	}
}

func TestConfigInit_RuntimeFromEnv(t *testing.T) {
	t.Setenv("INGESTD_LISTEN_ADDR", ":8080")
	t.Setenv("INGESTD_QUEUE_CAPACITY", "500")
	t.Setenv("INGESTD_WORKER_COUNT", "4")
	t.Setenv("INGESTD_DLQ_PATH", "/tmp/custom.jsonl")
	t.Setenv("INGESTD_SHUTDOWN_TIMEOUT", "30s")
	t.Setenv("INGESTD_WEBHOOK_URL", "http://example.test/hook")
	t.Setenv("INGESTD_WEBHOOK_TIMEOUT", "2s")
	t.Setenv("INGESTD_MAX_ATTEMPTS", "5")
	t.Setenv("INGESTD_RETRY_BACKOFF", "50ms")

	cfg := config.Init()

	if cfg.WebhookURL != "http://example.test/hook" {
		t.Errorf("expected custom webhook url, got %q", cfg.WebhookURL)
	}
	if cfg.WebhookTimeout != 2*time.Second {
		t.Errorf("expected webhook timeout 2s, got %s", cfg.WebhookTimeout)
	}
	if cfg.MaxAttempts != 5 {
		t.Errorf("expected max attempts 5, got %d", cfg.MaxAttempts)
	}
	if cfg.RetryBackoff != 50*time.Millisecond {
		t.Errorf("expected retry backoff 50ms, got %s", cfg.RetryBackoff)
	}

	if cfg.ListenAddr != ":8080" {
		t.Errorf("expected listen addr ':8080', got %q", cfg.ListenAddr)
	}
	if cfg.QueueCapacity != 500 {
		t.Errorf("expected queue capacity 500, got %d", cfg.QueueCapacity)
	}
	if cfg.WorkerCount != 4 {
		t.Errorf("expected worker count 4, got %d", cfg.WorkerCount)
	}
	if cfg.DLQPath != "/tmp/custom.jsonl" {
		t.Errorf("expected custom dlq path, got %q", cfg.DLQPath)
	}
	if cfg.ShutdownTimeout != 30*time.Second {
		t.Errorf("expected shutdown timeout 30s, got %s", cfg.ShutdownTimeout)
	}
}

func TestConfigInit_InvalidValuesFallBackToDefaults(t *testing.T) {
	cases := map[string]struct{ queue, workers, timeout string }{
		"not numbers": {"lots", "many", "soon"},
		"zero":        {"0", "0", "0s"},
		"negative":    {"-5", "-1", "-10s"},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			t.Setenv("INGESTD_QUEUE_CAPACITY", tc.queue)
			t.Setenv("INGESTD_WORKER_COUNT", tc.workers)
			t.Setenv("INGESTD_SHUTDOWN_TIMEOUT", tc.timeout)

			cfg := config.Init()

			if cfg.QueueCapacity != 10000 {
				t.Errorf("expected default queue capacity, got %d", cfg.QueueCapacity)
			}
			if cfg.WorkerCount != 16 {
				t.Errorf("expected default worker count, got %d", cfg.WorkerCount)
			}
			if cfg.ShutdownTimeout != 15*time.Second {
				t.Errorf("expected default shutdown timeout, got %s", cfg.ShutdownTimeout)
			}
		})
	}
}
