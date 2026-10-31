package config_test

import (
	"os"
	"testing"

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
