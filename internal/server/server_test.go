package server_test

import (
	"encoding/json"
	"io"
	"net/http"
	"testing"

	"github.com/gofiber/fiber/v3"
	"github.com/tharunn0/ingestd/internal/config"
	"github.com/tharunn0/ingestd/internal/server"
)

func TestHealthEndpoint(t *testing.T) {
	cfg := config.Config{AuthSecretKey: []byte("test-secret")}
	app := server.New(cfg)

	req, err := http.NewRequest(http.MethodGet, "/health", nil)
	if err != nil {
		t.Fatalf("failed to create request: %v", err)
	}

	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("failed to execute request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != fiber.StatusOK {
		t.Fatalf("expected status %d, got %d", fiber.StatusOK, resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("failed to read response body: %v", err)
	}

	var data map[string]string
	if err := json.Unmarshal(body, &data); err != nil {
		t.Fatalf("failed to parse json response: %v", err)
	}

	if data["status"] != "ok" {
		t.Fatalf("expected status 'ok', got '%s'", data["status"])
	}
}

func TestTelemetryEndpoint_Auth(t *testing.T) {
	cfg := config.Config{AuthSecretKey: []byte("valid-key")}
	app := server.New(cfg)

	// Unauthorized without header
	reqUnauthorized, _ := http.NewRequest(http.MethodPost, "/v1/telemetry", nil)
	resp, err := app.Test(reqUnauthorized)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != fiber.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", resp.StatusCode)
	}

	// Authorized with X-API-Key
	reqAuthorized, _ := http.NewRequest(http.MethodPost, "/v1/telemetry", nil)
	reqAuthorized.Header.Set("X-API-Key", "valid-key")
	resp, err = app.Test(reqAuthorized)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != fiber.StatusAccepted {
		t.Fatalf("expected 202, got %d", resp.StatusCode)
	}
}
