package handlers_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/gofiber/fiber/v3"
	"github.com/tharunn0/ingestd/internal/http/handlers"
	"github.com/tharunn0/ingestd/internal/service"
)

func TestTelemetryHandler_Ingest(t *testing.T) {
	h := handlers.NewTelemetryHandler(service.NewTransformService())

	app := fiber.New()
	app.Post("/telemetry", h.Ingest)

	req, err := http.NewRequest(http.MethodPost, "/telemetry", nil)
	if err != nil {
		t.Fatalf("failed to create request: %v", err)
	}

	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("failed to execute request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != fiber.StatusAccepted {
		t.Fatalf("expected status %d, got %d", fiber.StatusAccepted, resp.StatusCode)
	}

	var data map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		t.Fatalf("failed to parse json response: %v", err)
	}

	if data["status"] != "accepted" {
		t.Fatalf("expected status 'accepted', got '%s'", data["status"])
	}
}
