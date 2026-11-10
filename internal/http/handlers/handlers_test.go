package handlers_test

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/gofiber/fiber/v3"
	"github.com/tharunn0/ingestd/internal/http/handlers"
	"github.com/tharunn0/ingestd/internal/queue"
	"github.com/tharunn0/ingestd/internal/service"
)

const validBody = `{"event_name":"click","dim_rk1":"browser","dim_rv1":"chrome","dim_rt1":"string"}`

func newTestApp(q *queue.IngestionQueue) *fiber.App {
	h := handlers.NewTelemetryHandler(service.NewTransformService(q))
	app := fiber.New()
	app.Post("/telemetry", h.Ingest)
	return app
}

func post(t *testing.T, app *fiber.App, body string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, "/telemetry", strings.NewReader(body))
	if err != nil {
		t.Fatalf("failed to create request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("failed to execute request: %v", err)
	}
	t.Cleanup(func() { resp.Body.Close() })
	return resp
}

func TestTelemetryHandler_IngestAccepted(t *testing.T) {
	q := queue.New(1)
	resp := post(t, newTestApp(q), validBody)

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

	select {
	case job := <-q.Jobs():
		if job.Payload["dim_rv1"] != "chrome" {
			t.Fatalf("expected dim_rv1 'chrome', got %q", job.Payload["dim_rv1"])
		}
		if job.ReceivedAt.IsZero() {
			t.Fatal("expected ReceivedAt to be set")
		}
	default:
		t.Fatal("expected job to be enqueued")
	}
}

func TestTelemetryHandler_IngestBadRequest(t *testing.T) {
	cases := map[string]string{
		"empty body":   "",
		"invalid json": "{not json",
		"empty object": "{}",
	}

	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			q := queue.New(1)
			resp := post(t, newTestApp(q), body)

			if resp.StatusCode != fiber.StatusBadRequest {
				t.Fatalf("expected status %d, got %d", fiber.StatusBadRequest, resp.StatusCode)
			}
			if len(q.Jobs()) != 0 {
				t.Fatal("expected no job to be enqueued")
			}
		})
	}
}

func TestTelemetryHandler_IngestQueueFull(t *testing.T) {
	app := newTestApp(queue.New(1))

	if resp := post(t, app, validBody); resp.StatusCode != fiber.StatusAccepted {
		t.Fatalf("expected first request %d, got %d", fiber.StatusAccepted, resp.StatusCode)
	}

	if resp := post(t, app, validBody); resp.StatusCode != fiber.StatusServiceUnavailable {
		t.Fatalf("expected status %d, got %d", fiber.StatusServiceUnavailable, resp.StatusCode)
	}
}
