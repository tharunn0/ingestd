package handlers_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/gofiber/fiber/v3"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/tharunn0/ingestd/internal/http/handlers"
	"github.com/tharunn0/ingestd/internal/metrics"
	"github.com/tharunn0/ingestd/internal/queue"
	"github.com/tharunn0/ingestd/internal/service"
)

const validBody = `{"event_name":"click","dim_rk1":"browser","dim_rv1":"chrome","dim_rt1":"string"}`

func captureLogs(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	original := log.Logger
	log.Logger = zerolog.New(&buf)
	t.Cleanup(func() { log.Logger = original })
	return &buf
}

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
	logs := captureLogs(t)
	q := queue.New(1)
	resp := post(t, newTestApp(q), validBody)

	if logs.Len() != 0 {
		t.Fatalf("expected no logs on the success path, got %q", logs.String())
	}

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
	logs := captureLogs(t)
	app := newTestApp(queue.New(1))

	if resp := post(t, app, validBody); resp.StatusCode != fiber.StatusAccepted {
		t.Fatalf("expected first request %d, got %d", fiber.StatusAccepted, resp.StatusCode)
	}

	if resp := post(t, app, validBody); resp.StatusCode != fiber.StatusServiceUnavailable {
		t.Fatalf("expected status %d, got %d", fiber.StatusServiceUnavailable, resp.StatusCode)
	}

	var entry map[string]any
	if err := json.Unmarshal(logs.Bytes(), &entry); err != nil {
		t.Fatalf("expected a single JSON log entry, got %q: %v", logs.String(), err)
	}
	if entry["level"] != "warn" || entry["error"] != queue.ErrQueueFull.Error() {
		t.Fatalf("unexpected rejection log entry: %v", entry)
	}
}

func TestTelemetryHandler_CountsReceivedAndRejected(t *testing.T) {
	captureLogs(t)
	app := newTestApp(queue.New(2))

	received := testutil.ToFloat64(metrics.EventsReceived)
	rejected := testutil.ToFloat64(metrics.EventsRejected)

	post(t, app, validBody)
	post(t, app, validBody)
	post(t, app, validBody)
	post(t, app, "{not json")

	if got := testutil.ToFloat64(metrics.EventsReceived) - received; got != 2 {
		t.Errorf("expected 2 received events, got %v", got)
	}
	if got := testutil.ToFloat64(metrics.EventsRejected) - rejected; got != 1 {
		t.Errorf("expected 1 rejected event, got %v", got)
	}
}

func TestTelemetryHandler_RejectionLogsAreSampled(t *testing.T) {
	logs := captureLogs(t)
	app := newTestApp(queue.New(1))

	for range 50 {
		post(t, app, validBody)
	}

	lines := bytes.Count(logs.Bytes(), []byte("\n"))
	if lines == 0 || lines > 10 {
		t.Fatalf("expected a handful of sampled rejection logs for 49 rejections, got %d", lines)
	}
}
