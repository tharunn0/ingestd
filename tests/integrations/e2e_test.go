package integrations_test

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gofiber/fiber/v3"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/tharunn0/ingestd/internal/config"
	"github.com/tharunn0/ingestd/internal/dlq"
	"github.com/tharunn0/ingestd/internal/downstream"
	"github.com/tharunn0/ingestd/internal/http/handlers"
	"github.com/tharunn0/ingestd/internal/http/server"
	"github.com/tharunn0/ingestd/internal/queue"
	"github.com/tharunn0/ingestd/internal/service"
	"github.com/tharunn0/ingestd/internal/worker"
)

const apiKey = "integration-test-key"

const rawEvent = `{
	"ev_name": "user_checkout_completed",
	"ev_cat": "ecommerce",
	"tenant_id": "org_enterprise_99",
	"session_id": "sess_abc123xyz",
	"client_ip": "192.168.1.55",
	"user_agent": "Mozilla/5.0",
	"dim_rk1": "currency",
	"dim_rv1": "USD",
	"dim_rt1": "string",
	"dim_rk2": "payment_gateway",
	"dim_rv2": "stripe",
	"dim_rt2": "string",
	"met_rk1": "cart_total_cents",
	"met_rv1": "4999",
	"met_rt1": "integer",
	"met_rk2": "items_count",
	"met_rv2": "3",
	"met_rt2": "integer"
}`

const transformedEvent = `{
	"event_name": "user_checkout_completed",
	"event_category": "ecommerce",
	"organization_id": "org_enterprise_99",
	"session_id": "sess_abc123xyz",
	"client_metadata": {"ip_address": "192.168.1.55", "user_agent": "Mozilla/5.0"},
	"dimensions": {
		"currency": {"value": "USD", "type": "string"},
		"payment_gateway": {"value": "stripe", "type": "string"}
	},
	"metrics": {
		"cart_total_cents": {"value": "4999", "type": "integer"},
		"items_count": {"value": "3", "type": "integer"}
	}
}`

type delivery struct {
	method      string
	contentType string
	body        []byte
}

// recordingWebhook is a healthy downstream that captures every call.
func recordingWebhook(t *testing.T) (string, <-chan delivery) {
	t.Helper()

	deliveries := make(chan delivery, 10)
	webhook := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		deliveries <- delivery{method: r.Method, contentType: r.Header.Get("Content-Type"), body: body}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(webhook.Close)

	return webhook.URL, deliveries
}

// pipeline is the full service wired as in cmd/ingestd, listening on a real socket.
type pipeline struct {
	baseURL string
	pool    *worker.Pool
	dlq     *dlq.DLQ
	dlqPath string
}

func startPipeline(t *testing.T, webhookURL string, webhookTimeout time.Duration) *pipeline {
	t.Helper()

	dlqPath := filepath.Join(t.TempDir(), "dlq_failed_events.jsonl")
	deadLetters, err := dlq.New(dlqPath)
	if err != nil {
		t.Fatalf("failed to create dlq: %v", err)
	}

	ingestionQueue := queue.New(100)
	sender := downstream.New(webhookURL, webhookTimeout)
	retry := worker.RetryPolicy{MaxAttempts: 3, BaseBackoff: 10 * time.Millisecond}
	pool := worker.New(ingestionQueue.Jobs(), 4, sender, deadLetters, retry)
	pool.Start()

	cfg := config.Config{AuthSecretKey: []byte(apiKey)}
	app := server.New(cfg, handlers.NewTelemetryHandler(service.NewTransformService(ingestionQueue)))

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to open listener: %v", err)
	}
	go app.Listener(listener, fiber.ListenConfig{DisableStartupMessage: true})

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		app.ShutdownWithContext(ctx)
		pool.Shutdown(ctx)
		deadLetters.Close()
	})

	return &pipeline{
		baseURL: "http://" + listener.Addr().String(),
		pool:    pool,
		dlq:     deadLetters,
		dlqPath: dlqPath,
	}
}

func (p *pipeline) postTelemetry(t *testing.T, body string) {
	t.Helper()

	req, err := http.NewRequest(http.MethodPost, p.baseURL+"/v1/telemetry", strings.NewReader(body))
	if err != nil {
		t.Fatalf("failed to create request: %v", err)
	}
	req.Header.Set("X-API-Key", apiKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("failed to post telemetry: %v", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("failed to read response: %v", err)
	}
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("expected status 202, got %d: %s", resp.StatusCode, respBody)
	}
	if got, want := decodeJSON(t, respBody), decodeJSON(t, []byte(`{"status":"accepted"}`)); !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected response body: %s", respBody)
	}
}

func (p *pipeline) shutdownWorkers(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := p.pool.Shutdown(ctx); err != nil {
		t.Fatalf("expected clean worker shutdown, got %v", err)
	}
}

func (p *pipeline) dlqRecords(t *testing.T) []dlq.Record {
	t.Helper()
	content, err := os.ReadFile(p.dlqPath)
	if err != nil {
		t.Fatalf("failed to read dlq file: %v", err)
	}

	var records []dlq.Record
	for _, line := range strings.Split(strings.TrimSpace(string(content)), "\n") {
		if line == "" {
			continue
		}
		var record dlq.Record
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Fatalf("invalid dlq line %q: %v", line, err)
		}
		records = append(records, record)
	}
	return records
}

func (p *pipeline) expectNothingDeadLettered(t *testing.T) {
	t.Helper()
	if stats := p.dlq.Metrics(); stats != (dlq.Metrics{}) {
		t.Errorf("expected nothing dead-lettered, got %+v", stats)
	}
	if records := p.dlqRecords(t); len(records) != 0 {
		t.Errorf("expected empty dlq file, got %+v", records)
	}
}

func awaitDelivery(t *testing.T, deliveries <-chan delivery) delivery {
	t.Helper()
	select {
	case got := <-deliveries:
		return got
	case <-time.After(5 * time.Second):
		t.Fatal("webhook did not receive the event")
		return delivery{}
	}
}

func expectNoMoreDeliveries(t *testing.T, deliveries <-chan delivery) {
	t.Helper()
	select {
	case extra := <-deliveries:
		t.Errorf("expected exactly one webhook call, got an extra one: %s", extra.body)
	default:
	}
}

func decodeJSON(t *testing.T, raw []byte) any {
	t.Helper()
	var decoded any
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("invalid JSON %q: %v", raw, err)
	}
	return decoded
}

func expectSameJSON(t *testing.T, got []byte, want string) {
	t.Helper()
	if !reflect.DeepEqual(decodeJSON(t, got), decodeJSON(t, []byte(want))) {
		t.Errorf("unexpected JSON:\n got: %s\nwant: %s", got, want)
	}
}

func silenceLogs(t *testing.T) {
	t.Helper()
	original := log.Logger
	log.Logger = zerolog.Nop()
	t.Cleanup(func() { log.Logger = original })
}

func TestE2E_TelemetryIsTransformedAndDelivered(t *testing.T) {
	webhookURL, deliveries := recordingWebhook(t)
	p := startPipeline(t, webhookURL, 5*time.Second)

	p.postTelemetry(t, rawEvent)

	got := awaitDelivery(t, deliveries)
	if got.method != http.MethodPost {
		t.Errorf("expected webhook POST, got %s", got.method)
	}
	if !strings.HasPrefix(got.contentType, "application/json") {
		t.Errorf("expected JSON content type on webhook call, got %q", got.contentType)
	}
	expectSameJSON(t, got.body, transformedEvent)

	p.shutdownWorkers(t)

	if processed := p.pool.Processed(); processed != 1 {
		t.Errorf("expected 1 processed event, got %d", processed)
	}
	expectNoMoreDeliveries(t, deliveries)
	p.expectNothingDeadLettered(t)
}

func TestE2E_IncompleteTripletsAreDroppedButEventIsDelivered(t *testing.T) {
	webhookURL, deliveries := recordingWebhook(t)
	p := startPipeline(t, webhookURL, 5*time.Second)

	p.postTelemetry(t, `{
		"ev_name": "user_checkout_completed",
		"ev_cat": "ecommerce",
		"tenant_id": "org_enterprise_99",
		"session_id": "sess_abc123xyz",
		"client_ip": "192.168.1.55",
		"user_agent": "Mozilla/5.0",

		"dim_rk1": "currency", "dim_rv1": "USD", "dim_rt1": "string",
		"dim_rv2": "missing_key", "dim_rt2": "string",
		"dim_rk3": "missing_value", "dim_rt3": "string",
		"dim_rk4": "missing_type", "dim_rv4": "stripe",

		"met_rv1": "4999", "met_rt1": "integer",
		"met_rk2": "missing_value", "met_rt2": "integer",
		"met_rk3": "missing_type", "met_rv3": "7",
		"met_rk4": "items_count", "met_rv4": "3", "met_rt4": "integer"
	}`)

	got := awaitDelivery(t, deliveries)
	expectSameJSON(t, got.body, `{
		"event_name": "user_checkout_completed",
		"event_category": "ecommerce",
		"organization_id": "org_enterprise_99",
		"session_id": "sess_abc123xyz",
		"client_metadata": {"ip_address": "192.168.1.55", "user_agent": "Mozilla/5.0"},
		"dimensions": {
			"currency": {"value": "USD", "type": "string"}
		},
		"metrics": {
			"items_count": {"value": "3", "type": "integer"}
		}
	}`)

	p.shutdownWorkers(t)

	if processed := p.pool.Processed(); processed != 1 {
		t.Errorf("expected 1 processed event, got %d", processed)
	}
	expectNoMoreDeliveries(t, deliveries)
	p.expectNothingDeadLettered(t)
}

// expectDeadLettered posts the sample event to a pipeline whose webhook never
// succeeds and asserts it ends up in the DLQ after three attempts.
func expectDeadLettered(t *testing.T, p *pipeline) dlq.Record {
	t.Helper()

	started := time.Now()
	p.postTelemetry(t, rawEvent)
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Errorf("expected ingestion not to wait on the webhook, took %s", elapsed)
	}

	deadline := time.Now().Add(10 * time.Second)
	for p.dlq.Metrics().FailedEvents == 0 {
		if time.Now().After(deadline) {
			t.Fatal("event was not dead-lettered")
		}
		time.Sleep(5 * time.Millisecond)
	}

	p.shutdownWorkers(t)

	if processed := p.pool.Processed(); processed != 0 {
		t.Errorf("expected failed event not to count as processed, got %d", processed)
	}
	if stats := p.dlq.Metrics(); stats != (dlq.Metrics{FailedEvents: 1}) {
		t.Errorf("unexpected dlq metrics: %+v", stats)
	}

	records := p.dlqRecords(t)
	if len(records) != 1 {
		t.Fatalf("expected 1 dlq record, got %d", len(records))
	}
	record := records[0]

	if record.TenantID != "org_enterprise_99" {
		t.Errorf("expected tenant 'org_enterprise_99', got %q", record.TenantID)
	}
	if record.RetryAttempts != 3 {
		t.Errorf("expected 3 retry attempts, got %d", record.RetryAttempts)
	}
	if !strings.HasPrefix(record.Reason, "max attempts reached: ") {
		t.Errorf("expected reason to report exhausted attempts with a cause, got %q", record.Reason)
	}
	if record.FailedAt.Before(started) {
		t.Errorf("expected failed_at after the request, got %s", record.FailedAt)
	}

	payload, err := json.Marshal(record.Payload)
	if err != nil {
		t.Fatalf("failed to encode dlq payload: %v", err)
	}
	expectSameJSON(t, payload, transformedEvent)

	return record
}

func TestE2E_HangingWebhookDeadLettersEvent(t *testing.T) {
	silenceLogs(t)

	var calls atomic.Int32
	release := make(chan struct{})
	webhook := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		calls.Add(1)
		<-release
	}))
	t.Cleanup(webhook.Close)
	t.Cleanup(func() { close(release) })

	p := startPipeline(t, webhook.URL, 100*time.Millisecond)
	expectDeadLettered(t, p)

	if got := calls.Load(); got != 3 {
		t.Errorf("expected 3 webhook attempts, got %d", got)
	}
}

func TestE2E_UnreachableWebhookDeadLettersEvent(t *testing.T) {
	silenceLogs(t)

	webhook := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	unreachableURL := webhook.URL
	webhook.Close()

	p := startPipeline(t, unreachableURL, time.Second)
	expectDeadLettered(t, p)
}
