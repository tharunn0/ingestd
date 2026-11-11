package worker

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/tharunn0/ingestd/internal/dlq"
	"github.com/tharunn0/ingestd/internal/model"
)

func captureLogs(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	original := log.Logger
	log.Logger = zerolog.New(&buf)
	t.Cleanup(func() { log.Logger = original })
	return &buf
}

func decodeLog(t *testing.T, buf *bytes.Buffer) map[string]any {
	t.Helper()
	var entry map[string]any
	if err := json.Unmarshal(buf.Bytes(), &entry); err != nil {
		t.Fatalf("expected a single JSON log entry, got %q: %v", buf.String(), err)
	}
	return entry
}

func TestPool_DeadLetterPersistsRecord(t *testing.T) {
	logs := captureLogs(t)
	path := filepath.Join(t.TempDir(), "dlq.jsonl")
	d, err := dlq.New(path)
	if err != nil {
		t.Fatalf("failed to create dlq: %v", err)
	}
	defer d.Close()

	p := New(make(chan model.Job), 1, nil, d, RetryPolicy{})
	event := model.Transform(model.Job{
		TenantID: "org_enterprise_99",
		Payload:  model.RawEvent{"ev_name": "user_checkout_completed"},
	})

	p.deadLetter(event, 3, errors.New("downstream returned 503"))

	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("failed to read dlq file: %v", err)
	}

	var record dlq.Record
	if err := json.Unmarshal(content, &record); err != nil {
		t.Fatalf("invalid dlq line %q: %v", content, err)
	}

	if record.TenantID != "org_enterprise_99" {
		t.Errorf("expected tenant 'org_enterprise_99', got %q", record.TenantID)
	}
	if record.RetryAttempts != 3 {
		t.Errorf("expected 3 retry attempts, got %d", record.RetryAttempts)
	}
	if record.Reason != "downstream returned 503" {
		t.Errorf("unexpected reason %q", record.Reason)
	}
	if record.Payload.EventName != "user_checkout_completed" {
		t.Errorf("unexpected payload event name %q", record.Payload.EventName)
	}
	if record.FailedAt.IsZero() {
		t.Error("expected FailedAt to be set")
	}

	if got := d.Metrics(); got != (dlq.Metrics{FailedEvents: 1}) {
		t.Errorf("unexpected dlq metrics: %+v", got)
	}

	entry := decodeLog(t, logs)
	want := map[string]any{
		"level":          "warn",
		"message":        "event dead-lettered",
		"tenant_id":      "org_enterprise_99",
		"event_name":     "user_checkout_completed",
		"retry_attempts": float64(3),
		"reason":         "downstream returned 503",
	}
	for key, value := range want {
		if entry[key] != value {
			t.Errorf("expected log field %s=%v, got %v", key, value, entry[key])
		}
	}
}

func TestPool_DeadLetterLogsErrorWhenWriteFails(t *testing.T) {
	logs := captureLogs(t)

	d, err := dlq.New(filepath.Join(t.TempDir(), "dlq.jsonl"))
	if err != nil {
		t.Fatalf("failed to create dlq: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("failed to close dlq: %v", err)
	}

	p := New(make(chan model.Job), 1, nil, d, RetryPolicy{})
	event := model.Transform(model.Job{TenantID: "t1", Payload: model.RawEvent{"ev_name": "click"}})

	p.deadLetter(event, 3, errors.New("timeout"))

	entry := decodeLog(t, logs)
	if entry["level"] != "error" {
		t.Errorf("expected error level, got %v", entry["level"])
	}
	if entry["dlq_error"] == nil || entry["reason"] != "timeout" {
		t.Errorf("expected dlq_error and reason in log entry, got %v", entry)
	}
}
