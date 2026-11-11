package worker

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/tharunn0/ingestd/internal/dlq"
	"github.com/tharunn0/ingestd/internal/model"
)

func TestPool_DeadLetterPersistsRecord(t *testing.T) {
	path := filepath.Join(t.TempDir(), "dlq.jsonl")
	d, err := dlq.New(path)
	if err != nil {
		t.Fatalf("failed to create dlq: %v", err)
	}
	defer d.Close()

	p := New(make(chan model.Job), 1, d)
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
}
