package dlq_test

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/tharunn0/ingestd/internal/dlq"
	"github.com/tharunn0/ingestd/internal/model"
)

func newDLQ(t *testing.T) (*dlq.DLQ, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "dlq.jsonl")
	d, err := dlq.New(path)
	if err != nil {
		t.Fatalf("failed to create dlq: %v", err)
	}
	t.Cleanup(func() { d.Close() })
	return d, path
}

func readRecords(t *testing.T, path string) []dlq.Record {
	t.Helper()
	file, err := os.Open(path)
	if err != nil {
		t.Fatalf("failed to open dlq file: %v", err)
	}
	defer file.Close()

	var records []dlq.Record
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var record dlq.Record
		if err := json.Unmarshal(scanner.Bytes(), &record); err != nil {
			t.Fatalf("invalid jsonl line %q: %v", scanner.Text(), err)
		}
		records = append(records, record)
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("failed to scan dlq file: %v", err)
	}
	return records
}

func sampleRecord(reason string) dlq.Record {
	return dlq.Record{
		TenantID:      "org_enterprise_99",
		FailedAt:      time.Date(2026, 11, 11, 10, 0, 0, 0, time.UTC),
		RetryAttempts: 3,
		Reason:        reason,
		Payload: model.TransformedEvent{
			EventName:      "user_checkout_completed",
			OrganizationID: "org_enterprise_99",
			Dimensions:     map[string]model.TypedValue{"currency": {Value: "USD", Type: "string"}},
			Metrics:        map[string]model.TypedValue{"items_count": {Value: "3", Type: "integer"}},
		},
	}
}

func TestRecord_JSONShape(t *testing.T) {
	encoded, err := json.Marshal(sampleRecord("downstream returned 503"))
	if err != nil {
		t.Fatalf("failed to marshal: %v", err)
	}

	var fields map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &fields); err != nil {
		t.Fatalf("failed to decode: %v", err)
	}

	for _, key := range []string{"tenant_id", "failed_at", "retry_attempts", "reason", "payload"} {
		if _, ok := fields[key]; !ok {
			t.Errorf("expected field %q in %s", key, encoded)
		}
	}
	if len(fields) != 5 {
		t.Errorf("expected exactly 5 fields, got %d: %s", len(fields), encoded)
	}
}

func TestDLQ_EnqueuePersistsJSONLines(t *testing.T) {
	d, path := newDLQ(t)

	want := []dlq.Record{sampleRecord("timeout"), sampleRecord("downstream returned 503")}
	for _, record := range want {
		if err := d.Enqueue(record); err != nil {
			t.Fatalf("expected enqueue to succeed, got %v", err)
		}
	}

	if got := readRecords(t, path); !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected records:\n got: %+v\nwant: %+v", got, want)
	}

	if got := d.Metrics(); got != (dlq.Metrics{FailedEvents: 2}) {
		t.Fatalf("unexpected metrics: %+v", got)
	}
}

func TestDLQ_EnqueueSetsFailedAtWhenZero(t *testing.T) {
	d, path := newDLQ(t)

	record := sampleRecord("timeout")
	record.FailedAt = time.Time{}

	before := time.Now().Add(-time.Second)
	if err := d.Enqueue(record); err != nil {
		t.Fatalf("expected enqueue to succeed, got %v", err)
	}

	got := readRecords(t, path)
	if len(got) != 1 || got[0].FailedAt.Before(before) {
		t.Fatalf("expected FailedAt to default to now, got %+v", got)
	}
}

func TestDLQ_AppendsToExistingFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "dlq.jsonl")

	for _, reason := range []string{"first run", "second run"} {
		d, err := dlq.New(path)
		if err != nil {
			t.Fatalf("failed to create dlq: %v", err)
		}
		if err := d.Enqueue(sampleRecord(reason)); err != nil {
			t.Fatalf("expected enqueue to succeed, got %v", err)
		}
		if err := d.Close(); err != nil {
			t.Fatalf("expected close to succeed, got %v", err)
		}
	}

	got := readRecords(t, path)
	if len(got) != 2 || got[0].Reason != "first run" || got[1].Reason != "second run" {
		t.Fatalf("expected both runs to be preserved in order, got %+v", got)
	}
}

func TestDLQ_ConcurrentEnqueue(t *testing.T) {
	d, path := newDLQ(t)

	const writers = 50
	var wg sync.WaitGroup
	for range writers {
		wg.Go(func() {
			if err := d.Enqueue(sampleRecord("timeout")); err != nil {
				t.Errorf("expected enqueue to succeed, got %v", err)
			}
		})
	}
	wg.Wait()

	if got := len(readRecords(t, path)); got != writers {
		t.Fatalf("expected %d intact lines, got %d", writers, got)
	}
	if got := d.Metrics().FailedEvents; got != writers {
		t.Fatalf("expected failed_events %d, got %d", writers, got)
	}
}

func TestDLQ_EnqueueAfterCloseCountsWriteError(t *testing.T) {
	d, _ := newDLQ(t)

	if err := d.Close(); err != nil {
		t.Fatalf("expected close to succeed, got %v", err)
	}

	if err := d.Enqueue(sampleRecord("timeout")); err == nil {
		t.Fatal("expected enqueue after close to fail")
	}

	if got := d.Metrics(); got != (dlq.Metrics{FailedEvents: 1, WriteErrors: 1}) {
		t.Fatalf("unexpected metrics: %+v", got)
	}
}

func TestNew_InvalidPath(t *testing.T) {
	if _, err := dlq.New(filepath.Join(t.TempDir(), "missing", "dlq.jsonl")); err == nil {
		t.Fatal("expected error for path in a missing directory")
	}
}

func TestMetrics_JSONShape(t *testing.T) {
	encoded, err := json.Marshal(dlq.Metrics{FailedEvents: 2, WriteErrors: 1})
	if err != nil {
		t.Fatalf("failed to marshal: %v", err)
	}
	if string(encoded) != `{"failed_events":2,"write_errors":1}` {
		t.Fatalf("unexpected metrics JSON: %s", encoded)
	}
}
