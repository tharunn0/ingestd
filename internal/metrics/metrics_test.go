package metrics_test

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/tharunn0/ingestd/internal/dlq"
	"github.com/tharunn0/ingestd/internal/metrics"
	"github.com/tharunn0/ingestd/internal/model"
	"github.com/tharunn0/ingestd/internal/queue"
	"github.com/tharunn0/ingestd/internal/worker"
)

type components struct {
	queue    *queue.IngestionQueue
	workerIn chan model.Job
	pool     *worker.Pool
	dlq      *dlq.DLQ
}

// The pool reads from its own channel so the queue depth stays deterministic.
func newComponents(t *testing.T) components {
	t.Helper()

	d, err := dlq.New(filepath.Join(t.TempDir(), "dlq.jsonl"))
	if err != nil {
		t.Fatalf("failed to create dlq: %v", err)
	}

	workerIn := make(chan model.Job)
	pool := worker.New(workerIn, 4, d)
	pool.Start()

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		pool.Shutdown(ctx)
		d.Close()
	})

	return components{queue: queue.New(100), workerIn: workerIn, pool: pool, dlq: d}
}

func expectMetrics(t *testing.T, reg *prometheus.Registry, want string) {
	t.Helper()
	if err := testutil.GatherAndCompare(reg, strings.NewReader(want)); err != nil {
		t.Fatal(err)
	}
}

func waitForProcessed(t *testing.T, pool *worker.Pool, want int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for pool.Processed() != want || pool.ActiveWorkers() != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("expected %d processed events, got %d", want, pool.Processed())
		}
		time.Sleep(time.Millisecond)
	}
}

const metricsTemplate = `
# HELP ingestd_dlq_events_total Events handed to the dead letter queue.
# TYPE ingestd_dlq_events_total counter
ingestd_dlq_events_total DLQ_EVENTS
# HELP ingestd_dlq_write_errors_total Dead letter records that could not be persisted.
# TYPE ingestd_dlq_write_errors_total counter
ingestd_dlq_write_errors_total 0
# HELP ingestd_events_processed_total Telemetry events taken off the queue and processed by a worker.
# TYPE ingestd_events_processed_total counter
ingestd_events_processed_total PROCESSED
# HELP ingestd_queue_capacity Maximum number of jobs the ingestion queue can buffer.
# TYPE ingestd_queue_capacity gauge
ingestd_queue_capacity 100
# HELP ingestd_queue_depth Jobs currently buffered in the ingestion queue.
# TYPE ingestd_queue_depth gauge
ingestd_queue_depth DEPTH
# HELP ingestd_workers_active Workers currently processing a job.
# TYPE ingestd_workers_active gauge
ingestd_workers_active 0
# HELP ingestd_workers_total Worker goroutines currently running.
# TYPE ingestd_workers_total gauge
ingestd_workers_total 4
`

func render(depth, processed, dlqEvents string) string {
	return strings.NewReplacer("DEPTH", depth, "PROCESSED", processed, "DLQ_EVENTS", dlqEvents).Replace(metricsTemplate)
}

func TestRegister_ExposesLiveComponentState(t *testing.T) {
	c := newComponents(t)

	reg := prometheus.NewRegistry()
	if err := metrics.Register(reg, c.queue, c.pool, c.dlq); err != nil {
		t.Fatalf("expected register to succeed, got %v", err)
	}

	expectMetrics(t, reg, render("0", "0", "0"))

	for range 7 {
		if err := c.queue.Enqueue(model.Job{}); err != nil {
			t.Fatalf("expected enqueue to succeed, got %v", err)
		}
	}
	for range 3 {
		c.workerIn <- model.Job{}
	}
	waitForProcessed(t, c.pool, 3)
	for range 2 {
		if err := c.dlq.Enqueue(dlq.Record{Reason: "timeout"}); err != nil {
			t.Fatalf("expected dlq enqueue to succeed, got %v", err)
		}
	}

	expectMetrics(t, reg, render("7", "3", "2"))
}

func TestRegister_ReportsDLQWriteErrors(t *testing.T) {
	c := newComponents(t)

	reg := prometheus.NewRegistry()
	if err := metrics.Register(reg, c.queue, c.pool, c.dlq); err != nil {
		t.Fatalf("expected register to succeed, got %v", err)
	}

	if err := c.dlq.Close(); err != nil {
		t.Fatalf("expected close to succeed, got %v", err)
	}
	if err := c.dlq.Enqueue(dlq.Record{Reason: "timeout"}); err == nil {
		t.Fatal("expected enqueue after close to fail")
	}

	err := testutil.GatherAndCompare(reg, strings.NewReader(`
# HELP ingestd_dlq_write_errors_total Dead letter records that could not be persisted.
# TYPE ingestd_dlq_write_errors_total counter
ingestd_dlq_write_errors_total 1
`), "ingestd_dlq_write_errors_total")
	if err != nil {
		t.Fatal(err)
	}
}

func TestRegister_DuplicateRegistrationFails(t *testing.T) {
	c := newComponents(t)
	reg := prometheus.NewRegistry()

	if err := metrics.Register(reg, c.queue, c.pool, c.dlq); err != nil {
		t.Fatalf("expected first register to succeed, got %v", err)
	}
	if err := metrics.Register(reg, c.queue, c.pool, c.dlq); err == nil {
		t.Fatal("expected duplicate register to fail")
	}
}
