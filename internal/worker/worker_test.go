package worker_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/tharunn0/ingestd/internal/dlq"
	"github.com/tharunn0/ingestd/internal/model"
	"github.com/tharunn0/ingestd/internal/worker"
)

func newDLQ(t *testing.T) *dlq.DLQ {
	t.Helper()
	d, err := dlq.New(filepath.Join(t.TempDir(), "dlq.jsonl"))
	if err != nil {
		t.Fatalf("failed to create dlq: %v", err)
	}
	t.Cleanup(func() { d.Close() })
	return d
}

func shutdown(t *testing.T, p *worker.Pool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := p.Shutdown(ctx); err != nil {
		t.Fatalf("expected clean shutdown, got %v", err)
	}
}

func TestPool_StartAndShutdown(t *testing.T) {
	p := worker.New(make(chan model.Job, 10), 4, newDLQ(t))

	if got := p.TotalWorkers(); got != 0 {
		t.Fatalf("expected 0 total workers before Start, got %d", got)
	}

	p.Start()
	if got := p.TotalWorkers(); got != 4 {
		t.Fatalf("expected 4 total workers after Start, got %d", got)
	}
	if got := p.ActiveWorkers(); got != 0 {
		t.Fatalf("expected 0 active workers while idle, got %d", got)
	}

	shutdown(t, p)
	if got := p.TotalWorkers(); got != 0 {
		t.Fatalf("expected 0 total workers after Shutdown, got %d", got)
	}
}

func TestPool_ShutdownDrainsQueue(t *testing.T) {
	jobs := make(chan model.Job, 100)
	for range 100 {
		jobs <- model.Job{Payload: model.RawEvent{"event_name": "click"}}
	}

	p := worker.New(jobs, 3, newDLQ(t))
	p.Start()
	shutdown(t, p)

	if remaining := len(jobs); remaining != 0 {
		t.Fatalf("expected queue to be drained, %d jobs remaining", remaining)
	}
}

func TestPool_ShutdownIsIdempotent(t *testing.T) {
	p := worker.New(make(chan model.Job), 2, newDLQ(t))
	p.Start()

	shutdown(t, p)
	shutdown(t, p)
}

func TestPool_WorkersExitWhenJobsChannelClosed(t *testing.T) {
	jobs := make(chan model.Job)
	p := worker.New(jobs, 2, newDLQ(t))
	p.Start()

	close(jobs)

	deadline := time.Now().Add(time.Second)
	for p.TotalWorkers() != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("expected workers to exit, %d still running", p.TotalWorkers())
		}
		time.Sleep(5 * time.Millisecond)
	}

	shutdown(t, p)
}
