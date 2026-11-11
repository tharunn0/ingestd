package worker_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/tharunn0/ingestd/internal/dlq"
	"github.com/tharunn0/ingestd/internal/model"
	"github.com/tharunn0/ingestd/internal/worker"
)

type senderFunc func(ctx context.Context, event *model.TransformedEvent) error

func (f senderFunc) Send(ctx context.Context, event *model.TransformedEvent) error {
	return f(ctx, event)
}

func silenceLogs(t *testing.T) {
	t.Helper()
	original := log.Logger
	log.Logger = zerolog.Nop()
	t.Cleanup(func() { log.Logger = original })
}

func waitFor(t *testing.T, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal(msg)
		}
		time.Sleep(time.Millisecond)
	}
}

func readDLQ(t *testing.T, path string) []dlq.Record {
	t.Helper()
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("failed to read dlq file: %v", err)
	}

	var records []dlq.Record
	decoder := json.NewDecoder(bytes.NewReader(content))
	for decoder.More() {
		var record dlq.Record
		if err := decoder.Decode(&record); err != nil {
			t.Fatalf("invalid dlq content %q: %v", content, err)
		}
		records = append(records, record)
	}
	return records
}

func sampleJob() model.Job {
	return model.Job{
		TenantID: "org_enterprise_99",
		Payload: model.RawEvent{
			"ev_name": "user_checkout_completed",
			"dim_rk1": "currency", "dim_rv1": "USD", "dim_rt1": "string",
		},
	}
}

func TestPool_DeliversTransformedEvent(t *testing.T) {
	var (
		mu       sync.Mutex
		received []*model.TransformedEvent
	)
	sender := senderFunc(func(_ context.Context, event *model.TransformedEvent) error {
		mu.Lock()
		defer mu.Unlock()
		received = append(received, event)
		return nil
	})

	jobs := make(chan model.Job, 1)
	jobs <- sampleJob()

	d := newDLQ(t)
	p := worker.New(jobs, 2, sender, d, testRetry)
	p.Start()
	shutdown(t, p)

	if len(received) != 1 {
		t.Fatalf("expected 1 delivered event, got %d", len(received))
	}
	event := received[0]
	if event.EventName != "user_checkout_completed" || event.OrganizationID != "org_enterprise_99" {
		t.Errorf("unexpected event delivered: %+v", event)
	}
	if event.Dimensions["currency"].Value != "USD" {
		t.Errorf("expected transformed dimensions, got %+v", event.Dimensions)
	}
	if got := p.Processed(); got != 1 {
		t.Errorf("expected 1 processed event, got %d", got)
	}
	if got := d.Metrics().FailedEvents; got != 0 {
		t.Errorf("expected no dead-lettered events, got %d", got)
	}
}

func TestPool_RetriesUntilDeliverySucceeds(t *testing.T) {
	var calls atomic.Int32
	sender := senderFunc(func(context.Context, *model.TransformedEvent) error {
		if calls.Add(1) < 3 {
			return errors.New("downstream responded with status 503")
		}
		return nil
	})

	jobs := make(chan model.Job, 1)
	jobs <- sampleJob()

	d := newDLQ(t)
	p := worker.New(jobs, 1, sender, d, testRetry)
	p.Start()
	shutdown(t, p)

	if got := calls.Load(); got != 3 {
		t.Errorf("expected 3 attempts, got %d", got)
	}
	if got := p.Processed(); got != 1 {
		t.Errorf("expected 1 processed event, got %d", got)
	}
	if got := d.Metrics().FailedEvents; got != 0 {
		t.Errorf("expected no dead-lettered events, got %d", got)
	}
}

func TestPool_DeadLettersAfterExhaustingAttempts(t *testing.T) {
	silenceLogs(t)

	var calls atomic.Int32
	sender := senderFunc(func(context.Context, *model.TransformedEvent) error {
		calls.Add(1)
		return errors.New("downstream responded with status 503")
	})

	path := filepath.Join(t.TempDir(), "dlq.jsonl")
	d, err := dlq.New(path)
	if err != nil {
		t.Fatalf("failed to create dlq: %v", err)
	}
	defer d.Close()

	jobs := make(chan model.Job, 1)
	jobs <- sampleJob()

	p := worker.New(jobs, 1, sender, d, testRetry)
	p.Start()
	shutdown(t, p)

	if got := calls.Load(); got != 3 {
		t.Errorf("expected exactly 3 attempts, got %d", got)
	}
	if got := p.Processed(); got != 0 {
		t.Errorf("expected failed event not to count as processed, got %d", got)
	}

	records := readDLQ(t, path)
	if len(records) != 1 {
		t.Fatalf("expected 1 dlq record, got %d", len(records))
	}
	record := records[0]
	if record.RetryAttempts != 3 {
		t.Errorf("expected 3 retry attempts, got %d", record.RetryAttempts)
	}
	if record.Reason != "max attempts reached: downstream responded with status 503" {
		t.Errorf("unexpected reason %q", record.Reason)
	}
	if record.TenantID != "org_enterprise_99" || record.Payload.EventName != "user_checkout_completed" {
		t.Errorf("unexpected dlq record: %+v", record)
	}
}

func TestPool_BacksOffExponentiallyBetweenAttempts(t *testing.T) {
	silenceLogs(t)

	var (
		mu    sync.Mutex
		times []time.Time
	)
	sender := senderFunc(func(context.Context, *model.TransformedEvent) error {
		mu.Lock()
		defer mu.Unlock()
		times = append(times, time.Now())
		return errors.New("timeout")
	})

	jobs := make(chan model.Job, 1)
	jobs <- sampleJob()

	const base = 40 * time.Millisecond
	p := worker.New(jobs, 1, sender, newDLQ(t), worker.RetryPolicy{MaxAttempts: 3, BaseBackoff: base})
	p.Start()
	shutdown(t, p)

	if len(times) != 3 {
		t.Fatalf("expected 3 attempts, got %d", len(times))
	}
	if first := times[1].Sub(times[0]); first < base {
		t.Errorf("expected first backoff >= %s, got %s", base, first)
	}
	if second := times[2].Sub(times[1]); second < 2*base {
		t.Errorf("expected second backoff >= %s, got %s", 2*base, second)
	}
}

func TestPool_FailingEventDoesNotBlockOthers(t *testing.T) {
	silenceLogs(t)

	sender := senderFunc(func(_ context.Context, event *model.TransformedEvent) error {
		if event.EventName == "poison" {
			return errors.New("timeout")
		}
		return nil
	})

	jobs := make(chan model.Job, 21)
	jobs <- model.Job{Payload: model.RawEvent{"ev_name": "poison"}}
	for range 20 {
		jobs <- sampleJob()
	}

	d := newDLQ(t)
	p := worker.New(jobs, 4, sender, d, worker.RetryPolicy{MaxAttempts: 3, BaseBackoff: 100 * time.Millisecond})
	p.Start()

	waitFor(t, func() bool { return p.Processed() == 20 }, "expected healthy events to be delivered while one retries")
	if got := d.Metrics().FailedEvents; got != 0 {
		t.Errorf("expected poison event to still be retrying, got %d dead-lettered", got)
	}

	shutdown(t, p)
	if got := d.Metrics().FailedEvents; got != 1 {
		t.Errorf("expected 1 dead-lettered event, got %d", got)
	}
}

func TestPool_ActiveWorkersCountsBusyWorkers(t *testing.T) {
	release := make(chan struct{})
	sender := senderFunc(func(context.Context, *model.TransformedEvent) error {
		<-release
		return nil
	})

	jobs := make(chan model.Job, 2)
	p := worker.New(jobs, 4, sender, newDLQ(t), testRetry)
	p.Start()

	jobs <- sampleJob()
	jobs <- sampleJob()

	waitFor(t, func() bool { return p.ActiveWorkers() == 2 }, "expected 2 active workers")
	if got := p.TotalWorkers(); got != 4 {
		t.Fatalf("expected 4 total workers, got %d", got)
	}

	close(release)
	waitFor(t, func() bool { return p.ActiveWorkers() == 0 }, "expected 0 active workers after jobs finish")
	shutdown(t, p)
}

func TestPool_ShutdownTimeoutCancelsInFlightDelivery(t *testing.T) {
	silenceLogs(t)

	sender := senderFunc(func(ctx context.Context, _ *model.TransformedEvent) error {
		<-ctx.Done()
		return ctx.Err()
	})

	jobs := make(chan model.Job, 1)
	d := newDLQ(t)
	p := worker.New(jobs, 1, sender, d, worker.RetryPolicy{MaxAttempts: 3, BaseBackoff: time.Hour})
	p.Start()

	jobs <- sampleJob()
	waitFor(t, func() bool { return p.ActiveWorkers() == 1 }, "expected worker to pick up the job")

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	if err := p.Shutdown(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected DeadlineExceeded, got %v", err)
	}

	waitFor(t, func() bool { return p.TotalWorkers() == 0 }, "expected worker to exit after cancellation")
	if got := d.Metrics().FailedEvents; got != 1 {
		t.Errorf("expected the cancelled event to be dead-lettered, got %d", got)
	}
}
