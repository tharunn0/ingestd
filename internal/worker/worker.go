package worker

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/tharunn0/ingestd/internal/dlq"
	"github.com/tharunn0/ingestd/internal/model"
)

type Sender interface {
	Send(ctx context.Context, event *model.TransformedEvent) error
}

type RetryPolicy struct {
	MaxAttempts int
	BaseBackoff time.Duration
}

// Pool refer to the worker pools that process from the jobs chan
type Pool struct {
	jobs   <-chan model.Job
	size   int
	sender Sender
	dlq    *dlq.DLQ
	retry  RetryPolicy

	ctx    context.Context
	cancel context.CancelFunc

	stop     chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup

	total  atomic.Int32
	active atomic.Int32

	processed atomic.Uint64
}

func New(jobs <-chan model.Job, size int, sender Sender, deadLetters *dlq.DLQ, retry RetryPolicy) *Pool {
	ctx, cancel := context.WithCancel(context.Background())
	return &Pool{
		jobs:   jobs,
		size:   size,
		sender: sender,
		dlq:    deadLetters,
		retry:  retry,
		ctx:    ctx,
		cancel: cancel,
		stop:   make(chan struct{}),
	}
}

func (p *Pool) Start() {
	for range p.size {
		p.wg.Go(p.run)
	}
}

func (p *Pool) Shutdown(ctx context.Context) error {
	p.stopOnce.Do(func() { close(p.stop) })

	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()

	defer p.cancel()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *Pool) TotalWorkers() int {
	return int(p.total.Load())
}

func (p *Pool) ActiveWorkers() int {
	return int(p.active.Load())
}

func (p *Pool) Processed() uint64 {
	return p.processed.Load()
}

func (p *Pool) run() {
	p.total.Add(1)
	defer p.total.Add(-1)

	for {
		select {
		case job, ok := <-p.jobs:
			if !ok {
				return
			}
			p.process(job)
		case <-p.stop:
			p.drain()
			return
		}
	}
}

func (p *Pool) drain() {
	for {
		select {
		case job, ok := <-p.jobs:
			if !ok {
				return
			}
			p.process(job)
		default:
			return
		}
	}
}

func (p *Pool) process(job model.Job) {
	p.active.Add(1)
	defer p.active.Add(-1)
	event := model.Transform(job)

	attempts, err := p.deliver(event)
	if err != nil {
		p.deadLetter(event, attempts, err)
		return
	}
	p.processed.Add(1)
}

func (p *Pool) deliver(event *model.TransformedEvent) (int, error) {
	backoff := p.retry.BaseBackoff

	for attempt := 1; ; attempt++ {
		err := p.sender.Send(p.ctx, event)
		if err == nil {
			return attempt, nil
		}
		if attempt >= p.retry.MaxAttempts {
			return attempt, err
		}

		timer := time.NewTimer(backoff)
		select {
		case <-timer.C:
		case <-p.ctx.Done():
			timer.Stop()
			return attempt, err
		}
		backoff *= 2
	}
}

func (p *Pool) deadLetter(event *model.TransformedEvent, attempts int, cause error) {
	record := dlq.Record{
		TenantID:      event.OrganizationID,
		RetryAttempts: attempts,
		Reason:        cause.Error(),
		Payload:       *event,
	}
	failure := log.Warn()
	msg := "event dead-lettered"
	if err := p.dlq.Enqueue(record); err != nil {
		failure = log.Error().AnErr("dlq_error", err)
		msg = "event lost: dead-letter write failed"
	}

	failure.
		Str("tenant_id", record.TenantID).
		Str("event_name", event.EventName).
		Int("retry_attempts", attempts).
		Str("reason", record.Reason).
		Msg(msg)
}
