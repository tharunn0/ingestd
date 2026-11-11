package worker

import (
	"context"
	"log"
	"sync"
	"sync/atomic"

	"github.com/tharunn0/ingestd/internal/dlq"
	"github.com/tharunn0/ingestd/internal/model"
)

type Pool struct {
	jobs <-chan model.Job
	size int
	dlq  *dlq.DLQ

	ctx    context.Context
	cancel context.CancelFunc

	stop     chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup

	total  atomic.Int32
	active atomic.Int32
}

func New(jobs <-chan model.Job, size int, deadLetters *dlq.DLQ) *Pool {
	ctx, cancel := context.WithCancel(context.Background())
	return &Pool{
		jobs:   jobs,
		size:   size,
		dlq:    deadLetters,
		ctx:    ctx,
		cancel: cancel,
		stop:   make(chan struct{}),
	}
}

func (p *Pool) Start() {
	for range p.size {
		p.total.Add(1)
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

func (p *Pool) run() {
	defer p.total.Add(-1)

	for {
		select {
		case job, ok := <-p.jobs:
			if !ok {
				return
			}
			p.execute(job)
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
			p.execute(job)
		default:
			return
		}
	}
}

func (p *Pool) execute(job model.Job) {
	p.active.Add(1)
	defer p.active.Add(-1)
	p.process(p.ctx, job)
}

func (p *Pool) process(ctx context.Context, job model.Job) {
	// TODO: transform job.Payload, deliver downstream with retry, and call
	// p.deadLetter once the attempts are exhausted.
	_, _ = ctx, job
}

func (p *Pool) deadLetter(event *model.TransformedEvent, attempts int, cause error) {
	record := dlq.Record{
		TenantID:      event.OrganizationID,
		RetryAttempts: attempts,
		Reason:        cause.Error(),
		Payload:       *event,
	}
	if err := p.dlq.Enqueue(record); err != nil {
		log.Printf("worker: failed to dead-letter event %q: %v", event.EventName, err)
	}
}
