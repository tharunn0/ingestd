package queue

import (
	"errors"

	"github.com/tharunn0/ingestd/internal/model"
)

var (
	ErrQueueFull   = errors.New("ingestion queue is full")
	ErrQueueClosed = errors.New("ingestion queue is closed")
)

type IngestionQueue struct {
	jobs     chan model.Job
	capacity int

	closed chan struct{}
}

func New(capacity int) *IngestionQueue {
	return &IngestionQueue{
		jobs:     make(chan model.Job, capacity),
		capacity: capacity,
		closed:   make(chan struct{}),
	}
}

func (q *IngestionQueue) Enqueue(job model.Job) error {

	select {
	case <-q.closed:
		return ErrQueueClosed
	default:
	}

	select {
	case q.jobs <- job:
		return nil

	case <-q.closed:
		return ErrQueueClosed

	default:
		return ErrQueueFull
	}
}

func (q *IngestionQueue) Jobs() <-chan model.Job {
	return q.jobs
}

func (q *IngestionQueue) Len() int {
	return len(q.jobs)
}

func (q *IngestionQueue) Capacity() int {
	return q.capacity
}

func (q *IngestionQueue) Close() {
	close(q.closed)
}
