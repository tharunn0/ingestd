package queue_test

import (
	"testing"

	"github.com/tharunn0/ingestd/internal/model"
	"github.com/tharunn0/ingestd/internal/queue"
)

func TestQueue_Instantiation(t *testing.T) {
	q := queue.New(100)
	if q == nil {
		t.Fatal("expected non-nil queue instance")
	}

	if q.Capacity() != 100 {
		t.Fatalf("expected capacity 100, got %d", q.Capacity())
	}

	if q.Len() != 0 {
		t.Fatalf("expected empty queue, got length %d", q.Len())
	}

	if q.Jobs() == nil {
		t.Fatal("expected non-nil jobs channel")
	}
}

func TestQueue_LenTracksBufferedJobs(t *testing.T) {
	q := queue.New(5)

	for range 3 {
		if err := q.Enqueue(model.Job{}); err != nil {
			t.Fatalf("expected enqueue to succeed, got %v", err)
		}
	}
	if q.Len() != 3 {
		t.Fatalf("expected length 3, got %d", q.Len())
	}

	<-q.Jobs()
	if q.Len() != 2 {
		t.Fatalf("expected length 2 after a receive, got %d", q.Len())
	}
}
