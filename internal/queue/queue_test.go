package queue_test

import (
	"testing"

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

	if q.Jobs() == nil {
		t.Fatal("expected non-nil jobs channel")
	}
}
