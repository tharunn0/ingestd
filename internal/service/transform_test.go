package service_test

import (
	"errors"
	"testing"
	"time"

	"github.com/tharunn0/ingestd/internal/model"
	"github.com/tharunn0/ingestd/internal/queue"
	"github.com/tharunn0/ingestd/internal/service"
)

func TestTransformService_Enqueue(t *testing.T) {
	q := queue.New(1)
	s := service.NewTransformService(q)

	job := model.Job{ReceivedAt: time.Now(), Payload: model.RawEvent{"event_name": "click"}}
	if err := s.Enqueue(job); err != nil {
		t.Fatalf("expected enqueue to succeed, got %v", err)
	}

	select {
	case got := <-q.Jobs():
		if got.Payload["event_name"] != "click" {
			t.Fatalf("expected payload event_name 'click', got %q", got.Payload["event_name"])
		}
	default:
		t.Fatal("expected job to be on the queue")
	}
}

func TestTransformService_EnqueueFull(t *testing.T) {
	s := service.NewTransformService(queue.New(1))

	if err := s.Enqueue(model.Job{}); err != nil {
		t.Fatalf("expected first enqueue to succeed, got %v", err)
	}

	if err := s.Enqueue(model.Job{}); !errors.Is(err, queue.ErrQueueFull) {
		t.Fatalf("expected ErrQueueFull, got %v", err)
	}
}
