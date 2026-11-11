package worker

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/tharunn0/ingestd/internal/model"
)

type failingSender struct{ cause error }

func (f failingSender) Send(context.Context, *model.TransformedEvent) error { return f.cause }

func TestPool_DeliverWrapsCauseWhenAttemptsExhausted(t *testing.T) {
	cause := errors.New("downstream responded with status 503")
	p := New(make(chan model.Job), 1, failingSender{cause}, nil, RetryPolicy{MaxAttempts: 3, BaseBackoff: time.Millisecond})

	attempts, err := p.deliver(&model.TransformedEvent{})

	if attempts != 3 {
		t.Errorf("expected 3 attempts, got %d", attempts)
	}
	if !errors.Is(err, ErrMaxAttemptReached) {
		t.Errorf("expected ErrMaxAttemptReached, got %v", err)
	}
	if !errors.Is(err, cause) {
		t.Errorf("expected the downstream cause to be preserved, got %v", err)
	}
}

func TestPool_DeliverCancelledIsNotMaxAttempts(t *testing.T) {
	cause := errors.New("timeout")
	p := New(make(chan model.Job), 1, failingSender{cause}, nil, RetryPolicy{MaxAttempts: 3, BaseBackoff: time.Hour})
	p.cancel()

	attempts, err := p.deliver(&model.TransformedEvent{})

	if attempts != 1 {
		t.Errorf("expected delivery to stop after 1 attempt, got %d", attempts)
	}
	if errors.Is(err, ErrMaxAttemptReached) {
		t.Errorf("expected cancellation not to report max attempts, got %v", err)
	}
	if !errors.Is(err, cause) {
		t.Errorf("expected the downstream cause to be preserved, got %v", err)
	}
}
