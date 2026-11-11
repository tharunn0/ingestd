package downstream_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/tharunn0/ingestd/internal/downstream"
	"github.com/tharunn0/ingestd/internal/model"
)

func sampleEvent() *model.TransformedEvent {
	return model.Transform(model.Job{
		TenantID: "org_enterprise_99",
		Payload: model.RawEvent{
			"ev_name": "user_checkout_completed",
			"dim_rk1": "currency", "dim_rv1": "USD", "dim_rt1": "string",
		},
	})
}

func TestClient_SendPostsEventAsJSON(t *testing.T) {
	var (
		method, contentType string
		received            model.TransformedEvent
	)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		method = r.Method
		contentType = r.Header.Get("Content-Type")
		body, _ := io.ReadAll(r.Body)
		if err := json.Unmarshal(body, &received); err != nil {
			t.Errorf("invalid JSON body %q: %v", body, err)
		}
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()

	c := downstream.New(server.URL, time.Second)
	if err := c.Send(context.Background(), sampleEvent()); err != nil {
		t.Fatalf("expected send to succeed, got %v", err)
	}

	if method != http.MethodPost {
		t.Errorf("expected POST, got %s", method)
	}
	if !strings.HasPrefix(contentType, "application/json") {
		t.Errorf("expected JSON content type, got %q", contentType)
	}
	if received.EventName != "user_checkout_completed" || received.OrganizationID != "org_enterprise_99" {
		t.Errorf("unexpected event received: %+v", received)
	}
	if received.Dimensions["currency"].Value != "USD" {
		t.Errorf("expected currency dimension, got %+v", received.Dimensions)
	}
}

func TestClient_SendFailsOnNon2xx(t *testing.T) {
	for _, status := range []int{http.StatusBadRequest, http.StatusInternalServerError, http.StatusServiceUnavailable} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(status)
			}))
			defer server.Close()

			err := downstream.New(server.URL, time.Second).Send(context.Background(), sampleEvent())
			if err == nil {
				t.Fatalf("expected error for status %d", status)
			}
			if !strings.Contains(err.Error(), http.StatusText(status)) && !strings.Contains(err.Error(), "status") {
				t.Fatalf("expected status in error, got %v", err)
			}
		})
	}
}

func TestClient_SendTimesOut(t *testing.T) {
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {
		<-release
	}))
	defer server.Close()
	defer close(release)

	start := time.Now()
	err := downstream.New(server.URL, 50*time.Millisecond).Send(context.Background(), sampleEvent())
	if err == nil {
		t.Fatal("expected timeout error")
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("expected send to give up near the timeout, took %s", elapsed)
	}
}

func TestClient_SendHonoursContextCancellation(t *testing.T) {
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {
		<-release
	}))
	defer server.Close()
	defer close(release)

	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(30*time.Millisecond, cancel)

	start := time.Now()
	err := downstream.New(server.URL, 5*time.Second).Send(ctx, sampleEvent())
	if err == nil {
		t.Fatal("expected cancellation error")
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("expected send to stop on cancellation, took %s", elapsed)
	}
}

func TestClient_SendFailsWhenUnreachable(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	url := server.URL
	server.Close()

	if err := downstream.New(url, time.Second).Send(context.Background(), sampleEvent()); err == nil {
		t.Fatal("expected error for unreachable downstream")
	}
}
