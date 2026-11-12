package bench_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/gofiber/fiber/v3"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/tharunn0/ingestd/internal/http/handlers"
	"github.com/tharunn0/ingestd/internal/queue"
	"github.com/tharunn0/ingestd/internal/service"
	"github.com/valyala/fasthttp"
)

const sampleEvent = `{
	"ev_name": "user_checkout_completed",
	"ev_cat": "ecommerce",
	"tenant_id": "org_enterprise_99",
	"session_id": "sess_abc123xyz",
	"client_ip": "192.168.1.55",
	"user_agent": "Mozilla/5.0",
	"dim_rk1": "currency", "dim_rv1": "USD", "dim_rt1": "string",
	"dim_rk2": "payment_gateway", "dim_rv2": "stripe", "dim_rt2": "string",
	"met_rk1": "cart_total_cents", "met_rv1": "4999", "met_rt1": "integer",
	"met_rk2": "items_count", "met_rv2": "3", "met_rt2": "integer"
}`

// largeEvent builds an event with the given number of dimension and metric slots each.
func largeEvent(slots int) string {
	var sb strings.Builder
	sb.WriteString(`{"ev_name":"user_checkout_completed","ev_cat":"ecommerce","tenant_id":"org_enterprise_99"`)
	for i := 1; i <= slots; i++ {
		fmt.Fprintf(&sb, `,"dim_rk%d":"dimension_%d","dim_rv%d":"value_%d","dim_rt%d":"string"`, i, i, i, i, i)
		fmt.Fprintf(&sb, `,"met_rk%d":"metric_%d","met_rv%d":"%d","met_rt%d":"integer"`, i, i, i, i*100, i)
	}
	sb.WriteString("}")
	return sb.String()
}

// newIngest mounts only the telemetry handler, so the numbers exclude the
// network, the auth middleware and anything the workers do.
func newIngest(q *queue.IngestionQueue) fasthttp.RequestHandler {
	h := handlers.NewTelemetryHandler(service.NewTransformService(q))
	app := fiber.New()
	app.Post("/telemetry", h.Ingest)
	return app.Handler()
}

func newRequest(body string) *fasthttp.RequestCtx {
	ctx := &fasthttp.RequestCtx{}
	ctx.Request.Header.SetMethod(fiber.MethodPost)
	ctx.Request.Header.SetContentType(fiber.MIMEApplicationJSON)
	ctx.Request.SetRequestURI("/telemetry")
	ctx.Request.SetBodyString(body)
	return ctx
}

func BenchmarkIngest(b *testing.B) {
	cases := []struct {
		name string
		body string
	}{
		{"sample_18_fields", sampleEvent},
		{"large_50_slots", largeEvent(50)},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			q := queue.New(16)
			handle := newIngest(q)
			ctx := newRequest(tc.body)

			b.SetBytes(int64(len(tc.body)))
			b.ReportAllocs()

			for b.Loop() {
				handle(ctx)
				if status := ctx.Response.StatusCode(); status != fiber.StatusAccepted {
					b.Fatalf("expected status 202, got %d", status)
				}
				<-q.Jobs()
			}
		})
	}
}

func BenchmarkIngestParallel(b *testing.B) {
	q := queue.New(1024)
	handle := newIngest(q)

	b.SetBytes(int64(len(sampleEvent)))
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		ctx := newRequest(sampleEvent)
		for pb.Next() {
			handle(ctx)
			if status := ctx.Response.StatusCode(); status != fiber.StatusAccepted {
				b.Errorf("expected status 202, got %d", status)
				return
			}
			<-q.Jobs()
		}
	})
}

// BenchmarkIngestQueueFull measures the backpressure path: every request is
// rejected with 503 because the queue has no room.
func BenchmarkIngestQueueFull(b *testing.B) {
	original := log.Logger
	log.Logger = zerolog.Nop()
	b.Cleanup(func() { log.Logger = original })

	q := queue.New(1)
	handle := newIngest(q)
	ctx := newRequest(sampleEvent)

	handle(ctx)
	if status := ctx.Response.StatusCode(); status != fiber.StatusAccepted {
		b.Fatalf("expected the first request to fill the queue with 202, got %d", status)
	}

	b.ReportAllocs()

	for b.Loop() {
		handle(ctx)
		if status := ctx.Response.StatusCode(); status != fiber.StatusServiceUnavailable {
			b.Fatalf("expected status 503, got %d", status)
		}
	}
}
