package metrics

import (
	"errors"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/tharunn0/ingestd/internal/dlq"
	"github.com/tharunn0/ingestd/internal/queue"
	"github.com/tharunn0/ingestd/internal/worker"
)

var (
	EventsReceived = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ingestd_events_received_total",
		Help: "Telemetry events accepted onto the ingestion queue.",
	})
	EventsRejected = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ingestd_events_rejected_total",
		Help: "Telemetry events rejected because the ingestion queue was full or closed.",
	})
)

// Register exposes live queue, worker and DLQ state on reg. The values are
// read from each component at scrape time, so nothing is added to the hot path.
func Register(reg prometheus.Registerer, q *queue.IngestionQueue, w *worker.Pool, d *dlq.DLQ) error {
	collectors := []prometheus.Collector{
		gauge("ingestd_queue_depth", "Jobs currently buffered in the ingestion queue.", q.Len),
		gauge("ingestd_queue_capacity", "Maximum number of jobs the ingestion queue can buffer.", q.Capacity),
		gauge("ingestd_workers_active", "Workers currently processing a job.", w.ActiveWorkers),
		gauge("ingestd_workers_total", "Worker goroutines currently running.", w.TotalWorkers),
		counter("ingestd_events_processed_total", "Telemetry events taken off the queue and processed by a worker.", w.Processed),
		counter("ingestd_dlq_events_total", "Events handed to the dead letter queue.", func() uint64 {
			return d.Metrics().FailedEvents
		}),
		counter("ingestd_dlq_write_errors_total", "Dead letter records that could not be persisted.", func() uint64 {
			return d.Metrics().WriteErrors
		}),
	}

	var errs []error
	for _, c := range collectors {
		errs = append(errs, reg.Register(c))
	}
	return errors.Join(errs...)
}

func gauge(name, help string, read func() int) prometheus.Collector {
	return prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: name, Help: help}, func() float64 {
		return float64(read())
	})
}

func counter(name, help string, read func() uint64) prometheus.Collector {
	return prometheus.NewCounterFunc(prometheus.CounterOpts{Name: name, Help: help}, func() float64 {
		return float64(read())
	})
}
