package dlq

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tharunn0/ingestd/internal/model"
)

const DefaultPath = "dlq_failed_events.jsonl"

type Record struct {
	TenantID      string                 `json:"tenant_id"`
	FailedAt      time.Time              `json:"failed_at"`
	RetryAttempts int                    `json:"retry_attempts"`
	Reason        string                 `json:"reason"`
	Payload       model.TransformedEvent `json:"payload"`
}

type Metrics struct {
	FailedEvents uint64 `json:"failed_events"`
	WriteErrors  uint64 `json:"write_errors"`
}

type DLQ struct {
	mu   sync.Mutex
	file *os.File

	failedEvents atomic.Uint64
	writeErrors  atomic.Uint64
}

func New(path string) (*DLQ, error) {
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open dlq file: %w", err)
	}
	return &DLQ{file: file}, nil
}

func (d *DLQ) Enqueue(record Record) error {
	d.failedEvents.Add(1)

	if record.FailedAt.IsZero() {
		record.FailedAt = time.Now().UTC()
	}

	if err := d.write(record); err != nil {
		d.writeErrors.Add(1)
		return err
	}
	return nil
}

func (d *DLQ) write(record Record) error {
	line, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("encode dlq record: %w", err)
	}
	line = append(line, '\n')

	d.mu.Lock()
	defer d.mu.Unlock()

	if _, err := d.file.Write(line); err != nil {
		return fmt.Errorf("write dlq record: %w", err)
	}
	return nil
}

func (d *DLQ) Metrics() Metrics {
	return Metrics{
		FailedEvents: d.failedEvents.Load(),
		WriteErrors:  d.writeErrors.Load(),
	}
}

func (d *DLQ) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if err := d.file.Sync(); err != nil {
		d.file.Close()
		return fmt.Errorf("sync dlq file: %w", err)
	}
	return d.file.Close()
}
