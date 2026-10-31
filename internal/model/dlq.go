package model

import "time"

// DLQRecord represents a failed event written to the Dead Letter Queue
// fallback file (dlq_failed_events.jsonl) after all retry attempts are exhausted.
type DLQRecord struct {
	EventID    string           `json:"event_id,omitempty"`
	TenantID   string           `json:"tenant_id"`
	FailedAt   time.Time        `json:"failed_at"`
	RetryCount int              `json:"retry_count"`
	LastError  string           `json:"last_error"`
	Payload    TransformedEvent `json:"payload"`
}
