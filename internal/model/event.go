package model

import "time"

// RawEvent represents the dynamic, flat incoming JSON payload.
// All dynamic dimension and metric triplets (e.g. dim_rk1, dim_rv1, dim_rt1)
// are captured as dynamic string key-values.
type RawEvent map[string]string

// Job encapsulates an event accepted by the HTTP layer and placed on the
// internal worker queue.
type Job struct {
	TenantID   string
	ReceivedAt time.Time
	Payload    RawEvent
}

// TypedValue represents a dynamic dimension or metric entry with a value and a type.
type TypedValue struct {
	Value string `json:"value"`
	Type  string `json:"type"`
}

// ClientMetadata holds contextual client information.
type ClientMetadata struct {
	IPAddress string `json:"ip_address"`
	UserAgent string `json:"user_agent"`
}

// TransformedEvent represents the normalized, strongly typed schema forwarded
// downstream.
type TransformedEvent struct {
	EventName      string                `json:"event_name"`
	EventCategory  string                `json:"event_category"`
	OrganizationID string                `json:"organization_id"`
	SessionID      string                `json:"session_id"`
	ClientMetadata ClientMetadata        `json:"client_metadata"`
	Dimensions     map[string]TypedValue `json:"dimensions"`
	Metrics        map[string]TypedValue `json:"metrics"`
}
