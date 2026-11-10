package model

import "time"

type RawEvent map[string]string

type Job struct {
	TenantID   string
	ReceivedAt time.Time
	Payload    RawEvent
}

type TypedValue struct {
	Value string `json:"value"`
	Type  string `json:"type"`
}

type ClientMetadata struct {
	IPAddress string `json:"ip_address"`
	UserAgent string `json:"user_agent"`
}

type TransformedEvent struct {
	EventName      string                `json:"event_name"`
	EventCategory  string                `json:"event_category"`
	OrganizationID string                `json:"organization_id"`
	SessionID      string                `json:"session_id"`
	ClientMetadata ClientMetadata        `json:"client_metadata"`
	Dimensions     map[string]TypedValue `json:"dimensions"`
	Metrics        map[string]TypedValue `json:"metrics"`
}
