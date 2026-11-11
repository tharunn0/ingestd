package model

import (
	"cmp"
	"slices"
	"strings"
	"time"
)

// INPUT (flat, numbered slots)

// {
//   "ev_name":     "user_checkout_completed",   ← event name
//   "ev_cat":      "ecommerce",                 ← event category
//   "tenant_id":   "org_enterprise_99",         ← org / tenant
//   "session_id":  "sess_abc123xyz",            ← session
//   "client_ip":   "192.168.1.55",              ← client metadata
//   "user_agent":  "Mozilla/5.0",               ← client metadata

//   "dim_rk1": "currency",        ┐
//   "dim_rv1": "USD",             ├ slot 1: key / value / type
//   "dim_rt1": "string",          ┘
//   "dim_rk2": "payment_gateway", ┐
//   "dim_rv2": "stripe",          ├ slot 2
//   "dim_rt2": "string",          ┘
//   ... (dim_rk<N> / dim_rv<N> / dim_rt<N>)

//   "met_rk1": "cart_total_cents",┐
//   "met_rv1": "4999",            ├ slot 1: key / value / type
//   "met_rt1": "integer",         ┘
//   "met_rk2": "items_count",     ┐
//   "met_rv2": "3",               ├ slot 2
//   "met_rt2": "integer",         ┘
//   ... (met_rk<N> / met_rv<N> / met_rt<N>)
// }

// OUTPUT (nested, keyed maps)

// {
//   "event_name":       "user_checkout_completed",   ← ev_name
//   "event_category":   "ecommerce",                 ← ev_cat
//   "organization_id":  "org_enterprise_99",         ← tenant_id
//   "session_id":       "sess_abc123xyz",            ← session_id

//   "client_metadata": {                             ← client_ip + user_agent
//     "ip_address": "192.168.1.55",
//     "user_agent": "Mozilla/5.0"
//   },

//   "dimensions": {                                  ← folded dim_* slots
//     "currency":        { "value": "USD",    "type": "string"  },  ← dim_rk1
//     "payment_gateway": { "value": "stripe", "type": "string"  }   ← dim_rk2
//   },

//   "metrics": {                                     ← folded met_* slots
//     "cart_total_cents": { "value": "4999", "type": "integer" },   ← met_rk1
//     "items_count":      { "value": "3",    "type": "integer" }    ← met_rk2
//   }
// }

// FIELD MAP

// ev_name     → event_name
// ev_cat      → event_category
// tenant_id   → organization_id
// session_id  → session_id
// client_ip   → client_metadata.ip_address
// user_agent  → client_metadata.user_agent
// dim_rk<N>   → dimensions[<rk value>].value  ← dim_rv<N>
// dim_rt<N>   → dimensions[<rk value>].type
// met_rk<N>   → metrics[<rk value>].value     ← met_rv<N>
// met_rt<N>   → metrics[<rk value>].type
func Transform(job Job) *TransformedEvent {
	p := job.Payload

	return &TransformedEvent{
		EventName:      p["ev_name"],
		EventCategory:  p["ev_cat"],
		OrganizationID: job.TenantID,
		SessionID:      p["session_id"],
		ClientMetadata: ClientMetadata{
			IPAddress: p["client_ip"],
			UserAgent: p["user_agent"],
		},
		Dimensions: foldSlots(p, dimensionSlots),
		Metrics:    foldSlots(p, metricSlots),
	}
}

type slotKeys struct {
	key, value, typ string
}

var (
	dimensionSlots = slotKeys{key: "dim_rk", value: "dim_rv", typ: "dim_rt"}
	metricSlots    = slotKeys{key: "met_rk", value: "met_rv", typ: "met_rt"}
)

func foldSlots(p RawEvent, keys slotKeys) map[string]TypedValue {
	var backing [16]string
	slots := backing[:0]

	for k := range p {
		if idx, ok := strings.CutPrefix(k, keys.key); ok && isDigits(idx) {
			slots = append(slots, idx)
		}
	}

	slices.SortFunc(slots, func(a, b string) int {
		return cmp.Or(cmp.Compare(len(a), len(b)), strings.Compare(a, b))
	})

	out := make(map[string]TypedValue, len(slots))
	for _, idx := range slots {
		name := slotValue(p, keys.key, idx)
		value := slotValue(p, keys.value, idx)
		typ := slotValue(p, keys.typ, idx)
		if name == "" || value == "" || typ == "" {
			continue
		}
		out[name] = TypedValue{Value: value, Type: typ}
	}

	return out
}

// slotValue builds the lookup key in a stack buffer; the compiler elides the
// string conversion for map lookups, keeping the hot path allocation-free.
func slotValue(p RawEvent, prefix, idx string) string {
	var buf [32]byte
	key := append(append(buf[:0], prefix...), idx...)
	return p[string(key)]
}

func isDigits(s string) bool {
	if s == "" {
		return false
	}
	for i := range len(s) {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

// NOTE: values stay strings; "type" is descriptive metadata, not a coercion hint.

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
