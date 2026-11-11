package model_test

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/tharunn0/ingestd/internal/model"
)

func sampleJob() model.Job {
	return model.Job{Payload: model.RawEvent{
		"ev_name":    "user_checkout_completed",
		"ev_cat":     "ecommerce",
		"tenant_id":  "org_enterprise_99",
		"session_id": "sess_abc123xyz",
		"client_ip":  "192.168.1.55",
		"user_agent": "Mozilla/5.0",
		"dim_rk1":    "currency",
		"dim_rv1":    "USD",
		"dim_rt1":    "string",
		"dim_rk2":    "payment_gateway",
		"dim_rv2":    "stripe",
		"dim_rt2":    "string",
		"met_rk1":    "cart_total_cents",
		"met_rv1":    "4999",
		"met_rt1":    "integer",
		"met_rk2":    "items_count",
		"met_rv2":    "3",
		"met_rt2":    "integer",
	}}
}

func TestTransform_Sample(t *testing.T) {
	got := model.Transform(sampleJob())

	want := &model.TransformedEvent{
		EventName:      "user_checkout_completed",
		EventCategory:  "ecommerce",
		OrganizationID: "org_enterprise_99",
		SessionID:      "sess_abc123xyz",
		ClientMetadata: model.ClientMetadata{
			IPAddress: "192.168.1.55",
			UserAgent: "Mozilla/5.0",
		},
		Dimensions: map[string]model.TypedValue{
			"currency":        {Value: "USD", Type: "string"},
			"payment_gateway": {Value: "stripe", Type: "string"},
		},
		Metrics: map[string]model.TypedValue{
			"cart_total_cents": {Value: "4999", Type: "integer"},
			"items_count":      {Value: "3", Type: "integer"},
		},
	}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected transform result:\n got: %+v\nwant: %+v", got, want)
	}
}

func TestTransform_SampleJSON(t *testing.T) {
	const want = `{
		"event_name": "user_checkout_completed",
		"event_category": "ecommerce",
		"organization_id": "org_enterprise_99",
		"session_id": "sess_abc123xyz",
		"client_metadata": {"ip_address": "192.168.1.55", "user_agent": "Mozilla/5.0"},
		"dimensions": {
			"currency": {"value": "USD", "type": "string"},
			"payment_gateway": {"value": "stripe", "type": "string"}
		},
		"metrics": {
			"cart_total_cents": {"value": "4999", "type": "integer"},
			"items_count": {"value": "3", "type": "integer"}
		}
	}`

	encoded, err := json.Marshal(model.Transform(sampleJob()))
	if err != nil {
		t.Fatalf("failed to marshal: %v", err)
	}

	var gotJSON, wantJSON any
	if err := json.Unmarshal(encoded, &gotJSON); err != nil {
		t.Fatalf("failed to decode result: %v", err)
	}
	if err := json.Unmarshal([]byte(want), &wantJSON); err != nil {
		t.Fatalf("failed to decode expectation: %v", err)
	}

	if !reflect.DeepEqual(gotJSON, wantJSON) {
		t.Fatalf("unexpected JSON:\n got: %s", encoded)
	}
}

func TestTransform_DropsIncompleteTriplets(t *testing.T) {
	got := model.Transform(model.Job{Payload: model.RawEvent{
		"ev_name": "click",

		"dim_rk1": "complete", "dim_rv1": "yes", "dim_rt1": "string",
		"dim_rk2": "no_value", "dim_rt2": "string",
		"dim_rk3": "no_type", "dim_rv3": "x",
		"dim_rv4": "no_key", "dim_rt4": "string",
		"dim_rk5": "", "dim_rv5": "empty_key", "dim_rt5": "string",
		"dim_rk6": "empty_value", "dim_rv6": "", "dim_rt6": "string",
		"dim_rk7": "empty_type", "dim_rv7": "x", "dim_rt7": "",

		"met_rk1": "no_value", "met_rt1": "integer",
		"met_rk2": "latency_ms", "met_rv2": "12", "met_rt2": "integer",
	}})

	wantDims := map[string]model.TypedValue{"complete": {Value: "yes", Type: "string"}}
	if !reflect.DeepEqual(got.Dimensions, wantDims) {
		t.Fatalf("unexpected dimensions: %+v", got.Dimensions)
	}

	wantMets := map[string]model.TypedValue{"latency_ms": {Value: "12", Type: "integer"}}
	if !reflect.DeepEqual(got.Metrics, wantMets) {
		t.Fatalf("unexpected metrics: %+v", got.Metrics)
	}

	if got.EventName != "click" {
		t.Fatalf("expected event to survive malformed triplets, got name %q", got.EventName)
	}
}

func TestTransform_IgnoresNonNumericSlots(t *testing.T) {
	got := model.Transform(model.Job{Payload: model.RawEvent{
		"dim_rk":    "no_index",
		"dim_rv":    "x",
		"dim_rt":    "string",
		"dim_rkabc": "bad_index",
		"dim_rvabc": "x",
		"dim_rtabc": "string",
		"dimension": "unrelated",
		"met_rk1x":  "bad_index",
		"met_rv1x":  "1",
		"met_rt1x":  "integer",
	}})

	if len(got.Dimensions) != 0 || len(got.Metrics) != 0 {
		t.Fatalf("expected no slots, got dimensions=%+v metrics=%+v", got.Dimensions, got.Metrics)
	}
}

func TestTransform_SparseAndLargeSlotIndexes(t *testing.T) {
	got := model.Transform(model.Job{Payload: model.RawEvent{
		"dim_rk3": "region", "dim_rv3": "eu", "dim_rt3": "string",
		"dim_rk120": "plan", "dim_rv120": "pro", "dim_rt120": "string",
	}})

	want := map[string]model.TypedValue{
		"region": {Value: "eu", Type: "string"},
		"plan":   {Value: "pro", Type: "string"},
	}
	if !reflect.DeepEqual(got.Dimensions, want) {
		t.Fatalf("unexpected dimensions: %+v", got.Dimensions)
	}
}

func TestTransform_DuplicateNameHighestSlotWins(t *testing.T) {
	payload := model.RawEvent{
		"dim_rk2": "currency", "dim_rv2": "USD", "dim_rt2": "string",
		"dim_rk10": "currency", "dim_rv10": "EUR", "dim_rt10": "string",
		"dim_rk9": "currency", "dim_rv9": "GBP", "dim_rt9": "string",
	}

	for range 50 {
		got := model.Transform(model.Job{Payload: payload})
		if got.Dimensions["currency"].Value != "EUR" {
			t.Fatalf("expected slot 10 to win, got %q", got.Dimensions["currency"].Value)
		}
	}
}

func TestTransform_EmptyPayloadEncodesEmptyObjects(t *testing.T) {
	got := model.Transform(model.Job{})

	if got.Dimensions == nil || got.Metrics == nil {
		t.Fatal("expected non-nil dimensions and metrics maps")
	}

	encoded, err := json.Marshal(got)
	if err != nil {
		t.Fatalf("failed to marshal: %v", err)
	}

	var decoded map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("failed to decode: %v", err)
	}
	if string(decoded["dimensions"]) != "{}" || string(decoded["metrics"]) != "{}" {
		t.Fatalf("expected empty objects, got dimensions=%s metrics=%s", decoded["dimensions"], decoded["metrics"])
	}
}
