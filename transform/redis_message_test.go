package transform

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/dataixcom/gsyncx"
)

func TestNewRedisMessageTransformer(t *testing.T) {
	transformer := NewRedisMessageTransformer(nil, nil)
	if transformer == nil {
		t.Error("expected non-nil transformer")
	}
}

func TestRedisMessageTransformer_Transform(t *testing.T) {
	fieldMapping := map[string]string{
		"user_id":  "id",
		"username": "name",
	}
	transformer := NewRedisMessageTransformer(fieldMapping, nil)

	records := []gsyncx.Record{
		{Data: map[string]interface{}{
			"_stream_id": "123-0",
			"user_id":    "42",
			"username":   "alice",
			"extra":      "keep",
		}},
	}

	result, failed, err := transformer.Transform(context.Background(), records)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(failed) != 0 {
		t.Errorf("expected 0 failed, got %d", len(failed))
	}
	if result[0].Data["id"] != "42" {
		t.Errorf("expected id=42, got %v", result[0].Data["id"])
	}
	if result[0].Data["name"] != "alice" {
		t.Errorf("expected name=alice, got %v", result[0].Data["name"])
	}
	if result[0].Data["extra"] != "keep" {
		t.Error("expected unmapped field to be preserved")
	}
}

func TestRedisMessageTransformer_Transform_WithPayload(t *testing.T) {
	transformer := NewRedisMessageTransformer(map[string]string{}, nil)

	payload, _ := json.Marshal(map[string]interface{}{
		"order_id": "ORD-001",
		"amount":   99.99,
	})

	records := []gsyncx.Record{
		{Data: map[string]interface{}{
			"_stream_id": "123-0",
			"payload":    string(payload),
		}},
	}

	result, _, err := transformer.Transform(context.Background(), records)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result[0].Data["order_id"] != "ORD-001" {
		t.Errorf("expected order_id=ORD-001 from payload, got %v", result[0].Data["order_id"])
	}
}

func TestRedisMessageTransformer_Transform_CancelledContext(t *testing.T) {
	transformer := NewRedisMessageTransformer(nil, nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	records := []gsyncx.Record{{Data: map[string]interface{}{"key": "value"}}}
	_, _, err := transformer.Transform(ctx, records)
	if err == nil {
		t.Error("expected error for cancelled context")
	}
}

func TestRedisMessageTransformer_Transform_EmptyRecords(t *testing.T) {
	transformer := NewRedisMessageTransformer(nil, nil)
	result, failed, err := transformer.Transform(context.Background(), nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 0 {
		t.Errorf("expected 0 results, got %d", len(result))
	}
	if len(failed) != 0 {
		t.Errorf("expected 0 failed, got %d", len(failed))
	}
}

func TestRedisMessageTransformer_Transform_InvalidPayload(t *testing.T) {
	transformer := NewRedisMessageTransformer(nil, nil)

	records := []gsyncx.Record{
		{Data: map[string]interface{}{
			"_stream_id": "123-0",
			"payload":    "not-valid-json{",
		}},
	}

	result, failed, err := transformer.Transform(context.Background(), records)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 1 {
		t.Errorf("expected 1 result, got %d", len(result))
	}
	if len(failed) != 0 {
		t.Errorf("expected 0 failed for invalid payload (should keep original), got %d", len(failed))
	}
}

func TestRedisMessageTransformer_Transform_WithBuiltinTransform(t *testing.T) {
	fieldMapping := map[string]string{
		"age": "user_age",
	}
	transformer := NewRedisMessageTransformer(fieldMapping, nil)

	records := []gsyncx.Record{
		{Data: map[string]interface{}{
			"_stream_id": "123-0",
			"age":        "25",
			"name":       "Alice",
		}},
	}

	result, _, err := transformer.Transform(context.Background(), records)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result[0].Data["user_age"] != "25" {
		t.Errorf("expected user_age=25, got %v", result[0].Data["user_age"])
	}
	if result[0].Data["name"] != "Alice" {
		t.Errorf("expected name=Alice, got %v", result[0].Data["name"])
	}
}
