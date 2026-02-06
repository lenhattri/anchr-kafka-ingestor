package router

import (
	"encoding/json"
	"testing"
)

func TestParseTopic(t *testing.T) {
	info, err := ParseTopic("anchr/v1/tenant-a/station-b/pump-c/telemetry", "anchr/v1")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if info.DeviceID != "station-b:pump-c" {
		t.Fatalf("unexpected device id: %s", info.DeviceID)
	}
	if info.Type != "telemetry" {
		t.Fatalf("unexpected type: %s", info.Type)
	}
}

func TestValidateEnvelope(t *testing.T) {
	seq := int64(1)
	info := TopicInfo{TenantID: "tenant-a", StationID: "station-b", PumpID: "pump-c", DeviceID: "station-b:pump-c"}
	env := Envelope{
		Schema:        "anchr",
		SchemaVersion: 1,
		MessageID:     "msg-1",
		Type:          "telemetry",
		TenantID:      "tenant-a",
		StationID:     "station-b",
		PumpID:        "pump-c",
		DeviceID:      "station-b:pump-c",
		EventTime:     "2024-01-01T00:00:00Z",
		Seq:           &seq,
		Data:          json.RawMessage(`{"foo":"bar"}`),
	}

	if err := ValidateEnvelope(env, info); err != nil {
		t.Fatalf("expected valid envelope, got %v", err)
	}
}

func TestRouteKafkaTopic(t *testing.T) {
	topics := Topics{
		TelemetryRaw: "telemetry",
		TxRaw:        "tx",
		AckRaw:       "ack",
		DLQ:          "dlq",
	}
	got, err := RouteKafkaTopic("state", topics)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "telemetry" {
		t.Fatalf("expected telemetry topic, got %s", got)
	}
}
