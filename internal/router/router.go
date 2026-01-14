package router

import (
	"encoding/json"
	"fmt"
	"strings"
)

type TopicInfo struct {
	TenantID  string
	StationID string
	PumpID    string
	Type      string
	DeviceID  string
}

type Envelope struct {
	Schema        string          `json:"schema"`
	SchemaVersion string          `json:"schema_version"`
	MessageID     string          `json:"message_id"`
	Type          string          `json:"type"`
	TenantID      string          `json:"tenant_id"`
	StationID     string          `json:"station_id"`
	PumpID        string          `json:"pump_id"`
	DeviceID      string          `json:"device_id"`
	EventTime     string          `json:"event_time"`
	Seq           *int64          `json:"seq"`
	Data          json.RawMessage `json:"data"`
}

func ParseTopic(topic string, prefix string) (TopicInfo, error) {
	prefix = strings.Trim(prefix, "/")
	parts := strings.Split(strings.Trim(topic, "/"), "/")
	prefixParts := []string{}
	if prefix != "" {
		prefixParts = strings.Split(prefix, "/")
	}

	if len(parts) != len(prefixParts)+4 {
		return TopicInfo{}, fmt.Errorf("unexpected topic format: %s", topic)
	}

	for idx, part := range prefixParts {
		if parts[idx] != part {
			return TopicInfo{}, fmt.Errorf("topic prefix mismatch: %s", topic)
		}
	}

	base := parts[len(prefixParts):]
	info := TopicInfo{
		TenantID:  base[0],
		StationID: base[1],
		PumpID:    base[2],
		Type:      base[3],
	}
	if info.TenantID == "" || info.StationID == "" || info.PumpID == "" || info.Type == "" {
		return TopicInfo{}, fmt.Errorf("topic missing identifiers: %s", topic)
	}
	info.DeviceID = fmt.Sprintf("%s:%s", info.StationID, info.PumpID)
	return info, nil
}

func ValidateEnvelope(env Envelope, info TopicInfo) error {
	if env.Schema == "" {
		return fmt.Errorf("missing schema")
	}
	if env.SchemaVersion == "" {
		return fmt.Errorf("missing schema_version")
	}
	if env.MessageID == "" {
		return fmt.Errorf("missing message_id")
	}
	if env.Type == "" {
		return fmt.Errorf("missing type")
	}
	if env.TenantID == "" {
		return fmt.Errorf("missing tenant_id")
	}
	if env.StationID == "" {
		return fmt.Errorf("missing station_id")
	}
	if env.PumpID == "" {
		return fmt.Errorf("missing pump_id")
	}
	if env.DeviceID == "" {
		return fmt.Errorf("missing device_id")
	}
	if env.EventTime == "" {
		return fmt.Errorf("missing event_time")
	}
	if env.Seq == nil {
		return fmt.Errorf("missing seq")
	}
	if len(env.Data) == 0 {
		return fmt.Errorf("missing data")
	}

	if env.TenantID != info.TenantID {
		return fmt.Errorf("tenant_id mismatch")
	}
	if env.StationID != info.StationID {
		return fmt.Errorf("station_id mismatch")
	}
	if env.PumpID != info.PumpID {
		return fmt.Errorf("pump_id mismatch")
	}
	if env.DeviceID != info.DeviceID {
		return fmt.Errorf("device_id mismatch")
	}
	if env.Type != info.Type {
		return fmt.Errorf("type mismatch")
	}

	return nil
}

func RouteKafkaTopic(messageType string, topics Topics) (string, error) {
	switch strings.ToLower(messageType) {
	case "telemetry", "state", "event":
		return topics.TelemetryRaw, nil
	case "tx":
		return topics.TxRaw, nil
	case "ack":
		return topics.AckRaw, nil
	default:
		return "", fmt.Errorf("unknown message type: %s", messageType)
	}
}

type Topics struct {
	TelemetryRaw string
	TxRaw        string
	AckRaw       string
	DLQ          string
}
