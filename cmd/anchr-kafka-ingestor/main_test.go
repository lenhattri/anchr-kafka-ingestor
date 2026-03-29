package main

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"testing"
	"time"

	"anchr-kafka-ingestor/internal/kafka"
	"anchr-kafka-ingestor/internal/metrics"
	"anchr-kafka-ingestor/internal/mqtt"
	"anchr-kafka-ingestor/internal/router"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

type fakeProducer struct {
	batches [][]kafka.Message
}

func (f *fakeProducer) Publish(context.Context, kafka.Message) error {
	return nil
}

func (f *fakeProducer) PublishBatch(_ context.Context, msgs []kafka.Message) error {
	copied := make([]kafka.Message, len(msgs))
	copy(copied, msgs)
	f.batches = append(f.batches, copied)
	return nil
}

func (f *fakeProducer) Ready(context.Context) bool {
	return true
}

func (f *fakeProducer) Close() error {
	return nil
}

func TestProcessMessagesUsesMQTTReceiptTimeForEndToEndLatency(t *testing.T) {
	metricsCollector := newTestMetrics()
	producer := &fakeProducer{}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	seq := int64(42)
	receivedAt := time.Now().Add(-1500 * time.Millisecond).UTC()
	eventTime := time.Now().Add(-15 * time.Second).UTC().Format(time.RFC3339Nano)
	payload, err := json.Marshal(router.Envelope{
		Schema:        "anchr.telemetry",
		SchemaVersion: 1,
		MessageID:     "msg-1",
		Type:          "telemetry",
		TenantID:      "tenant-1",
		StationID:     "station-1",
		PumpID:        "pump-1",
		DeviceID:      "station-1:pump-1",
		EventTime:     eventTime,
		Seq:           &seq,
		Data:          json.RawMessage(`{"temperature":25.5}`),
	})
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}

	jobs := make(chan ingestJob, 1)
	jobs <- ingestJob{
		msg: mqtt.Message{
			Topic:      "anchr/tenant-1/station-1/pump-1/telemetry",
			Payload:    payload,
			ReceivedAt: receivedAt,
		},
		info: router.TopicInfo{
			TenantID:  "tenant-1",
			StationID: "station-1",
			PumpID:    "pump-1",
			Type:      "telemetry",
			DeviceID:  "station-1:pump-1",
		},
	}
	close(jobs)

	processMessages(
		context.Background(),
		logger,
		metricsCollector,
		producer,
		router.Topics{TelemetryRaw: "anchr.mqtt.telemetry.raw.v1"},
		1,
		0,
		jobs,
	)

	hist := histogramSnapshot(t, metricsCollector.EndToEndLatency)
	if got := hist.GetSampleCount(); got != 1 {
		t.Fatalf("sample_count=%d, want 1", got)
	}
	if got := hist.GetSampleSum(); got < 1000 || got > 5000 {
		t.Fatalf("sample_sum=%fms, want latency from MQTT receipt, not event_time", got)
	}
	if got := len(producer.batches); got != 1 {
		t.Fatalf("published_batches=%d, want 1", got)
	}
}

func TestProcessMessagesSkipsEndToEndLatencyWhenReceiptTimeMissing(t *testing.T) {
	metricsCollector := newTestMetrics()
	producer := &fakeProducer{}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	seq := int64(7)
	payload, err := json.Marshal(router.Envelope{
		Schema:        "anchr.telemetry",
		SchemaVersion: 1,
		MessageID:     "msg-2",
		Type:          "telemetry",
		TenantID:      "tenant-1",
		StationID:     "station-1",
		PumpID:        "pump-1",
		DeviceID:      "station-1:pump-1",
		EventTime:     time.Now().Add(-20 * time.Second).UTC().Format(time.RFC3339Nano),
		Seq:           &seq,
		Data:          json.RawMessage(`{"temperature":27.0}`),
	})
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}

	jobs := make(chan ingestJob, 1)
	jobs <- ingestJob{
		msg: mqtt.Message{
			Topic:   "anchr/tenant-1/station-1/pump-1/telemetry",
			Payload: payload,
		},
		info: router.TopicInfo{
			TenantID:  "tenant-1",
			StationID: "station-1",
			PumpID:    "pump-1",
			Type:      "telemetry",
			DeviceID:  "station-1:pump-1",
		},
	}
	close(jobs)

	processMessages(
		context.Background(),
		logger,
		metricsCollector,
		producer,
		router.Topics{TelemetryRaw: "anchr.mqtt.telemetry.raw.v1"},
		1,
		0,
		jobs,
	)

	hist := histogramSnapshot(t, metricsCollector.EndToEndLatency)
	if got := hist.GetSampleCount(); got != 0 {
		t.Fatalf("sample_count=%d, want 0 when receipt time is missing", got)
	}
}

func newTestMetrics() *metrics.Metrics {
	return &metrics.Metrics{
		MQTTMessagesReceived: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "mqtt_messages_received_total",
			Help: "Total MQTT messages received, labeled by type.",
		}, []string{"type"}),
		KafkaMessagesPublished: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "kafka_messages_published_total",
			Help: "Total Kafka messages published, labeled by topic.",
		}, []string{"topic"}),
		PublishErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "publish_errors_total",
			Help: "Total publish errors, labeled by target.",
		}, []string{"target"}),
		DLQTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "dlq_total",
			Help: "Total messages sent to DLQ, labeled by reason.",
		}, []string{"reason"}),
		ReconnectTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "reconnect_total",
			Help: "Total MQTT reconnect attempts.",
		}),
		KafkaPublishLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "kafka_publish_latency_ms",
			Help:    "Kafka publish latency in milliseconds.",
			Buckets: []float64{5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000},
		}),
		EndToEndLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "end_to_end_ingest_latency_ms",
			Help:    "Ingest latency from MQTT receipt to Kafka publish in milliseconds.",
			Buckets: []float64{10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000},
		}),
	}
}

func histogramSnapshot(t *testing.T, h prometheus.Histogram) *dto.Histogram {
	t.Helper()

	var metric dto.Metric
	if err := h.Write(&metric); err != nil {
		t.Fatalf("write histogram metric: %v", err)
	}
	return metric.GetHistogram()
}
