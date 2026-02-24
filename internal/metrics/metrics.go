package metrics

import "github.com/prometheus/client_golang/prometheus"

type Metrics struct {
	MQTTMessagesReceived   *prometheus.CounterVec
	KafkaMessagesPublished *prometheus.CounterVec
	PublishErrors          *prometheus.CounterVec
	DLQTotal               *prometheus.CounterVec
	ReconnectTotal         prometheus.Counter
	KafkaPublishLatency    prometheus.Histogram
	EndToEndLatency        prometheus.Histogram

}

func New() *Metrics {
	metrics := &Metrics{
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
			Help:    "End-to-end ingest latency in milliseconds.",
			Buckets: []float64{10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000},
		}),
	}

	prometheus.MustRegister(
		metrics.MQTTMessagesReceived,
		metrics.KafkaMessagesPublished,
		metrics.PublishErrors,
		metrics.DLQTotal,
		metrics.ReconnectTotal,
		metrics.KafkaPublishLatency,
		metrics.EndToEndLatency,
	)

	return metrics
}
