package metrics

import "github.com/prometheus/client_golang/prometheus"

type Metrics struct {
	MQTTMessagesReceived    *prometheus.CounterVec
	KafkaMessagesPublished  *prometheus.CounterVec
	PublishErrors           *prometheus.CounterVec
	DLQTotal                *prometheus.CounterVec
	ReconnectTotal          prometheus.Counter
	IngestQueueLatency      prometheus.Histogram
	IngestProcessingLatency prometheus.Histogram
	IngestPreKafkaLatency   prometheus.Histogram
	KafkaPublishLatency     prometheus.Histogram
	EndToEndLatency         prometheus.Histogram
}

func New() *Metrics {
	ingestLatencyBuckets := []float64{
		1, 2, 5, 10, 15, 20, 30, 40, 50, 75, 100,
		125, 150, 175, 200, 225, 250, 300, 400, 500,
		750, 1000, 1500, 2000, 2500, 5000, 10000, 30000, 60000,
	}
	kafkaPublishLatencyBuckets := []float64{
		1, 2, 5, 10, 15, 20, 30, 40, 50, 75, 100,
		125, 150, 175, 200, 225, 250, 300, 400, 500,
		750, 1000, 1500, 2000, 2500, 5000, 10000, 30000, 60000,
	}

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
		IngestQueueLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "ingest_queue_latency_ms",
			Help:    "Ingest latency from MQTT receipt to worker dequeue in milliseconds.",
			Buckets: ingestLatencyBuckets,
		}),
		IngestProcessingLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "ingest_processing_latency_ms",
			Help:    "Ingest latency from worker dequeue to Kafka publish start in milliseconds.",
			Buckets: ingestLatencyBuckets,
		}),
		IngestPreKafkaLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "ingest_pre_kafka_latency_ms",
			Help:    "Ingest latency from MQTT receipt to Kafka publish start in milliseconds.",
			Buckets: ingestLatencyBuckets,
		}),
		KafkaPublishLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "kafka_publish_latency_ms",
			Help:    "Kafka publish latency in milliseconds.",
			Buckets: kafkaPublishLatencyBuckets,
		}),
		EndToEndLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "end_to_end_ingest_latency_ms",
			Help:    "Ingest latency from MQTT receipt to Kafka publish in milliseconds.",
			Buckets: ingestLatencyBuckets,
		}),
	}

	prometheus.MustRegister(
		metrics.MQTTMessagesReceived,
		metrics.KafkaMessagesPublished,
		metrics.PublishErrors,
		metrics.DLQTotal,
		metrics.ReconnectTotal,
		metrics.IngestQueueLatency,
		metrics.IngestProcessingLatency,
		metrics.IngestPreKafkaLatency,
		metrics.KafkaPublishLatency,
		metrics.EndToEndLatency,
	)

	return metrics
}
