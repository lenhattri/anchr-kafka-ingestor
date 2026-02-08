package config

import (
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
)

type Config struct {
	LogLevel string

	MQTT  MQTTConfig
	Kafka KafkaConfig

	HTTPAddr               string
	BufferSize             int
	IngestWorkers          int
	IngestWorkerBufferSize int
	IngestBatchSize        int
	IngestBatchLingerMs    int
}

type MQTTConfig struct {
	BrokerHost              string
	BrokerPort              int
	Username                string
	Password                string
	UseTLS                  bool
	CAFile                  string
	ClientID                string
	QoS                     byte
	TopicPrefix             string
	SubFilters              []string
	SharedSubscriptionGroup string
	OrderMatters            bool
}

type KafkaConfig struct {
	Brokers          []string
	ClientID         string
	SecurityProtocol string
	SASLMechanism    string
	SASLUsername     string
	SASLPassword     string
	TLSCAFile        string
	TLSCertFile      string
	TLSKeyFile       string
	TLSSkipVerify    bool
	RequiredAcks     string
	Compression      string
	FlushBytes       int
	FlushMessages    int
	FlushFrequencyMs int
	MaxMessageBytes  int

	TopicTelemetryRaw string
	TopicTxRaw        string
	TopicAckRaw       string
	TopicDLQ          string
}

func Load() (Config, error) {
	cfg := Config{}
	cfg.LogLevel = getEnv("LOG_LEVEL", "info")
	cfg.HTTPAddr = getEnv("HTTP_ADDR", ":8080")
	cfg.BufferSize = getEnvInt("INGEST_BUFFER_SIZE", 1000)
	defaultWorkers := maxInt(4, runtime.NumCPU()*2)
	cfg.IngestWorkers = getEnvInt("INGEST_WORKERS", defaultWorkers)
	if cfg.IngestWorkers <= 0 {
		cfg.IngestWorkers = defaultWorkers
	}
	cfg.IngestBatchSize = getEnvInt("INGEST_BATCH_SIZE", 500)
	if cfg.IngestBatchSize <= 0 {
		cfg.IngestBatchSize = 1
	}
	cfg.IngestBatchLingerMs = getEnvInt("INGEST_BATCH_LINGER_MS", 5)
	if cfg.IngestBatchLingerMs < 0 {
		cfg.IngestBatchLingerMs = 0
	}
	cfg.IngestWorkerBufferSize = getEnvInt("INGEST_WORKER_BUFFER_SIZE", 0)
	if cfg.IngestWorkerBufferSize <= 0 {
		derived := cfg.BufferSize / cfg.IngestWorkers
		if derived < 128 {
			derived = 128
		}
		cfg.IngestWorkerBufferSize = derived
	}

	cfg.MQTT = MQTTConfig{
		BrokerHost:              getEnv("MQTT_BROKER_HOST", "localhost"),
		BrokerPort:              getEnvInt("MQTT_BROKER_PORT", 1883),
		Username:                os.Getenv("MQTT_USERNAME"),
		Password:                os.Getenv("MQTT_PASSWORD"),
		UseTLS:                  getEnvBool("MQTT_USE_TLS", false),
		CAFile:                  os.Getenv("MQTT_CA_FILE"),
		ClientID:                getEnv("MQTT_CLIENT_ID", "anchr-kafka-ingestor"),
		QoS:                     byte(getEnvInt("MQTT_QOS", 1)),
		TopicPrefix:             getEnv("MQTT_TOPIC_PREFIX", "anchr/v1"),
		SubFilters:              getEnvList("MQTT_SUB_FILTERS"),
		SharedSubscriptionGroup: getEnv("MQTT_SHARED_SUBSCRIPTION_GROUP", ""),
		OrderMatters:            getEnvBool("MQTT_ORDER_MATTERS", true),
	}

	cfg.Kafka = KafkaConfig{
		Brokers:           strings.Split(getEnv("KAFKA_BROKERS", "localhost:9092"), ","),
		ClientID:          getEnv("KAFKA_CLIENT_ID", "anchr-kafka-ingestor"),
		SecurityProtocol:  getEnv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT"),
		SASLMechanism:     getEnv("KAFKA_SASL_MECHANISM", ""),
		SASLUsername:      os.Getenv("KAFKA_SASL_USERNAME"),
		SASLPassword:      os.Getenv("KAFKA_SASL_PASSWORD"),
		TLSCAFile:         os.Getenv("KAFKA_TLS_CA_FILE"),
		TLSCertFile:       os.Getenv("KAFKA_TLS_CERT_FILE"),
		TLSKeyFile:        os.Getenv("KAFKA_TLS_KEY_FILE"),
		TLSSkipVerify:     getEnvBool("KAFKA_TLS_SKIP_VERIFY", false),
		RequiredAcks:      getEnv("KAFKA_REQUIRED_ACKS", "all"),
		Compression:       getEnv("KAFKA_COMPRESSION", "none"),
		FlushBytes:        getEnvInt("KAFKA_FLUSH_BYTES", 0),
		FlushMessages:     getEnvInt("KAFKA_FLUSH_MESSAGES", 0),
		FlushFrequencyMs:  getEnvInt("KAFKA_FLUSH_FREQUENCY_MS", 0),
		MaxMessageBytes:   getEnvInt("KAFKA_MAX_MESSAGE_BYTES", 0),
		TopicTelemetryRaw: getEnv("KAFKA_TOPIC_TELEMETRY_RAW", "anchr.mqtt.telemetry.raw.v1"),
		TopicTxRaw:        getEnv("KAFKA_TOPIC_TX_RAW", "anchr.mqtt.tx.raw.v1"),
		TopicAckRaw:       getEnv("KAFKA_TOPIC_ACK_RAW", "anchr.mqtt.ack.raw.v1"),
		TopicDLQ:          getEnv("KAFKA_TOPIC_DLQ", "anchr.dlq.v1"),
	}

	if cfg.MQTT.BrokerHost == "" {
		return cfg, fmt.Errorf("MQTT_BROKER_HOST is required")
	}
	if len(cfg.Kafka.Brokers) == 0 || cfg.Kafka.Brokers[0] == "" {
		return cfg, fmt.Errorf("KAFKA_BROKERS is required")
	}

	return cfg, nil
}

func getEnv(key, def string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return def
}

func getEnvBool(key string, def bool) bool {
	if value := os.Getenv(key); value != "" {
		parsed, err := strconv.ParseBool(value)
		if err == nil {
			return parsed
		}
	}
	return def
}

func getEnvInt(key string, def int) int {
	if value := os.Getenv(key); value != "" {
		parsed, err := strconv.Atoi(value)
		if err == nil {
			return parsed
		}
	}
	return def
}

func getEnvList(key string) []string {
	value := os.Getenv(key)
	if value == "" {
		return nil
	}
	parts := strings.Split(value, ",")
	filters := make([]string, 0, len(parts))
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed != "" {
			filters = append(filters, trimmed)
		}
	}
	return filters
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
