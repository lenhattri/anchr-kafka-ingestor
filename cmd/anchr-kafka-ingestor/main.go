package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"math/rand"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"anchr-kafka-ingestor/internal/config"
	httpserver "anchr-kafka-ingestor/internal/http"
	"anchr-kafka-ingestor/internal/kafka"
	"anchr-kafka-ingestor/internal/metrics"
	"anchr-kafka-ingestor/internal/mqtt"
	"anchr-kafka-ingestor/internal/router"
)

type DLQRecord struct {
	OriginalMQTTTopic string `json:"original_mqtt_topic"`
	ReceivedAt        string `json:"received_at"`
	ErrorType         string `json:"error_type"`
	ErrorMessage      string `json:"error_message"`
	RawPayload        string `json:"raw_payload"`
	ParsedDeviceID    string `json:"parsed_device_id,omitempty"`
}

func main() {
	rand.Seed(time.Now().UnixNano())

	cfg, err := config.Load()
	if err != nil {
		panic(err)
	}

	logger := newLogger(cfg.LogLevel)
	logger.Info("starting anchr-kafka-ingestor")

	metricsCollector := metrics.New()

	producer, err := kafka.NewProducer(kafka.Config{
		Brokers:          cfg.Kafka.Brokers,
		ClientID:         cfg.Kafka.ClientID,
		SecurityProtocol: cfg.Kafka.SecurityProtocol,
		SASLMechanism:    cfg.Kafka.SASLMechanism,
		SASLUsername:     cfg.Kafka.SASLUsername,
		SASLPassword:     cfg.Kafka.SASLPassword,
		TLSCAFile:        cfg.Kafka.TLSCAFile,
		TLSCertFile:      cfg.Kafka.TLSCertFile,
		TLSKeyFile:       cfg.Kafka.TLSKeyFile,
		TLSSkipVerify:    cfg.Kafka.TLSSkipVerify,
	})
	if err != nil {
		logger.Error("failed to create kafka producer", slog.Any("error", err))
		os.Exit(1)
	}
	defer func() {
		_ = producer.Close()
	}()

	messageCh := make(chan mqtt.Message, cfg.BufferSize)

	var mqttClient *mqtt.Client
	mqttClient, err = mqtt.New(toMQTTConfig(cfg.MQTT), mqtt.Handlers{
		OnMessage: func(msg mqtt.Message) {
			messageCh <- msg
		},
		OnReconnect: func() {
			metricsCollector.ReconnectTotal.Inc()
			logger.Warn("mqtt reconnecting")
		},
		OnConnect: func() {
			logger.Info("mqtt connected")
		},
		OnConnectionLost: func(err error) {
			logger.Error("mqtt connection lost", slog.Any("error", err))
		},
	})
	if err != nil {
		logger.Error("failed to create mqtt client", slog.Any("error", err))
		os.Exit(1)
	}

	if err := mqttClient.ConnectAndSubscribe(toMQTTConfig(cfg.MQTT)); err != nil {
		logger.Error("failed to connect or subscribe mqtt", slog.Any("error", err))
		os.Exit(1)
	}

	httpserver.Start(cfg.HTTPAddr, httpserver.HealthChecks{
		Ready: func(ctx context.Context) bool {
			return mqttClient.Connected() && producer.Ready(ctx)
		},
	})

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	topics := router.Topics{
		TelemetryRaw: cfg.Kafka.TopicTelemetryRaw,
		TxRaw:        cfg.Kafka.TopicTxRaw,
		AckRaw:       cfg.Kafka.TopicAckRaw,
		DLQ:          cfg.Kafka.TopicDLQ,
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		processMessages(ctx, logger, metricsCollector, producer, topics, cfg.MQTT.TopicPrefix, messageCh)
	}()

	<-ctx.Done()
	logger.Info("shutdown requested")
	close(messageCh)
	wg.Wait()
}

func processMessages(
	ctx context.Context,
	logger *slog.Logger,
	metricsCollector *metrics.Metrics,
	producer *kafka.Producer,
	topics router.Topics,
	topicPrefix string,
	messages <-chan mqtt.Message,
) {
	for msg := range messages {
		info, err := router.ParseTopic(msg.Topic, topicPrefix)
		msgType := "unknown"
		if err == nil {
			msgType = info.Type
		}
		metricsCollector.MQTTMessagesReceived.WithLabelValues(msgType).Inc()

		if err != nil {
			sendToDLQ(ctx, logger, metricsCollector, producer, topics.DLQ, msg, "topic_parse", err, "")
			continue
		}

		var env router.Envelope
		if err := json.Unmarshal(msg.Payload, &env); err != nil {
			sendToDLQ(ctx, logger, metricsCollector, producer, topics.DLQ, msg, "invalid_json", err, info.DeviceID)
			continue
		}

		if err := router.ValidateEnvelope(env, info); err != nil {
			sendToDLQ(ctx, logger, metricsCollector, producer, topics.DLQ, msg, "validation", err, info.DeviceID)
			continue
		}

		topic, err := router.RouteKafkaTopic(info.Type, topics)
		if err != nil {
			sendToDLQ(ctx, logger, metricsCollector, producer, topics.DLQ, msg, "routing", err, info.DeviceID)
			continue
		}

		msgLogger := logger.With(
			slog.String("message_id", env.MessageID),
			slog.String("device_id", info.DeviceID),
			slog.String("kafka_topic", topic),
		)

		start := time.Now()
		publishErr := producer.Publish(ctx, kafka.Message{
			Topic: topic,
			Key:   []byte(info.DeviceID),
			Value: msg.Payload,
			Time:  time.Now().UTC(),
		})
		metricsCollector.KafkaPublishLatency.Observe(float64(time.Since(start).Milliseconds()))

		if publishErr != nil {
			metricsCollector.PublishErrors.WithLabelValues("kafka").Inc()
			sendToDLQ(ctx, msgLogger, metricsCollector, producer, topics.DLQ, msg, "kafka_publish", publishErr, info.DeviceID)
			continue
		}

		metricsCollector.KafkaMessagesPublished.WithLabelValues(topic).Inc()

		if env.EventTime != "" {
			if parsedTime, err := time.Parse(time.RFC3339Nano, env.EventTime); err == nil {
				latency := time.Since(parsedTime).Milliseconds()
				if latency >= 0 {
					metricsCollector.EndToEndLatency.Observe(float64(latency))
				}
			}
		}

		msgLogger.Info("message published")
	}
}

func sendToDLQ(
	ctx context.Context,
	logger *slog.Logger,
	metricsCollector *metrics.Metrics,
	producer *kafka.Producer,
	dlqTopic string,
	msg mqtt.Message,
	reason string,
	err error,
	deviceID string,
) {
	record := DLQRecord{
		OriginalMQTTTopic: msg.Topic,
		ReceivedAt:        msg.ReceivedAt.Format(time.RFC3339Nano),
		ErrorType:         reason,
		ErrorMessage:      err.Error(),
		RawPayload:        string(msg.Payload),
		ParsedDeviceID:    deviceID,
	}

	payload, marshalErr := json.Marshal(record)
	if marshalErr != nil {
		logger.Error("failed to marshal DLQ record", slog.Any("error", marshalErr))
		metricsCollector.PublishErrors.WithLabelValues("dlq_marshal").Inc()
		return
	}

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	publishErr := producer.Publish(ctx, kafka.Message{
		Topic: dlqTopic,
		Key:   []byte(deviceID),
		Value: payload,
		Time:  time.Now().UTC(),
	})
	if publishErr != nil {
		metricsCollector.PublishErrors.WithLabelValues("dlq").Inc()
		logger.Error("failed to publish DLQ message", slog.Any("error", publishErr))
		return
	}

	metricsCollector.DLQTotal.WithLabelValues(reason).Inc()
	logger.Warn("message sent to DLQ", slog.String("reason", reason))
}

func toMQTTConfig(cfg config.MQTTConfig) mqtt.Config {
	return mqtt.Config{
		BrokerHost:  cfg.BrokerHost,
		BrokerPort:  cfg.BrokerPort,
		Username:    cfg.Username,
		Password:    cfg.Password,
		UseTLS:      cfg.UseTLS,
		CAFile:      cfg.CAFile,
		ClientID:    cfg.ClientID,
		QoS:         cfg.QoS,
		TopicPrefix: cfg.TopicPrefix,
		SubFilters:  cfg.SubFilters,
	}
}

func newLogger(level string) *slog.Logger {
	var lvl slog.Level
	switch strings.ToLower(level) {
	case "debug":
		lvl = slog.LevelDebug
	case "warn", "warning":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}

	handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: lvl})
	return slog.New(handler)
}
