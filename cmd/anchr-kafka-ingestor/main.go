package main

import (
	"context"
	"encoding/json"
	"log/slog"
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

type ingestJob struct {
	msg  mqtt.Message
	info router.TopicInfo
}

func main() {
	cfg, err := config.Load()
	if err != nil {
		panic(err)
	}

	logger := newLogger(cfg.LogLevel)
	logger.Info("starting anchr-kafka-ingestor")

	metricsCollector := metrics.New()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

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
		RequiredAcks:     cfg.Kafka.RequiredAcks,
		Compression:      cfg.Kafka.Compression,
		FlushBytes:       cfg.Kafka.FlushBytes,
		FlushMessages:    cfg.Kafka.FlushMessages,
		FlushFrequencyMs: cfg.Kafka.FlushFrequencyMs,
		MaxMessageBytes:  cfg.Kafka.MaxMessageBytes,
	})
	if err != nil {
		logger.Error("failed to create kafka producer", slog.Any("error", err))
		os.Exit(1)
	}
	defer func() {
		_ = producer.Close()
	}()

	incomingCh := make(chan mqtt.Message, cfg.BufferSize)
	workerChans := make([]chan ingestJob, cfg.IngestWorkers)
	for idx := range workerChans {
		workerChans[idx] = make(chan ingestJob, cfg.IngestWorkerBufferSize)
	}

	var mqttClient *mqtt.Client
	mqttClient, err = mqtt.New(toMQTTConfig(cfg.MQTT), mqtt.Handlers{
		OnMessage: func(msg mqtt.Message) {
			select {
			case incomingCh <- msg:
			case <-ctx.Done():
			}
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

	topics := router.Topics{
		TelemetryRaw: cfg.Kafka.TopicTelemetryRaw,
		TxRaw:        cfg.Kafka.TopicTxRaw,
		AckRaw:       cfg.Kafka.TopicAckRaw,
		DLQ:          cfg.Kafka.TopicDLQ,
	}

	batchLinger := time.Duration(cfg.IngestBatchLingerMs) * time.Millisecond
	if batchLinger < 0 {
		batchLinger = 0
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		dispatchMessages(ctx, logger, metricsCollector, producer, topics, cfg.MQTT.TopicPrefix, incomingCh, workerChans)
	}()
	for idx := range workerChans {
		wg.Add(1)
		go func(workerID int, ch <-chan ingestJob) {
			defer wg.Done()
			processMessages(ctx, logger, metricsCollector, producer, topics, cfg.IngestBatchSize, batchLinger, ch)
		}(idx, workerChans[idx])
	}

	<-ctx.Done()
	logger.Info("shutdown requested")
	wg.Wait()
}

func processMessages(
	ctx context.Context,
	logger *slog.Logger,
	metricsCollector *metrics.Metrics,
	producer *kafka.Producer,
	topics router.Topics,
	batchSize int,
	batchLinger time.Duration,
	messages <-chan ingestJob,
) {
	if batchSize <= 0 {
		batchSize = 1
	}

	type publishItem struct {
		job   ingestJob
		env   router.Envelope
		topic string
	}

	var (
		batch      = make([]ingestJob, 0, batchSize)
		timer      *time.Timer
		timerC     <-chan time.Time
		timerArmed bool
	)
	if batchLinger > 0 {
		timer = time.NewTimer(batchLinger)
		timer.Stop()
	}

	flush := func(items []ingestJob) {
		if len(items) == 0 {
			return
		}
		publishItems := make([]publishItem, 0, len(items))
		for _, job := range items {
			var env router.Envelope
			if err := json.Unmarshal(job.msg.Payload, &env); err != nil {
				sendToDLQ(ctx, logger, metricsCollector, producer, topics.DLQ, job.msg, "invalid_json", err, job.info.DeviceID)
				continue
			}

			if err := router.ValidateEnvelope(env, job.info); err != nil {
				sendToDLQ(ctx, logger, metricsCollector, producer, topics.DLQ, job.msg, "validation", err, job.info.DeviceID)
				continue
			}

			topic, err := router.RouteKafkaTopic(job.info.Type, topics)
			if err != nil {
				sendToDLQ(ctx, logger, metricsCollector, producer, topics.DLQ, job.msg, "routing", err, job.info.DeviceID)
				continue
			}

			publishItems = append(publishItems, publishItem{
				job:   job,
				env:   env,
				topic: topic,
			})
		}

		if len(publishItems) == 0 {
			return
		}

		now := time.Now().UTC()
		kafkaMessages := make([]kafka.Message, len(publishItems))
		for idx, item := range publishItems {
			kafkaMessages[idx] = kafka.Message{
				Topic: item.topic,
				Key:   []byte(item.job.info.DeviceID),
				Value: item.job.msg.Payload,
				Time:  now,
			}
		}

		start := time.Now()
		publishErr := producer.PublishBatch(ctx, kafkaMessages)
		latencyMs := float64(time.Since(start).Milliseconds())

		var failed map[int]error
		if publishErr != nil {
			failed = make(map[int]error)
			if batchErr, ok := publishErr.(kafka.BatchPublishError); ok {
				failedAll := false
				for _, errItem := range batchErr.Errors {
					if errItem.Index < 0 || errItem.Index >= len(publishItems) {
						failedAll = true
						break
					}
					failed[errItem.Index] = errItem.Err
				}
				if failedAll {
					for idx := range publishItems {
						failed[idx] = publishErr
					}
				}
			} else {
				for idx := range publishItems {
					failed[idx] = publishErr
				}
			}
		}

		for idx, item := range publishItems {
			metricsCollector.KafkaPublishLatency.Observe(latencyMs)
			if failed != nil {
				if err, ok := failed[idx]; ok {
					metricsCollector.PublishErrors.WithLabelValues("kafka").Inc()
					msgLogger := logger.With(
						slog.String("message_id", item.env.MessageID),
						slog.String("device_id", item.job.info.DeviceID),
						slog.String("kafka_topic", item.topic),
					)
					sendToDLQ(ctx, msgLogger, metricsCollector, producer, topics.DLQ, item.job.msg, "kafka_publish", err, item.job.info.DeviceID)
					continue
				}
			}

			metricsCollector.KafkaMessagesPublished.WithLabelValues(item.topic).Inc()
			if item.env.EventTime != "" {
				if parsedTime, err := time.Parse(time.RFC3339Nano, item.env.EventTime); err == nil {
					latency := time.Since(parsedTime).Milliseconds()
					if latency >= 0 {
						metricsCollector.EndToEndLatency.Observe(float64(latency))
					}
				}
			}
		}
	}

	for {
		select {
		case <-ctx.Done():
			flush(batch)
			return
		case job, ok := <-messages:
			if !ok {
				flush(batch)
				return
			}
			batch = append(batch, job)
			if len(batch) == 1 && batchLinger > 0 {
				if timer != nil {
					timer.Reset(batchLinger)
					timerC = timer.C
					timerArmed = true
				}
			}
			if len(batch) >= batchSize {
				if timerArmed && timer != nil {
					if !timer.Stop() {
						select {
						case <-timer.C:
						default:
						}
					}
					timerArmed = false
					timerC = nil
				}
				flush(batch)
				batch = batch[:0]
			}
		case <-timerC:
			timerArmed = false
			timerC = nil
			flush(batch)
			batch = batch[:0]
		}
	}
}

func dispatchMessages(
	ctx context.Context,
	logger *slog.Logger,
	metricsCollector *metrics.Metrics,
	producer *kafka.Producer,
	topics router.Topics,
	topicPrefix string,
	messages <-chan mqtt.Message,
	workers []chan ingestJob,
) {
	workerCount := len(workers)
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-messages:
			if !ok {
				return
			}
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

			workerIdx := workerIndex(info.DeviceID, workerCount)
			select {
			case workers[workerIdx] <- ingestJob{msg: msg, info: info}:
			case <-ctx.Done():
				return
			}
		}
	}
}

func workerIndex(deviceID string, workerCount int) int {
	if workerCount <= 1 {
		return 0
	}
	const (
		offset32 = 2166136261
		prime32  = 16777619
	)
	hash := uint32(offset32)
	for i := 0; i < len(deviceID); i++ {
		hash ^= uint32(deviceID[i])
		hash *= prime32
	}
	return int(hash % uint32(workerCount))
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
		BrokerHost:              cfg.BrokerHost,
		BrokerPort:              cfg.BrokerPort,
		Username:                cfg.Username,
		Password:                cfg.Password,
		UseTLS:                  cfg.UseTLS,
		CAFile:                  cfg.CAFile,
		ClientID:                cfg.ClientID,
		QoS:                     cfg.QoS,
		TopicPrefix:             cfg.TopicPrefix,
		SubFilters:              cfg.SubFilters,
		SharedSubscriptionGroup: cfg.SharedSubscriptionGroup,
		OrderMatters:            cfg.OrderMatters,
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
