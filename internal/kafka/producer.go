package kafka

import (
	"context"
	"crypto/sha256"
	"crypto/sha512"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"hash"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/xdg-go/scram"
)

type Config struct {
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
}

type Producer struct {
	producer sarama.SyncProducer
	brokers  []string
	mu       sync.RWMutex
	lastErr  error
}

type Message struct {
	Topic string
	Key   []byte
	Value []byte
	Time  time.Time
}

type BatchMessageError struct {
	Index int
	Err   error
}

type BatchPublishError struct {
	Errors []BatchMessageError
}

func (e BatchPublishError) Error() string {
	return fmt.Sprintf("kafka batch publish failed: %d messages", len(e.Errors))
}

func NewProducer(cfg Config) (*Producer, error) {
	saramaConfig := sarama.NewConfig()
	saramaConfig.ClientID = cfg.ClientID
	saramaConfig.Producer.Return.Errors = true
	saramaConfig.Producer.Return.Successes = true
	saramaConfig.Producer.RequiredAcks = parseRequiredAcks(cfg.RequiredAcks)
	saramaConfig.Producer.Retry.Max = 5
	saramaConfig.Producer.Compression = parseCompression(cfg.Compression)
	if cfg.FlushBytes > 0 {
		saramaConfig.Producer.Flush.Bytes = cfg.FlushBytes
	}
	if cfg.FlushMessages > 0 {
		saramaConfig.Producer.Flush.Messages = cfg.FlushMessages
	}
	if cfg.FlushFrequencyMs > 0 {
		saramaConfig.Producer.Flush.Frequency = time.Duration(cfg.FlushFrequencyMs) * time.Millisecond
	}
	if cfg.MaxMessageBytes > 0 {
		saramaConfig.Producer.MaxMessageBytes = cfg.MaxMessageBytes
	}
	saramaConfig.Net.WriteTimeout = 10 * time.Second
	saramaConfig.Net.ReadTimeout = 10 * time.Second

	// Map SecurityProtocol
	proto := strings.ToUpper(cfg.SecurityProtocol)
	if proto == "" {
		proto = "PLAINTEXT"
	}

	useTLS := strings.Contains(proto, "SSL") || cfg.TLSCAFile != "" || cfg.TLSCertFile != ""
	useSASL := strings.Contains(proto, "SASL") || cfg.SASLMechanism != ""

	if useTLS {
		saramaConfig.Net.TLS.Enable = true
		tlsConfig, err := buildTLSConfig(cfg)
		if err != nil {
			return nil, err
		}
		saramaConfig.Net.TLS.Config = tlsConfig
	}

	if useSASL {
		saramaConfig.Net.SASL.Enable = true
		if err := configureSASL(cfg, saramaConfig); err != nil {
			return nil, err
		}
	}

	producer, err := sarama.NewSyncProducer(cfg.Brokers, saramaConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create sarama producer: %w", err)
	}

	return &Producer{producer: producer, brokers: cfg.Brokers}, nil
}

func (p *Producer) Publish(ctx context.Context, msg Message) error {
	// Sarama SyncProducer doesn't take context in SendMessage directly,
	// but the underlying network calls verify deadlines if set in config.
	// For strict context cancellation support we might need AsyncProducer or check context before sending.

	if ctx.Err() != nil {
		return ctx.Err()
	}

	pm := &sarama.ProducerMessage{
		Topic: msg.Topic,
		Value: sarama.ByteEncoder(msg.Value),
	}
	if len(msg.Key) > 0 {
		pm.Key = sarama.ByteEncoder(msg.Key)
	}
	if !msg.Time.IsZero() {
		pm.Timestamp = msg.Time
	}

	_, _, err := p.producer.SendMessage(pm)
	if err != nil {
		p.setErr(err)
		return err
	}

	p.setErr(nil)
	return nil
}

func (p *Producer) PublishBatch(ctx context.Context, msgs []Message) error {
	if len(msgs) == 0 {
		return nil
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}

	producerMsgs := make([]*sarama.ProducerMessage, len(msgs))
	for idx, msg := range msgs {
		pm := &sarama.ProducerMessage{
			Topic:    msg.Topic,
			Value:    sarama.ByteEncoder(msg.Value),
			Metadata: idx,
		}
		if len(msg.Key) > 0 {
			pm.Key = sarama.ByteEncoder(msg.Key)
		}
		if !msg.Time.IsZero() {
			pm.Timestamp = msg.Time
		}
		producerMsgs[idx] = pm
	}

	err := p.producer.SendMessages(producerMsgs)
	if err == nil {
		p.setErr(nil)
		return nil
	}

	p.setErr(err)
	if producerErrors, ok := err.(sarama.ProducerErrors); ok {
		batchErr := BatchPublishError{Errors: make([]BatchMessageError, 0, len(producerErrors))}
		for _, producerErr := range producerErrors {
			index := -1
			if producerErr.Msg != nil {
				if metaIdx, ok := producerErr.Msg.Metadata.(int); ok {
					index = metaIdx
				}
			}
			batchErr.Errors = append(batchErr.Errors, BatchMessageError{
				Index: index,
				Err:   producerErr.Err,
			})
		}
		return batchErr
	}

	return err
}

func (p *Producer) Close() error {
	return p.producer.Close()
}

func (p *Producer) Ready(ctx context.Context) bool {
	// Sarama doesn't have a lightweight "ping" on SyncProducer easily accessbile without internal clients.
	// We can assume it's ready if lastErr is nil, or we can rely on the fact that NewProducer checks connectivity initially.
	// A simple check is to lock and check last reported error.
	if p.LastError() != nil {
		return false
	}
	return true
}

func (p *Producer) LastError() error {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.lastErr
}

func (p *Producer) setErr(err error) {
	p.mu.Lock()
	p.lastErr = err
	p.mu.Unlock()
}

func buildTLSConfig(cfg Config) (*tls.Config, error) {
	tlsConfig := &tls.Config{InsecureSkipVerify: cfg.TLSSkipVerify}

	if cfg.TLSCAFile != "" {
		caData, err := os.ReadFile(cfg.TLSCAFile)
		if err != nil {
			return nil, err
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caData) {
			return nil, fmt.Errorf("failed to parse Kafka CA file")
		}
		tlsConfig.RootCAs = pool
	}

	if cfg.TLSCertFile != "" && cfg.TLSKeyFile != "" {
		cert, err := tls.LoadX509KeyPair(cfg.TLSCertFile, cfg.TLSKeyFile)
		if err != nil {
			return nil, err
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	return tlsConfig, nil
}

func configureSASL(cfg Config, saramaConfig *sarama.Config) error {
	mechanism := strings.ToUpper(cfg.SASLMechanism)
	if mechanism == "" {
		mechanism = "PLAIN"
	}
	saramaConfig.Net.SASL.User = cfg.SASLUsername
	saramaConfig.Net.SASL.Password = cfg.SASLPassword
	saramaConfig.Net.SASL.Handshake = true

	switch mechanism {
	case "PLAIN":
		saramaConfig.Net.SASL.Mechanism = sarama.SASLTypePlaintext
	case "SCRAM-SHA-256":
		saramaConfig.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA256
		saramaConfig.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &XDGSCRAMClient{HashGeneratorFcn: SHA256} }
	case "SCRAM-SHA-512":
		saramaConfig.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512
		saramaConfig.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &XDGSCRAMClient{HashGeneratorFcn: SHA512} }
	default:
		return fmt.Errorf("unsupported SASL mechanism: %s", cfg.SASLMechanism)
	}
	return nil
}

var SHA256 scram.HashGeneratorFcn = func() hash.Hash { return sha256.New() }
var SHA512 scram.HashGeneratorFcn = func() hash.Hash { return sha512.New() }

type XDGSCRAMClient struct {
	*scram.Client
	*scram.ClientConversation
	HashGeneratorFcn scram.HashGeneratorFcn
}

func (x *XDGSCRAMClient) Begin(userName, password, authzID string) (err error) {
	x.Client, err = x.HashGeneratorFcn.NewClient(userName, password, authzID)
	if err != nil {
		return err
	}
	x.ClientConversation = x.Client.NewConversation()
	return nil
}

func (x *XDGSCRAMClient) Step(challenge string) (response string, err error) {
	return x.ClientConversation.Step(challenge)
}

func (x *XDGSCRAMClient) Done() bool {
	return x.ClientConversation.Done()
}

func parseRequiredAcks(value string) sarama.RequiredAcks {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", "all", "-1":
		return sarama.WaitForAll
	case "1", "local", "leader":
		return sarama.WaitForLocal
	case "0", "none":
		return sarama.NoResponse
	default:
		return sarama.WaitForAll
	}
}

func parseCompression(value string) sarama.CompressionCodec {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", "none":
		return sarama.CompressionNone
	case "gzip":
		return sarama.CompressionGZIP
	case "snappy":
		return sarama.CompressionSnappy
	case "lz4":
		return sarama.CompressionLZ4
	case "zstd":
		return sarama.CompressionZSTD
	default:
		return sarama.CompressionNone
	}
}
