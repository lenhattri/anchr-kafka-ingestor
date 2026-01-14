package kafka

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/segmentio/kafka-go/sasl/scram"
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
}

type Producer struct {
	writer  *kafka.Writer
	brokers []string
	mu      sync.RWMutex
	lastErr error
}

type Message struct {
	Topic string
	Key   []byte
	Value []byte
	Time  time.Time
}

func NewProducer(cfg Config) (*Producer, error) {
	dialer, err := buildDialer(cfg)
	if err != nil {
		return nil, err
	}

	writer := &kafka.Writer{
		Addr:         kafka.TCP(cfg.Brokers...),
		Async:        false,
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireAll,
		BatchTimeout: 10 * time.Millisecond,
		Dialer:       dialer,
		WriteTimeout: 10 * time.Second,
		ReadTimeout:  10 * time.Second,
		Logger:       kafka.LoggerFunc(func(string, ...interface{}) {}),
		ErrorLogger:  kafka.LoggerFunc(func(string, ...interface{}) {}),
	}

	return &Producer{writer: writer, brokers: cfg.Brokers}, nil
}

func (p *Producer) Publish(ctx context.Context, msg Message) error {
	write := func() error {
		return p.writer.WriteMessages(ctx, kafka.Message{
			Topic: msg.Topic,
			Key:   msg.Key,
			Value: msg.Value,
			Time:  msg.Time,
		})
	}

	const maxAttempts = 6
	base := 200 * time.Millisecond
	maxDelay := 5 * time.Second

	var err error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		err = write()
		if err == nil {
			p.setErr(nil)
			return nil
		}
		p.setErr(err)
		if attempt == maxAttempts {
			break
		}
		sleep := backoffDuration(base, maxDelay, attempt)
		select {
		case <-time.After(sleep):
		case <-ctx.Done():
			return fmt.Errorf("publish canceled: %w", ctx.Err())
		}
	}
	return err
}

func (p *Producer) Close() error {
	return p.writer.Close()
}

func (p *Producer) Ready(ctx context.Context) bool {
	if len(p.brokers) == 0 {
		return false
	}
	broker := strings.TrimSpace(p.brokers[0])
	if broker == "" {
		return false
	}

	dialer := p.writer.Dialer
	conn, err := dialer.DialContext(ctx, "tcp", broker)
	if err != nil {
		p.setErr(err)
		return false
	}
	_ = conn.Close()
	p.setErr(nil)
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

func buildDialer(cfg Config) (*kafka.Dialer, error) {
	dialer := &kafka.Dialer{
		Timeout:   10 * time.Second,
		DualStack: true,
		ClientID:  cfg.ClientID,
	}

	useTLS := false
	proto := strings.ToUpper(cfg.SecurityProtocol)
	if proto == "SSL" || proto == "SASL_SSL" {
		useTLS = true
	}
	if cfg.TLSCAFile != "" || cfg.TLSCertFile != "" || cfg.TLSKeyFile != "" {
		useTLS = true
	}

	if useTLS {
		tlsConfig, err := buildTLSConfig(cfg)
		if err != nil {
			return nil, err
		}
		dialer.TLS = tlsConfig
	}

	if strings.Contains(proto, "SASL") || cfg.SASLMechanism != "" {
		mechanism, err := buildSASL(cfg)
		if err != nil {
			return nil, err
		}
		dialer.SASLMechanism = mechanism
	}

	return dialer, nil
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

func buildSASL(cfg Config) (sasl.Mechanism, error) {
	mechanism := strings.ToUpper(cfg.SASLMechanism)
	if mechanism == "" {
		mechanism = "PLAIN"
	}

	switch mechanism {
	case "PLAIN":
		return plain.Mechanism{
			Username: cfg.SASLUsername,
			Password: cfg.SASLPassword,
		}, nil
	case "SCRAM-SHA-256":
		return scram.Mechanism(scram.SHA256, cfg.SASLUsername, cfg.SASLPassword)
	case "SCRAM-SHA-512":
		return scram.Mechanism(scram.SHA512, cfg.SASLUsername, cfg.SASLPassword)
	default:
		return nil, fmt.Errorf("unsupported SASL mechanism: %s", cfg.SASLMechanism)
	}
}

func backoffDuration(base, maxDelay time.Duration, attempt int) time.Duration {
	multiplier := 1 << (attempt - 1)
	delay := time.Duration(multiplier) * base
	if delay > maxDelay {
		delay = maxDelay
	}
	jitter := time.Duration(rand.Int63n(int64(delay / 2)))
	return delay + jitter
}
