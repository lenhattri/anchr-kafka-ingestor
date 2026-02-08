package mqtt

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"time"

	paho "github.com/eclipse/paho.mqtt.golang"
)

type Message struct {
	Topic      string
	Payload    []byte
	ReceivedAt time.Time
}

type Client struct {
	client    paho.Client
	connected atomic.Bool
}

type Config struct {
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

type Handlers struct {
	OnMessage        func(Message)
	OnReconnect      func()
	OnConnect        func()
	OnConnectionLost func(error)
}

func New(cfg Config, handlers Handlers) (*Client, error) {
	opts := paho.NewClientOptions()
	protocol := "tcp"
	if cfg.UseTLS {
		protocol = "ssl"
	}
	broker := fmt.Sprintf("%s://%s:%d", protocol, cfg.BrokerHost, cfg.BrokerPort)
	opts.AddBroker(broker)
	opts.SetClientID(cfg.ClientID)
	opts.SetUsername(cfg.Username)
	opts.SetPassword(cfg.Password)
	opts.SetAutoReconnect(true)
	opts.SetConnectRetry(true)
	opts.SetConnectRetryInterval(2 * time.Second)
	opts.SetKeepAlive(30 * time.Second)
	opts.SetPingTimeout(10 * time.Second)
	opts.SetCleanSession(true)
	opts.SetOrderMatters(cfg.OrderMatters)

	client := &Client{}

	if cfg.UseTLS {
		tlsConfig, err := buildTLSConfig(cfg.CAFile)
		if err != nil {
			return nil, err
		}
		opts.SetTLSConfig(tlsConfig)
	}

	opts.OnConnect = func(_ paho.Client) {
		client.connected.Store(true)
		if handlers.OnConnect != nil {
			handlers.OnConnect()
		}
	}
	opts.OnReconnecting = func(_ paho.Client, _ *paho.ClientOptions) {
		if handlers.OnReconnect != nil {
			handlers.OnReconnect()
		}
	}
	opts.OnConnectionLost = func(_ paho.Client, err error) {
		client.connected.Store(false)
		if handlers.OnConnectionLost != nil {
			handlers.OnConnectionLost(err)
		}
	}

	opts.SetDefaultPublishHandler(func(_ paho.Client, msg paho.Message) {
		if handlers.OnMessage != nil {
			handlers.OnMessage(Message{
				Topic:      msg.Topic(),
				Payload:    msg.Payload(),
				ReceivedAt: time.Now().UTC(),
			})
		}
	})

	client.client = paho.NewClient(opts)
	return client, nil
}

func (c *Client) ConnectAndSubscribe(cfg Config) error {
	if token := c.client.Connect(); token.Wait() && token.Error() != nil {
		return token.Error()
	}

	filters := cfg.SubFilters
	if len(filters) == 0 {
		base := strings.TrimSuffix(cfg.TopicPrefix, "/")
		filters = []string{
			fmt.Sprintf("%s/+/+/+/telemetry", base),
			fmt.Sprintf("%s/+/+/+/state", base),
			fmt.Sprintf("%s/+/+/+/tx", base),
			fmt.Sprintf("%s/+/+/+/ack", base),
			fmt.Sprintf("%s/+/+/+/event", base),
		}
	}
	for idx, filter := range filters {
		filters[idx] = applySharedSubscription(cfg.SharedSubscriptionGroup, filter)
	}

	subscriptions := make(map[string]byte, len(filters))
	for _, filter := range filters {
		subscriptions[filter] = cfg.QoS
	}

	if token := c.client.SubscribeMultiple(subscriptions, nil); token.Wait() && token.Error() != nil {
		return token.Error()
	}
	return nil
}

func (c *Client) Connected() bool {
	return c.connected.Load()
}

func buildTLSConfig(caFile string) (*tls.Config, error) {
	tlsConfig := &tls.Config{}
	if caFile == "" {
		return tlsConfig, nil
	}
	pemData, err := os.ReadFile(caFile)
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pemData) {
		return nil, fmt.Errorf("failed to parse MQTT CA file")
	}
	tlsConfig.RootCAs = pool
	return tlsConfig, nil
}

func applySharedSubscription(group, filter string) string {
	if group == "" {
		return filter
	}
	if strings.HasPrefix(filter, "$share/") {
		return filter
	}
	trimmed := strings.TrimPrefix(filter, "/")
	return fmt.Sprintf("$share/%s/%s", group, trimmed)
}
