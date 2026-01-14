# anchr-kafka-ingestor

Production-grade starter service that ingests ANCHR MQTT uplink topics from EMQX and forwards
messages to Kafka raw topics. The service provides structured JSON logs, Prometheus metrics, and
health probes for Kubernetes.

## Feature Summary
- Subscribes to ANCHR MQTT uplink topics (QoS 1) with optional TLS.
- Validates ANCHR JSON envelope and derives `device_id` from the topic path.
- Routes telemetry/state to `anchr.mqtt.telemetry.raw.v1`, tx to `anchr.mqtt.tx.raw.v1`,
  ack to `anchr.mqtt.ack.raw.v1`, and event to `anchr.mqtt.telemetry.raw.v1` (documented choice).
- Uses a bounded in-memory buffer to apply backpressure instead of dropping messages.
- Writes invalid messages to `anchr.dlq.v1` with error context.
- Exposes `/metrics`, `/healthz`, `/readyz` endpoints.

## Required MQTT Topics
The service subscribes to:
- `anchr/v1/{tenant}/{station}/{pump}/telemetry`
- `anchr/v1/{tenant}/{station}/{pump}/state`
- `anchr/v1/{tenant}/{station}/{pump}/tx`
- `anchr/v1/{tenant}/{station}/{pump}/ack`
- `anchr/v1/{tenant}/{station}/{pump}/event`

## Mapping to Kafka Topics
| MQTT Type | Kafka Topic |
| --- | --- |
| telemetry | `anchr.mqtt.telemetry.raw.v1` |
| state | `anchr.mqtt.telemetry.raw.v1` |
| tx | `anchr.mqtt.tx.raw.v1` |
| ack | `anchr.mqtt.ack.raw.v1` |
| event | `anchr.mqtt.telemetry.raw.v1` (documented choice) |

The Kafka message key is always `device_id` (derived as `{station_id}:{pump_id}`), preserving
per-device ordering.

## Environment Variables
Use `.env.example` as a starting point.

### Core
- `LOG_LEVEL` (default `info`)
- `HTTP_ADDR` (default `:8080`)
- `INGEST_BUFFER_SIZE` (default `1000`) – bounded MQTT -> Kafka buffer size

### MQTT
- `MQTT_BROKER_HOST` (required)
- `MQTT_BROKER_PORT` (default `1883`)
- `MQTT_USERNAME`
- `MQTT_PASSWORD`
- `MQTT_USE_TLS` (`true`/`false`)
- `MQTT_CA_FILE` (optional CA file)
- `MQTT_CLIENT_ID` (default `anchr-kafka-ingestor`)
- `MQTT_QOS` (default `1`)
- `MQTT_TOPIC_PREFIX` (default `anchr/v1`)
- `MQTT_SUB_FILTERS` (optional comma-separated override list)

### Kafka
- `KAFKA_BROKERS` (comma-separated, required)
- `KAFKA_CLIENT_ID` (default `anchr-kafka-ingestor`)
- `KAFKA_SECURITY_PROTOCOL` (`PLAINTEXT`, `SSL`, `SASL_PLAINTEXT`, `SASL_SSL`)
- `KAFKA_SASL_MECHANISM` (`PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512`)
- `KAFKA_SASL_USERNAME`
- `KAFKA_SASL_PASSWORD`
- `KAFKA_TLS_CA_FILE`
- `KAFKA_TLS_CERT_FILE`
- `KAFKA_TLS_KEY_FILE`
- `KAFKA_TLS_SKIP_VERIFY`
- `KAFKA_TOPIC_TELEMETRY_RAW` (default `anchr.mqtt.telemetry.raw.v1`)
- `KAFKA_TOPIC_TX_RAW` (default `anchr.mqtt.tx.raw.v1`)
- `KAFKA_TOPIC_ACK_RAW` (default `anchr.mqtt.ack.raw.v1`)
- `KAFKA_TOPIC_DLQ` (default `anchr.dlq.v1`)

## Local Development
1. Copy env file and adjust for your environment:
   ```bash
   cp .env.example .env
   ```
2. Start dependencies with Docker Compose:
   ```bash
   docker compose up -d kafka mosquitto
   ```
3. Run the service:
   ```bash
   make run
   ```

Metrics and health endpoints:
- `http://localhost:8080/metrics`
- `http://localhost:8080/healthz`
- `http://localhost:8080/readyz`

## Kubernetes (EKS)
1. Update `k8s/configmap.yaml` and `k8s/secret.yaml` with your environment values.
2. Apply manifests:
   ```bash
   kubectl apply -f k8s/
   ```
3. Confirm pods are ready:
   ```bash
   kubectl get pods -l app=anchr-kafka-ingestor
   ```

## DLQ Record Format
DLQ messages are JSON with:
- `original_mqtt_topic`
- `received_at`
- `error_type`
- `error_message`
- `raw_payload`
- `parsed_device_id`

## Integration Test Strategy
A suggested integration test flow:
1. Bring up docker-compose dependencies.
2. Publish a telemetry message to MQTT.
3. Verify the message lands in `anchr.mqtt.telemetry.raw.v1` with `device_id` as the key.
4. Publish an invalid envelope and confirm it lands in `anchr.dlq.v1`.

## Architecture Notes
See [docs/architecture.md](docs/architecture.md) for flow details.
