# Architecture Notes

## Flow
1. MQTT client subscribes to ANCHR uplink topics (QoS 1) and pushes messages into an in-memory channel.
2. A single worker validates the topic + JSON envelope, derives `device_id`, and routes to Kafka.
3. Kafka producer publishes to raw topics using `device_id` as the message key to preserve ordering.
4. Validation or routing failures are sent to the DLQ with error context.

## Backpressure
Messages are buffered in a bounded channel. When the channel is full, the MQTT handler blocks until
capacity is available. This avoids silent drops and applies backpressure to upstream producers.

## Reliability
Kafka publishes retry with exponential backoff + jitter. After retries are exhausted, messages are
routed to the DLQ to avoid silent loss.
