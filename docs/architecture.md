# Architecture Notes

## Flow
1. MQTT client subscribes to ANCHR uplink topics (QoS 1) and pushes messages into an in-memory channel.
2. A dispatcher parses topics, counts metrics, and routes messages to a worker shard by `device_id`.
3. Workers validate the JSON envelope, derive routing, and batch Kafka publishes.
4. Kafka producer publishes to raw topics using `device_id` as the message key to preserve per-device ordering.
5. Validation or routing failures are sent to the DLQ with error context.

## Backpressure
Messages are buffered in a bounded channel. When the channel is full, the MQTT handler blocks until
capacity is available. This avoids silent drops and applies backpressure to upstream producers.

## Reliability
Kafka publishes retry with exponential backoff + jitter. After retries are exhausted, messages are
routed to the DLQ to avoid silent loss.
