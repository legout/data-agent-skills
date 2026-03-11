---
name: building-streaming-pipelines
description: "Build real-time data pipelines with Apache Kafka, MQTT (IoT), and NATS JetStream. Covers producers, consumers, streaming patterns, and integration with data platforms. Use when designing or implementing streaming data ingestion, event-driven architectures, or real-time processing workflows in Python."
---

# Building Streaming Pipelines

Build robust, real-time data pipelines in Python. This skill covers the complete streaming lifecycle: producing events, consuming streams, processing in real-time, and operating with production standards for reliability and scalability.

## When to use this skill

Use this skill when:
- Building real-time event streaming pipelines
- Implementing producers and consumers for Kafka, MQTT, or NATS
- Designing IoT data ingestion with constrained devices
- Setting up event-driven microservices communication
- Implementing Change Data Capture (CDC) pipelines
- Handling high-throughput log aggregation
- Designing stream processing with exactly-once semantics
- Implementing backpressure and flow control

## When not to use this skill

Use other skills for:
- **Batch data processing** → `building-data-pipelines`
- **Cloud storage authentication and access** → `accessing-cloud-storage`
- **Lakehouse table formats (Delta/Iceberg)** → `designing-data-storage`
- **Workflow orchestration (Prefect/Dagster)** → `orchestrating-data-pipelines`
- **Data quality frameworks** → `assuring-data-pipelines`
- **AI/ML pipelines (embeddings/vectors)** → `engineering-ai-pipelines`
- **Data observability and monitoring** → `data-engineering-observability`

---

## Quick tool selection

| Use Case | Default choice | When to consider alternatives |
|----------|---------------|------------------------------|
| High-throughput event streaming | **Apache Kafka** | Use NATS for simpler ops, MQTT for IoT |
| IoT devices, constrained networks | **MQTT** | Use Kafka for aggregation layer, NATS for internal routing |
| Cloud-native microservices | **NATS JetStream** | Use Kafka for replayable event logs, MQTT for edge devices |
| Log aggregation, CDC | **Apache Kafka** | NATS for lower throughput, simpler requirements |
| Real-time analytics | **Kafka + ksqlDB** | NATS for simpler stream processing |
| Request-reply patterns | **NATS (core)** | Use gRPC for binary efficiency |

**Decision rule**: Use Kafka for high-volume, replayable event logs. Use MQTT for IoT and mobile with constrained connectivity. Use NATS for cloud-native, microservices, and simpler operational requirements.

---

## Core workflow

### 1. Design the stream

Answer these questions before writing code:

1. **Message volume**: Events per second? Peak vs average?
2. **Durability requirements**: Can you lose messages? Need replay?
3. **Ordering guarantees**: Per-key ordering required? Global ordering?
4. **Delivery semantics**: At-least-once, at-most-once, or exactly-once?
5. **Consumer patterns**: Single consumer, consumer groups, or fan-out?
6. **Schema evolution**: Will message formats change over time?

### 2. Implement producer

```python
# Kafka example
from confluent_kafka import Producer
import json
import socket

def delivery_report(err, msg):
    if err:
        print(f"Delivery failed: {err}")
    else:
        print(f"Delivered to {msg.topic()}[{msg.partition()}]")

conf = {
    'bootstrap.servers': 'localhost:9092',
    'client.id': socket.gethostname(),
    'acks': 'all'  # Wait for all replicas
}

producer = Producer(conf)

data = {'id': 1, 'event': 'user_action', 'value': 100}
producer.produce(
    topic='user_events',
    key=str(data['id']),
    value=json.dumps(data).encode('utf-8'),
    callback=delivery_report
)
producer.flush()
```

### 3. Implement consumer

```python
# Kafka consumer with manual commit
from confluent_kafka import Consumer, KafkaError

conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'my_consumer_group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False  # Manual for reliability
}

consumer = Consumer(conf)
consumer.subscribe(['user_events'])

try:
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            raise KafkaException(msg.error())
        
        # Process message
        data = json.loads(msg.value().decode('utf-8'))
        process_event(data)
        
        # Commit after successful processing
        consumer.commit(asynchronous=False)
except KeyboardInterrupt:
    pass
finally:
    consumer.close()
```

### 4. Validate and operate

- **Consumer lag monitoring**: Track lag per partition
- **Dead letter queues**: Handle poison messages
- **Schema validation**: Validate messages before processing
- **Idempotency**: Handle duplicate delivery gracefully

---

## Production standards

### Idempotent processing

Stream processors may receive duplicates. Design idempotent consumers:

```python
# Idempotent consumer with deduplication
processed_ids = load_checkpoint()  # From Redis/DB

if msg_id in processed_ids:
    consumer.commit()  # Skip duplicate
else:
    process(msg)
    save_checkpoint(msg_id)
    consumer.commit()
```

### Error handling (Dead Letter Queue)

```python
try:
    process(msg)
except RetryableError as e:
    # Retry with backoff
    nack(requeue=True)
except Exception as e:
    # Send to DLQ
    dlq_producer.produce(
        topic='events.dlq',
        value=json.dumps({'original': msg, 'error': str(e)})
    )
    ack()  # Acknowledge original to prevent reprocessing
```

### Schema evolution

- Use Avro/Protobuf with Schema Registry for compatibility
- Evolve schemas additively (new fields optional, old fields preserved)
- Register schemas per topic/subject
- Version schemas and test compatibility

### Batch processing within streams

```python
# Accumulate messages before writing to reduce downstream load
batch = []
while True:
    msg = consumer.poll(timeout=0.1)
    if msg:
        batch.append(msg.value())
    if len(batch) >= BATCH_SIZE or timeout_reached:
        write_to_database(batch)
        consumer.commit()  # Commit after successful batch write
        batch.clear()
```

---

## Progressive disclosure

Start here based on your need:

- **Kafka patterns** → `references/kafka.md` - Producers, consumers, Schema Registry, ksqlDB
- **MQTT for IoT** → `references/mqtt.md` - QoS levels, retained messages, last will
- **NATS JetStream** → `references/nats.md` - Streams, push/pull consumers, work queues

---

## Related skills

- `accessing-cloud-storage` — Cloud storage authentication and remote file access
- `designing-data-storage` — Lakehouse formats (Delta Lake, Iceberg), file formats, storage design
- `orchestrating-data-pipelines` — Prefect, Dagster, dbt for workflow scheduling
- `assuring-data-pipelines` — Data quality testing and observability
- `building-data-pipelines` — Batch data processing with Polars, DuckDB, PyArrow
- `engineering-ai-pipelines` — Embeddings, vector databases, RAG patterns
- `data-engineering-observability` — Monitoring and observability for data pipelines

---

## Migration notes

This skill replaces and consolidates:
- `data-engineering-streaming` — Real-time data pipelines with Kafka, MQTT, NATS

Content has been reorganized into workflow-focused SKILL.md with detailed reference files for each streaming platform.
