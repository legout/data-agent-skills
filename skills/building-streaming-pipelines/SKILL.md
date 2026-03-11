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

### 2. Implement producer and consumer

**Producer pattern** (Kafka example):
```python
from confluent_kafka import Producer
producer = Producer({'bootstrap.servers': 'localhost:9092', 'acks': 'all'})
producer.produce(topic, key=key, value=json.dumps(data).encode(), callback=delivery_report)
producer.flush()
```

**Consumer pattern** (with manual commit for reliability):
```python
from confluent_kafka import Consumer
consumer = Consumer({'bootstrap.servers': 'localhost:9092', 'group.id': 'my_group',
                     'enable.auto.commit': False})
consumer.subscribe(['topic'])
msg = consumer.poll(timeout=1.0)
# Process message, then commit
consumer.commit(asynchronous=False)
```

For detailed patterns with Schema Registry, error handling, and production configuration, see `references/kafka.md`.

### 3. Validate and operate

- **Consumer lag monitoring**: Track lag per partition
- **Dead letter queues**: Handle poison messages
- **Schema validation**: Validate messages before processing
- **Idempotency**: Handle duplicate delivery gracefully

---

## Production standards

### Delivery semantics

| Semantic | When to use | Implementation |
|----------|-------------|----------------|
| **At-most-once** | Loss is acceptable, highest throughput | Auto-commit before processing |
| **At-least-once** | No data loss, duplicates OK | Manual commit after processing |
| **Exactly-once** | No duplicates, financial/critical | Idempotent consumers + transactional writes |

### Idempotency pattern

Stream processors may receive duplicates. Always design idempotent consumers:
```python
if msg_id not in processed_ids:  # Check dedup store (Redis/DB)
    process(msg)
    save_checkpoint(msg_id)
ack()  # Acknowledge after handling (skip or process)
```

### Error handling strategy

- **Retryable errors** (timeouts, transient): Requeue with backoff
- **Non-retryable errors** (schema mismatch, bad data): Send to dead letter queue
- **Always acknowledge** after handling to prevent infinite retry loops

For complete error handling patterns, see platform-specific references.

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

---

## Migration notes

This skill replaces and consolidates:
- `data-engineering-streaming` — Real-time data pipelines with Kafka, MQTT, NATS

Content has been reorganized into workflow-focused SKILL.md with detailed reference files for each streaming platform.
