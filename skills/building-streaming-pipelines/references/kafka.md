# Apache Kafka Reference

Comprehensive guide to Apache Kafka for streaming data pipelines in Python.

## Table of Contents

1. [Installation and Setup](#installation-and-setup)
2. [Producer Patterns](#producer-patterns)
3. [Consumer Patterns](#consumer-patterns)
4. [Schema Registry and Avro](#schema-registry-and-avro)
5. [Stream Processing](#stream-processing)
6. [Production Considerations](#production-considerations)
7. [Troubleshooting](#troubleshooting)

---

## Installation and Setup

### Install the Kafka client

```bash
pip install confluent-kafka
```

For Schema Registry support:
```bash
pip install confluent-kafka[avro]
```

### Basic Configuration

```python
from confluent_kafka import Producer, Consumer, KafkaError
import socket

# Common configuration patterns
KAFKA_BROKERS = 'localhost:9092'

# Producer configuration
producer_config = {
    'bootstrap.servers': KAFKA_BROKERS,
    'client.id': socket.gethostname(),
    'acks': 'all',              # Wait for all replicas
    'retries': 3,               # Retry on transient failures
    'retry.backoff.ms': 1000,   # Wait between retries
    'batch.size': 16384,        # Batch messages before sending
    'linger.ms': 5,             # Wait up to 5ms for batching
    'compression.type': 'gzip'  # Compress messages
}

# Consumer configuration
consumer_config = {
    'bootstrap.servers': KAFKA_BROKERS,
    'group.id': 'my_consumer_group',
    'auto.offset.reset': 'earliest',    # Start from earliest if no commit
    'enable.auto.commit': False,        # Manual commit for reliability
    'max.poll.interval.ms': 300000,     # 5 minutes between polls
    'session.timeout.ms': 45000,        # 45 second session timeout
    'heartbeat.interval.ms': 15000      # Heartbeat every 15 seconds
}
```

---

## Producer Patterns

### Basic Producer

```python
from confluent_kafka import Producer
import json
import socket

def delivery_report(err, msg):
    """Callback for delivery confirmation."""
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()}[{msg.partition()}] at offset {msg.offset()}')

conf = {
    'bootstrap.servers': 'localhost:9092',
    'client.id': socket.gethostname(),
    'acks': 'all'
}

producer = Producer(conf)

# Send messages asynchronously
for i in range(100):
    data = {
        'id': i,
        'event': 'user_activity',
        'value': i * 10,
        'timestamp': time.time()
    }
    producer.produce(
        topic='user_activity_events',
        key=str(i),  # Same key = same partition = ordering
        value=json.dumps(data).encode('utf-8'),
        callback=delivery_report
    )
    producer.poll(0)  # Trigger callbacks

# Wait for all messages to be delivered
producer.flush()
```

### Producer with Error Handling

```python
import time
from confluent_kafka import KafkaException

def produce_with_retry(producer, topic, key, value, max_retries=3):
    """Produce with retry logic for transient errors."""
    for attempt in range(max_retries):
        try:
            producer.produce(
                topic=topic,
                key=key,
                value=value,
                callback=delivery_report
            )
            producer.poll(0)
            return True
        except BufferError:
            # Queue full, wait and retry
            time.sleep(0.1)
            producer.poll(0)
        except KafkaException as e:
            if attempt < max_retries - 1:
                time.sleep(0.5 * (attempt + 1))
            else:
                raise
    return False

# Usage
for i in range(1000):
    success = produce_with_retry(
        producer, 
        'events', 
        str(i), 
        json.dumps({'id': i}).encode()
    )
    if not success:
        print(f"Failed to produce message {i}")

producer.flush()
```

### Producer Partitioning Strategies

```python
# Strategy 1: Round-robin (default, no key)
producer.produce(topic='events', value=b'message_without_key')

# Strategy 2: Key-based (same key -> same partition)
producer.produce(topic='events', key=b'user_123', value=b'message')

# Strategy 3: Custom partitioner (via config)
conf = {
    'bootstrap.servers': 'localhost:9092',
    'partitioner': 'murmur2_random'  # Options: random, roundrobin, murmur2
}
```

---

## Consumer Patterns

### Basic Consumer

```python
from confluent_kafka import Consumer, KafkaError

conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'my_consumer_group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': True,  # Auto-commit for simple cases
    'auto.commit.interval.ms': 5000
}

consumer = Consumer(conf)
consumer.subscribe(['user_activity_events'])

try:
    while True:
        msg = consumer.poll(timeout=1.0)
        
        if msg is None:
            continue
            
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                print(f'Reached end of partition {msg.partition()}')
            else:
                raise KafkaException(msg.error())
            continue
        
        # Process message
        data = json.loads(msg.value().decode('utf-8'))
        print(f'Received from partition {msg.partition()}: {data}')
        
except KeyboardInterrupt:
    pass
finally:
    consumer.close()
```

### Consumer with Manual Commit

```python
conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'reliable_consumer',
    'enable.auto.commit': False,  # Manual commit
    'max.poll.records': 100       # Process up to 100 at a time
}

consumer = Consumer(conf)
consumer.subscribe(['events'])

def process_message(msg):
    """Process a single message."""
    data = json.loads(msg.value().decode('utf-8'))
    # Your processing logic here
    return process_data(data)

try:
    while True:
        msg = consumer.poll(timeout=1.0)
        
        if msg is None:
            continue
            
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            raise KafkaException(msg.error())
        
        # Process the message
        try:
            result = process_message(msg)
            # Commit synchronously after successful processing
            consumer.commit(asynchronous=False)
        except Exception as e:
            print(f"Processing failed: {e}")
            # Don't commit - message will be reprocessed
            
except KeyboardInterrupt:
    pass
finally:
    consumer.close()
```

### Consumer Groups and Rebalancing

```python
# Multiple consumers in the same group partition the load
# Each partition is assigned to exactly one consumer in the group

# Consumer 1
conf1 = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'processing_group',
    'enable.auto.commit': False
}
consumer1 = Consumer(conf1)
consumer1.subscribe(['events'])

# Consumer 2 (same group)
conf2 = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'processing_group',
    'enable.auto.commit': False
}
consumer2 = Consumer(conf2)
consumer2.subscribe(['events'])

# Rebalance listener for custom handling
from confluent_kafka import Consumer, TopicPartition

def on_assign(consumer, partitions):
    print(f'Partitions assigned: {partitions}')
    # Can seek to specific offsets here

def on_revoke(consumer, partitions):
    print(f'Partitions revoked: {partitions}')
    # Commit any pending work
    consumer.commit()

consumer.subscribe(['events'], on_assign=on_assign, on_revoke=on_revoke)
```

---

## Schema Registry and Avro

### Using Avro with Schema Registry

```python
from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

# Schema Registry configuration
schema_registry_client = SchemaRegistryClient({
    'url': 'http://localhost:8081'
})

# Define Avro schema
schema_str = '''
{
    "namespace": "example.avro",
    "type": "record",
    "name": "User",
    "fields": [
        {"name": "id", "type": "int"},
        {"name": "name", "type": "string"},
        {"name": "email", "type": ["null", "string"], "default": null}
    ]
}
'''

# Create Avro serializer
avro_serializer = AvroSerializer(
    schema_registry_client,
    schema_str
)

# Configure producer with serializers
producer = SerializingProducer({
    'bootstrap.servers': 'localhost:9092',
    'key.serializer': StringSerializer('utf_8'),
    'value.serializer': avro_serializer
})

# Produce Avro message
user = {
    'id': 1,
    'name': 'John Doe',
    'email': 'john@example.com'
}

producer.produce(
    topic='user_events',
    key=str(user['id']),
    value=user
)
producer.flush()
```

### Consuming Avro Messages

```python
from confluent_kafka import DeserializingConsumer
from confluent_kafka.serialization import StringDeserializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer

schema_registry_client = SchemaRegistryClient({
    'url': 'http://localhost:8081'
})

# Avro deserializer (schema fetched from registry)
avro_deserializer = AvroDeserializer(
    schema_registry_client,
    schema_str=None  # Will fetch from registry
)

consumer = DeserializingConsumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'avro_consumer',
    'key.deserializer': StringDeserializer('utf_8'),
    'value.deserializer': avro_deserializer,
    'auto.offset.reset': 'earliest'
})

consumer.subscribe(['user_events'])

try:
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            print(f'Error: {msg.error()}')
            continue
        
        # Value is already deserialized as Python dict
        user = msg.value()
        print(f'Received user: {user["name"]} ({user["id"]})')
        
except KeyboardInterrupt:
    pass
finally:
    consumer.close()
```

---

## Stream Processing

### ksqlDB Integration

```python
import requests
import json

KSQLDB_URL = 'http://localhost:8088'

# Create a stream
ksql_create = {
    "ksql": '''
        CREATE STREAM user_events (
            id INT,
            event_type STRING,
            value DOUBLE
        ) WITH (
            KAFKA_TOPIC = 'user_events',
            VALUE_FORMAT = 'JSON',
            PARTITIONS = 4
        );
    '''
}

response = requests.post(
    f'{KSQLDB_URL}/ksql',
    json=ksql_create
)
print(response.json())

# Run a query
query = {
    "ksql": '''
        SELECT id,
               COUNT(*) AS event_count,
               AVG(value) AS avg_value
        FROM user_events
        WINDOW TUMBLING (SIZE 1 MINUTE)
        GROUP BY id
        EMIT CHANGES;
    '''
}

response = requests.post(
    f'{KSQLDB_URL}/query',
    json=query,
    stream=True
)

for line in response.iter_lines():
    if line:
        print(json.loads(line))
```

### Simple Stream Processing in Python

```python
from confluent_kafka import Consumer, Producer

# Simple stream processor: consume, transform, produce
def stream_processor(input_topic, output_topic):
    consumer_conf = {
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'processor_group',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False
    }
    
    producer_conf = {
        'bootstrap.servers': 'localhost:9092',
        'acks': 'all'
    }
    
    consumer = Consumer(consumer_conf)
    producer = Producer(producer_conf)
    
    consumer.subscribe([input_topic])
    
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                continue
            
            # Transform
            data = json.loads(msg.value().decode('utf-8'))
            transformed = {
                'id': data['id'],
                'processed_value': data['value'] * 2,
                'timestamp': time.time()
            }
            
            # Produce to output topic
            producer.produce(
                topic=output_topic,
                key=msg.key(),
                value=json.dumps(transformed).encode('utf-8')
            )
            
            # Commit after successful production
            consumer.commit(asynchronous=False)
            
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        producer.flush()
```

---

## Production Considerations

### Partitioning Strategy

```python
# Number of partitions affects:
# - Parallelism (max consumers = partitions)
# - Ordering (same key -> same partition)
# - Throughput

# Calculate partitions based on throughput
# Target: ~10MB/s per partition, ~10k messages/s per partition

# Example: Need 100MB/s throughput, 50MB messages
# Partitions = 100MB/s / 10MB/s = 10 partitions

# Create topic with specific partitions
from confluent_kafka.admin import AdminClient, NewTopic

admin = AdminClient({'bootstrap.servers': 'localhost:9092'})

topic = NewTopic(
    'high_throughput_events',
    num_partitions=12,
    replication_factor=3,
    config={
        'retention.ms': 7 * 24 * 60 * 60 * 1000,  # 7 days
        'cleanup.policy': 'delete'
    }
)

fs = admin.create_topics([topic])
for topic, future in fs.items():
    try:
        future.result()
        print(f"Topic {topic} created")
    except Exception as e:
        print(f"Failed to create topic {topic}: {e}")
```

### Replication and Durability

```python
# acks=all ensures message is written to all replicas
# min.insync.replicas=2 means at least 2 replicas must acknowledge

producer_config = {
    'bootstrap.servers': 'localhost:9092',
    'acks': 'all',                    # Wait for all ISRs
    'retries': 5,
    'retry.backoff.ms': 1000,
    'enable.idempotence': True,       # Exactly-once semantics
    'max.in.flight.requests.per.connection': 5,
    'compression.type': 'lz4'
}
```

### Monitoring Consumer Lag

```python
from confluent_kafka.admin import AdminClient

admin = AdminClient({'bootstrap.servers': 'localhost:9092'})

# Get consumer group offsets
groups = admin.list_groups()
for group in groups:
    if group == 'my_consumer_group':
        # Describe the group
        future = admin.describe_consumer_groups([group])
        result = future[group].result()
        
        for member in result.members:
            print(f"Member: {member.client_id}")
            for assignment in member.assignment.topic_partitions:
                print(f"  Partition: {assignment.partition}")
```

---

## Troubleshooting

### Common Issues

```python
# Issue: Connection refused
# Solution: Check broker is running, verify bootstrap.servers

# Issue: UnknownTopicOrPartition
# Solution: Topic doesn't exist, create it first
from confluent_kafka.admin import AdminClient, NewTopic

admin = AdminClient({'bootstrap.servers': 'localhost:9092'})
topic = NewTopic('my_topic', num_partitions=1, replication_factor=1)
admin.create_topics([topic])

# Issue: Consumer not receiving messages
# Solutions:
# 1. Check auto.offset.reset (earliest vs latest)
# 2. Verify consumer group hasn't committed offset
# 3. Check partition assignment with on_assign callback

# Issue: Message delivery failures
# Solution: Implement retry with exponential backoff
import time

def produce_with_backoff(producer, topic, value, max_retries=3):
    for i in range(max_retries):
        try:
            producer.produce(topic=topic, value=value)
            return
        except Exception as e:
            if i < max_retries - 1:
                time.sleep(2 ** i)  # Exponential backoff
            else:
                raise
```

### Debugging Configuration

```python
# Enable debug logging
debug_config = {
    'bootstrap.servers': 'localhost:9092',
    'debug': 'broker,topic,msg'  # Comma-separated debug contexts
}

# Available debug contexts:
# generic, broker, topic, metadata, feature, queue, msg, protocol, 
# cgrp, security, fetch, interceptor, plugin, consumer, admin, eos, all
```

---

## References

- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [confluent-kafka Python Client](https://github.com/confluentinc/confluent-kafka-python)
- [Kafka Streams Documentation](https://kafka.apache.org/documentation/streams/)
- [ksqlDB Documentation](https://docs.ksqldb.io/)
- [Schema Registry Documentation](https://docs.confluent.io/platform/current/schema-registry/index.html)
