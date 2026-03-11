# NATS JetStream Reference

Comprehensive guide to NATS JetStream for streaming data pipelines in Python.

## Table of Contents

1. [Installation and Setup](#installation-and-setup)
2. [Publisher Patterns](#publisher-patterns)
3. [Consumer Patterns](#consumer-patterns)
4. [Push vs Pull Consumers](#push-vs-pull-consumers)
5. [Stream Configuration](#stream-configuration)
6. [Work Queue Patterns](#work-queue-patterns)
7. [Request-Reply Patterns](#request-reply-patterns)
8. [Production Considerations](#production-considerations)
9. [Troubleshooting](#troubleshooting)

---

## Installation and Setup

### Install the NATS client

```bash
pip install nats-py
```

### Basic Configuration

```python
import asyncio
import nats
from nats.errors import ConnectionClosedError, TimeoutError

# Connection options dictionary
options = {
    "servers": ["nats://localhost:4222"],
    "name": "my_client",
    "reconnect_time_wait": 2,
    "max_reconnect_attempts": 10,
    "ping_interval": 20,
    "max_outstanding_pings": 2,
}

# All NATS operations are async - wrap in async functions
async def connect_with_options():
    nc = await nats.connect(**options)
    return nc

async def connect_with_auth():
    nc = await nats.connect(
        servers=["nats://localhost:4222"],
        user="my_user",
        password="my_password"
    )
    return nc

async def connect_with_token():
    nc = await nats.connect(
        servers=["nats://localhost:4222"],
        token="my_token"
    )
    return nc

async def connect_with_tls():
    import ssl
    ssl_context = ssl.create_default_context()
    ssl_context.load_verify_locations('/path/to/ca.pem')
    nc = await nats.connect(
        servers=["nats://localhost:4222"],
        tls=ssl_context
    )
    return nc
```

---

## Publisher Patterns

### Basic Publisher (Core NATS)

```python
import asyncio
import nats
import json
import time

async def basic_publisher():
    # Connect to NATS
    nc = await nats.connect("nats://localhost:4222")

    # Publish to a subject (fire-and-forget)
    await nc.publish(
        subject="events.user.created",
        payload=b'{"user_id": 123, "name": "Alice"}'
    )

    # With JSON payload
    data = {
        "user_id": 123,
        "name": "Alice",
        "timestamp": time.time()
    }
    await nc.publish(
        subject="events.user.created",
        payload=json.dumps(data).encode()
    )

    # Flush to ensure delivery
    await nc.flush()
    await nc.close()

asyncio.run(basic_publisher())
```

### JetStream Publisher

```python
import asyncio
import nats
import json

async def jetstream_publisher():
    nc = await nats.connect("nats://localhost:4222")
    js = nc.jetstream()

    # Create stream first (or ensure it exists)
    await js.add_stream(
        name="EVENTS",
        subjects=["events.*"]
    )

    # Publish with acknowledgment
    ack = await js.publish(
        subject="events.user.created",
        payload=json.dumps({"user_id": 123}).encode()
    )
    print(f"Published: stream={ack.stream}, sequence={ack.seq}")

    # Publish with headers
    headers = {
        "source": "web-app",
        "version": "1.0"
    }
    await js.publish(
        subject="events.user.created",
        payload=b'{"user_id": 456}',
        headers=headers
    )

    await nc.close()

asyncio.run(jetstream_publisher())
```

### Publisher with Retry

```python
import asyncio
import nats
from nats.errors import TimeoutError, NoRespondersError

async def publish_with_retry(js, subject, payload, max_retries=3):
    """Publish with retry logic for transient errors."""
    for attempt in range(max_retries):
        try:
            ack = await js.publish(subject, payload)
            return ack
        except TimeoutError:
            if attempt < max_retries - 1:
                await asyncio.sleep(0.5 * (attempt + 1))
            else:
                raise
        except Exception as e:
            print(f"Publish error: {e}")
            if attempt == max_retries - 1:
                raise

# Usage
async def main():
    nc = await nats.connect("nats://localhost:4222")
    js = nc.jetstream()

    ack = await publish_with_retry(
        js,
        "events.user.created",
        b'{"user_id": 123}'
    )
    print(f"Published with ack: {ack.seq}")

    await nc.close()

asyncio.run(main())
```

---

## Consumer Patterns

### Basic Consumer (Push Consumer)

```python
import asyncio
import nats
import json

async def push_consumer():
    nc = await nats.connect("nats://localhost:4222")
    js = nc.jetstream()

    # Subscribe to a stream (push consumer)
    # Messages are pushed to the subscriber automatically
    sub = await js.subscribe(
        subject="events.*",
        stream="EVENTS",
        durable="my_consumer"  # Durable name for state persistence
    )

    async for msg in sub.messages:
        try:
            data = json.loads(msg.data.decode())
            print(f"Received: subject={msg.subject}, data={data}")

            # Acknowledge the message
            await msg.ack()

        except Exception as e:
            print(f"Processing error: {e}")
            # Negative acknowledge for redelivery
            await msg.nak()

    await nc.close()

asyncio.run(push_consumer())
```

### Pull Consumer

```python
import asyncio
import nats
import json

async def pull_consumer():
    nc = await nats.connect("nats://localhost:4222")
    js = nc.jetstream()

    # Create pull consumer
    sub = await js.pull_subscribe(
        subject="events.*",
        durable="my_pull_consumer",
        stream="EVENTS"
    )

    while True:
        try:
            # Fetch messages in batches
            msgs = await sub.fetch(batch=10, timeout=5)

            for msg in msgs:
                try:
                    data = json.loads(msg.data.decode())
                    print(f"Processing: {data}")
                    await msg.ack()
                except Exception as e:
                    print(f"Error: {e}")
                    await msg.nak()

        except nats.errors.TimeoutError:
            # No messages available
            continue
        except Exception as e:
            print(f"Fetch error: {e}")
            await asyncio.sleep(1)

    await nc.close()

asyncio.run(pull_consumer())
```

### Consumer with Manual Acknowledgment

```python
import asyncio
import nats
import json

async def consumer_with_ack_handling():
    nc = await nats.connect("nats://localhost:4222")
    js = nc.jetstream()

    sub = await js.subscribe("events.*", stream="EVENTS", durable="ack_consumer")

    async for msg in sub.messages:
        try:
            data = json.loads(msg.data.decode())

            # Process the message
            result = await process_message(data)

            if result.success:
                # Acknowledge successful processing
                await msg.ack()
            elif result.retryable:
                # Negative acknowledge with delay for retry
                await msg.nak(delay=30)  # Retry after 30 seconds
            else:
                # Terminate message (no more retries)
                await msg.term()

        except Exception as e:
            print(f"Processing error: {e}")
            # Nak for redelivery
            await msg.nak()

    await nc.close()

async def process_message(data):
    """Process message and return result."""
    # Your processing logic here
    class Result:
        success = True
        retryable = False
    return Result()

asyncio.run(consumer_with_ack_handling())
```

---

## Push vs Pull Consumers

### Comparison Table

| Feature | Push Consumer | Pull Consumer |
|---------|---------------|---------------|
| **Message Flow** | Broker pushes to client | Client requests messages |
| **Flow Control** | Limited (rate limiting) | Full control (batch size) |
| **Latency** | Lower (immediate push) | Higher (polling interval) |
| **Use Case** | Real-time processing | Batch processing, backpressure |
| **Scalability** | Auto-scales | Manual scaling |

### Push Consumer Example

```python
# Push consumer: messages are delivered automatically
# Good for real-time, low-latency processing

async def push_example():
    nc = await nats.connect("nats://localhost:4222")
    js = nc.jetstream()

    # Push consumer with manual ack
    sub = await js.subscribe(
        "events.*",
        stream="EVENTS",
        durable="push_consumer",
        manual_ack=True  # Required for reliable processing
    )

    async for msg in sub.messages:
        await process(msg)
        await msg.ack()

    await nc.close()
```

### Pull Consumer Example

```python
# Pull consumer: client fetches messages
# Good for batch processing, rate limiting, backpressure handling

async def pull_example():
    nc = await nats.connect("nats://localhost:4222")
    js = nc.jetstream()

    sub = await js.pull_subscribe(
        "events.*",
        stream="EVENTS",
        durable="pull_consumer"
    )

    while True:
        # Fetch up to 100 messages at a time
        msgs = await sub.fetch(batch=100, timeout=5)

        # Process batch
        batch_data = []
        for msg in msgs:
            batch_data.append(json.loads(msg.data.decode()))
            await msg.ack()

        # Write batch to database
        await write_batch(batch_data)

    await nc.close()
```

---

## Stream Configuration

### Creating Streams

```python
import asyncio
import nats

async def create_streams():
    nc = await nats.connect("nats://localhost:4222")
    js = nc.jetstream()

    # Basic stream
    await js.add_stream(
        name="EVENTS",
        subjects=["events.*"]
    )

    # Stream with file storage and retention
    await js.add_stream(
        name="PERSISTENT_EVENTS",
        subjects=["persistent.*"],
        storage="file",           # File-based storage
        retention="limits",       # Retain based on limits
        max_msgs=100000,          # Max messages
        max_bytes=1024*1024*1024, # 1GB max
        max_age=86400,            # 24 hours
        replicas=3                # Replication factor
    )

    # Work queue stream (each message processed once)
    await js.add_stream(
        name="JOBS",
        subjects=["jobs.*"],
        storage="file",
        retention="workqueue",    # Work queue semantics
        max_msgs=10000
    )

    # Interest-based retention
    await js.add_stream(
        name="NOTIFICATIONS",
        subjects=["notifications.*"],
        retention="interest",     # Remove when all consumers ack
        max_msgs=5000
    )

    await nc.close()

asyncio.run(create_streams())
```

### Stream Information

```python
async def stream_info():
    nc = await nats.connect("nats://localhost:4222")
    js = nc.jetstream()

    # Get stream info
    info = await js.stream_info("EVENTS")
    print(f"Stream: {info.config.name}")
    print(f"Messages: {info.state.messages}")
    print(f"Bytes: {info.state.bytes}")
    print(f"First sequence: {info.state.first_seq}")
    print(f"Last sequence: {info.state.last_seq}")

    await nc.close()

asyncio.run(stream_info())
```

### Updating Streams

```python
async def update_stream():
    nc = await nats.connect("nats://localhost:4222")
    js = nc.jetstream()

    # Update stream configuration
    await js.update_stream(
        name="EVENTS",
        max_msgs=500000,          # Increase max messages
        max_age=172800            # Extend to 48 hours
    )

    await nc.close()

asyncio.run(update_stream())
```

---

## Work Queue Patterns

### Job Producer

```python
import asyncio
import nats
import json
import uuid

async def job_producer():
    nc = await nats.connect("nats://localhost:4222")
    js = nc.jetstream()

    # Create work queue stream
    try:
        await js.add_stream(
            name="JOBS",
            subjects=["jobs.*"],
            storage="file",
            retention="workqueue"
        )
    except Exception:
        pass  # Stream already exists

    # Submit jobs
    for i in range(10):
        job = {
            "job_id": str(uuid.uuid4()),
            "type": "process_data",
            "payload": {"data_id": i},
            "submitted_at": asyncio.get_event_loop().time()
        }

        ack = await js.publish(
            subject="jobs.process_data",
            payload=json.dumps(job).encode()
        )
        print(f"Job submitted: {job['job_id']}, seq={ack.seq}")

    await nc.close()

asyncio.run(job_producer())
```

### Job Worker

```python
import asyncio
import nats
import json

async def job_worker(worker_id):
    nc = await nats.connect("nats://localhost:4222")
    js = nc.jetstream()

    # Join work queue (same durable name = shared queue)
    sub = await js.pull_subscribe(
        "jobs.*",
        stream="JOBS",
        durable="workers"  # All workers share this durable name
    )

    print(f"Worker {worker_id} started")

    while True:
        try:
            # Pull one job at a time
            msgs = await sub.fetch(batch=1, timeout=5)

            for msg in msgs:
                job = json.loads(msg.data.decode())
                print(f"Worker {worker_id} processing: {job['job_id']}")

                # Process the job
                await process_job(job)

                # Acknowledge completion
                await msg.ack()
                print(f"Worker {worker_id} completed: {job['job_id']}")

        except nats.errors.TimeoutError:
            continue
        except Exception as e:
            print(f"Worker {worker_id} error: {e}")
            await asyncio.sleep(1)

    await nc.close()

async def process_job(job):
    """Simulate job processing."""
    await asyncio.sleep(1)  # Simulate work

# Run multiple workers
async def main():
    workers = [job_worker(i) for i in range(3)]
    await asyncio.gather(*workers)

asyncio.run(main())
```

---

## Request-Reply Patterns

### Request (Client)

```python
import asyncio
import nats
import json

async def request_reply_client():
    nc = await nats.connect("nats://localhost:4222")

    # Send request and wait for response
    response = await nc.request(
        subject="service.calculate",
        payload=json.dumps({"x": 10, "y": 20}).encode(),
        timeout=5
    )

    result = json.loads(response.data.decode())
    print(f"Response: {result}")

    await nc.close()

asyncio.run(request_reply_client())
```

### Reply (Service)

```python
import asyncio
import nats
import json

async def request_reply_service():
    nc = await nats.connect("nats://localhost:4222")

    async def handle_request(msg):
        """Handle incoming request."""
        request = json.loads(msg.data.decode())

        # Process request
        result = request["x"] + request["y"]

        # Send response
        response = json.dumps({"result": result})
        await msg.respond(response.encode())

    # Subscribe to requests
    sub = await nc.subscribe("service.calculate", cb=handle_request)

    print("Service listening on service.calculate")

    # Keep running
    try:
        while True:
            await asyncio.sleep(1)
    except asyncio.CancelledError:
        pass

    await sub.unsubscribe()
    await nc.close()

asyncio.run(request_reply_service())
```

---

## Production Considerations

### Connection Management

```python
import asyncio
import nats
from nats.errors import ConnectionClosedError

class RobustNATSClient:
    def __init__(self, servers):
        self.servers = servers
        self.nc = None
        self.js = None

    async def connect(self):
        """Connect with reconnection handling."""
        options = {
            "servers": self.servers,
            "reconnect_time_wait": 2,
            "max_reconnect_attempts": -1,  # Infinite
            "dont_randomize": True,
            "reconnected_cb": self._on_reconnect,
            "disconnected_cb": self._on_disconnect,
            "closed_cb": self._on_close,
            "error_cb": self._on_error,
        }

        self.nc = await nats.connect(**options)
        self.js = self.nc.jetstream()

    async def _on_reconnect(self):
        print("Reconnected to NATS")

    async def _on_disconnect(self):
        print("Disconnected from NATS")

    async def _on_close(self):
        print("Connection closed")

    async def _on_error(self, e):
        print(f"NATS error: {e}")

    async def close(self):
        if self.nc:
            await self.nc.close()

# Usage
async def main():
    client = RobustNATSClient(["nats://localhost:4222"])
    await client.connect()

    # Use client.js for JetStream operations

    await client.close()

asyncio.run(main())
```

### Consumer Durability

```python
# Durable consumers persist their state across restarts
# Ephemeral consumers lose state on disconnect

async def create_durable_consumer(js):
    # Durable consumer (state persists across restarts)
    sub = await js.subscribe(
        "events.*",
        stream="EVENTS",
        durable="my_durable_consumer"  # State saved
    )
    return sub

async def create_ephemeral_consumer(js):
    # Ephemeral consumer (state lost on disconnect)
    sub = await js.subscribe(
        "events.*",
        stream="EVENTS"
        # No durable name = ephemeral
    )
    return sub

async def delete_consumer(js):
    # Delete a consumer
    await js.delete_consumer("EVENTS", "my_durable_consumer")
```

### Monitoring Consumer Lag

```python
async def check_consumer_lag():
    nc = await nats.connect("nats://localhost:4222")
    js = nc.jetstream()

    # Get consumer info
    consumer = await js.consumer_info("EVENTS", "my_consumer")

    print(f"Delivered: {consumer.delivered.stream_seq}")
    print(f"Acknowledged: {consumer.ack_floor.stream_seq}")
    print(f"Pending: {consumer.num_pending}")
    print(f"Waiting: {consumer.num_waiting}")

    # Calculate lag
    stream = await js.stream_info("EVENTS")
    lag = stream.state.last_seq - consumer.delivered.stream_seq
    print(f"Lag: {lag} messages")

    await nc.close()

asyncio.run(check_consumer_lag())
```

---

## Troubleshooting

### Common Issues

```python
import asyncio
import nats

# Issue: Connection refused
# Solution: Check NATS server is running and accessible

# Issue: Stream not found
# Solution: Create the stream first
async def ensure_stream_exists(js):
    try:
        await js.add_stream(name="EVENTS", subjects=["events.*"])
    except nats.js.errors.StreamAlreadyExistsError:
        pass  # Stream exists

# Issue: Consumer not found
# Solution: Create consumer or check durable name
async def ensure_consumer_exists(js):
    try:
        sub = await js.pull_subscribe("events.*", stream="EVENTS", durable="my_consumer")
    except nats.js.errors.ConsumerNotFoundError:
        # Consumer doesn't exist, create it
        await js.add_consumer(
            "EVENTS",
            durable_name="my_consumer",
            ack_policy="explicit"
        )
        sub = await js.pull_subscribe("events.*", stream="EVENTS", durable="my_consumer")
    return sub

# Issue: Message timeout
# Solution: Increase timeout or check consumer processing
async def fetch_with_longer_timeout(sub):
    msgs = await sub.fetch(batch=10, timeout=30)  # Longer timeout
    return msgs
```

### Debugging

```python
import asyncio
import nats
import logging

# Enable debug logging
logging.basicConfig(level=logging.DEBUG)

async def debug_connection():
    # Connect with debug options
    nc = await nats.connect(
        servers=["nats://localhost:4222"],
        verbose=True,  # Enable verbose mode
        allow_reconnect=True,
        reconnect_time_wait=2
    )

    # Test connectivity
    print(f"Connected: {nc.is_connected}")
    print(f"Server: {nc.connected_url}")

    await nc.close()

asyncio.run(debug_connection())
```

### Testing Server Connectivity

```python
import asyncio
import socket

async def test_nats_server(host, port=4222, timeout=5):
    """Test if NATS server is reachable."""
    try:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(host, port),
            timeout=timeout
        )
        writer.close()
        await writer.wait_closed()
        return True
    except Exception as e:
        print(f"Connection test failed: {e}")
        return False

# Usage
async def main():
    if await test_nats_server("localhost", 4222):
        print("NATS server is reachable")
    else:
        print("Cannot reach NATS server")

asyncio.run(main())
```

---

## References

- [NATS Documentation](https://docs.nats.io/)
- [NATS JetStream Documentation](https://docs.nats.io/jetstream)
- [nats-py Python Client](https://github.com/nats-io/nats.py)
- [NATS Server (nats-server)](https://github.com/nats-io/nats-server)
- [NATS CLI Tool](https://github.com/nats-io/natscli)
