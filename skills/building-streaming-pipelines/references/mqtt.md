# MQTT Reference

Comprehensive guide to MQTT for IoT streaming data pipelines in Python.

## Table of Contents

1. [Installation and Setup](#installation-and-setup)
2. [Publisher Patterns](#publisher-patterns)
3. [Subscriber Patterns](#subscriber-patterns)
4. [QoS Levels Explained](#qos-levels-explained)
5. [IoT-Specific Considerations](#iot-specific-considerations)
6. [Production Considerations](#production-considerations)
7. [Troubleshooting](#troubleshooting)

---

## Installation and Setup

### Install the MQTT client

```bash
pip install paho-mqtt
```

### Basic Configuration

```python
import paho.mqtt.client as mqtt
import json
import time

# Common configuration patterns
MQTT_BROKER = "broker.emqx.io"  # Public test broker
MQTT_PORT = 1883
MQTT_KEEPALIVE = 60

# Client configuration
client = mqtt.Client(
    client_id="my_client_id",
    protocol=mqtt.MQTTv5  # Use MQTT 5.0 for best features
)

# With authentication
client.username_pw_set(username="my_user", password="my_password")

# With TLS
client.tls_set(ca_certs="/path/to/ca.crt")
```

---

## Publisher Patterns

### Basic Publisher

```python
import paho.mqtt.client as mqtt
import json
import time

broker = "broker.emqx.io"
topic = "iot/sensors/temperature"

client = mqtt.Client(client_id="publisher_1", protocol=mqtt.MQTTv5)

def on_connect(client, userdata, flags, rc, properties=None):
    """Callback when connected to broker."""
    if rc == 0:
        print("Connected to broker successfully")
    else:
        print(f"Connection failed with code: {rc}")

def on_publish(client, userdata, mid, rc, properties=None):
    """Callback when message is published."""
    print(f"Message {mid} published successfully")

client.on_connect = on_connect
client.on_publish = on_publish
client.connect(broker, 1883, 60)
client.loop_start()

# Publish messages
for i in range(10):
    payload = {
        "sensor_id": "temp_sensor_1",
        "temperature": 20 + i * 0.5,
        "humidity": 45 + i,
        "timestamp": time.time()
    }
    
    result = client.publish(
        topic=topic,
        payload=json.dumps(payload),
        qos=1  # At-least-once delivery
    )
    result.wait_for_publish()  # Wait for acknowledgment
    time.sleep(5)

client.loop_stop()
client.disconnect()
```

### Publisher with Retained Messages

```python
# Retained messages are stored by the broker and delivered to new subscribers
# Useful for device status, configuration, last-known-good values

def publish_device_status():
    """Publish device status as retained message."""
    status = {
        "device_id": "sensor_001",
        "status": "online",
        "firmware_version": "2.1.0",
        "last_seen": time.time()
    }
    
    client.publish(
        topic="devices/sensor_001/status",
        payload=json.dumps(status),
        qos=1,
        retain=True  # Broker stores this message
    )
```

### Publisher with Last Will and Testament (LWT)

```python
# LWT is published automatically if client disconnects ungracefully
def setup_lwt(client, device_id):
    """Configure Last Will for device monitoring."""
    will_payload = json.dumps({
        "device_id": device_id,
        "status": "offline",
        "timestamp": time.time()
    })
    
    client.will_set(
        topic=f"devices/{device_id}/status",
        payload=will_payload,
        qos=1,
        retain=True
    )
    
# Usage
client = mqtt.Client(client_id="sensor_001")
setup_lwt(client, "sensor_001")
client.connect(broker, 1883, 60)

# Publish online status (overwrites LWT retained message)
client.publish(
    topic="devices/sensor_001/status",
    payload=json.dumps({"status": "online", "timestamp": time.time()}),
    qos=1,
    retain=True
)
```

---

## Subscriber Patterns

### Basic Subscriber

```python
import paho.mqtt.client as mqtt
import json

broker = "broker.emqx.io"
topic = "iot/sensors/#"  # Wildcard: matches iot/sensors/anything

def on_connect(client, userdata, flags, rc, properties=None):
    """Subscribe to topics after connecting."""
    if rc == 0:
        print("Connected, subscribing to topics...")
        client.subscribe(topic, qos=1)

def on_message(client, userdata, msg):
    """Handle incoming messages."""
    try:
        payload = json.loads(msg.payload.decode("utf-8"))
        print(f"[{msg.topic}] QoS={msg.qos}: {payload}")
        
        # Process the message
        process_sensor_data(payload)
        
    except json.JSONDecodeError as e:
        print(f"Failed to parse JSON: {e}")
    except Exception as e:
        print(f"Error processing message: {e}")

def process_sensor_data(data):
    """Process sensor data."""
    # Your processing logic here
    pass

client = mqtt.Client(client_id="subscriber_1", protocol=mqtt.MQTTv5)
client.on_connect = on_connect
client.on_message = on_message
client.connect(broker, 1883, 60)

# Blocking loop
client.loop_forever()
```

### Subscriber with Topic Wildcards

```python
# MQTT supports two wildcards:
# + (single level): sensors/+/temperature matches sensors/room1/temperature
# # (multi-level): sensors/# matches sensors/anything/anywhere

def on_connect(client, userdata, flags, rc, properties=None):
    # Subscribe to multiple patterns
    subscriptions = [
        ("sensors/+/temperature", 1),      # Single-level wildcard
        ("sensors/+/humidity", 1),         # Single-level wildcard
        ("devices/+/status", 1),           # Device status
        ("alerts/#", 1),                   # Multi-level wildcard for alerts
    ]
    
    for topic, qos in subscriptions:
        client.subscribe(topic, qos)
        print(f"Subscribed to: {topic}")

client.on_connect = on_connect
```

### Subscriber with Manual Acknowledgment (MQTT 5.0)

```python
def on_message(client, userdata, msg):
    """Process message with manual acknowledgment."""
    try:
        payload = json.loads(msg.payload.decode("utf-8"))
        
        # Process the message
        result = process_message(payload)
        
        # Acknowledge only after successful processing
        # In MQTT 5.0, use msg.mid for manual ack
        print(f"Processed: {result}")
        
    except Exception as e:
        print(f"Processing failed: {e}")
        # Message will be redelivered based on QoS settings
```

---

## QoS Levels Explained

MQTT provides three Quality of Service levels:

### QoS 0 - At Most Once (Fire and Forget)

```python
# Message delivered once or not at all
# No acknowledgment from broker
# Fastest, least reliable

client.publish(topic="sensors/data", payload=payload, qos=0)
```

**Use when:**
- Data is sent frequently
- Losing occasional messages is acceptable
- Network is reliable
- Bandwidth is constrained

### QoS 1 - At Least Once

```python
# Message delivered at least once
# Broker acknowledges receipt
# Possible duplicates

client.publish(topic="sensors/data", payload=payload, qos=1)
```

**Use when:**
- Must not lose messages
- Can handle duplicates (idempotent processing)
- Most common choice for IoT

### QoS 2 - Exactly Once

```python
# Message delivered exactly once
# Four-part handshake (slowest)
# Guaranteed no duplicates

client.publish(topic="critical/data", payload=payload, qos=2)
```

**Use when:**
- Must not lose or duplicate messages
- Processing is expensive or has side effects
- Performance impact is acceptable

### QoS Comparison Table

| QoS | Delivery | Duplicates | Speed | Use Case |
|-----|----------|------------|-------|----------|
| 0 | At most once | Possible | Fastest | High-frequency telemetry |
| 1 | At least once | Possible | Medium | General IoT (recommended) |
| 2 | Exactly once | None | Slowest | Critical commands |

---

## IoT-Specific Considerations

### Constrained Networks

```python
import paho.mqtt.client as mqtt

# Configuration for slow/unreliable networks
client = mqtt.Client(client_id="iot_device_001")

# Longer keepalive for intermittent connectivity
client.connect(broker, 1883, keepalive=120)  # 2 minutes

# For very constrained devices, minimize payload
def publish_minimal(client, topic, sensor_id, value):
    """Publish minimal JSON payload."""
    payload = f'{{"i":"{sensor_id}","v":{value}}}'
    client.publish(topic=topic, payload=payload, qos=1)

# Batch readings when offline
pending_messages = []

def queue_message(topic, payload):
    """Queue message for later delivery."""
    pending_messages.append((topic, payload))

def flush_pending_messages():
    """Send all queued messages when connected."""
    for topic, payload in pending_messages:
        client.publish(topic=topic, payload=payload, qos=1)
    pending_messages.clear()
```

### Last Will for Device Monitoring

```python
class IoTDevice:
    def __init__(self, device_id, broker):
        self.device_id = device_id
        self.client = mqtt.Client(client_id=device_id, protocol=mqtt.MQTTv5)
        self.broker = broker
        
        # Configure Last Will
        self.client.will_set(
            topic=f"devices/{device_id}/status",
            payload=json.dumps({
                "device_id": device_id,
                "status": "offline",
                "timestamp": time.time()
            }),
            qos=1,
            retain=True
        )
    
    def connect(self):
        self.client.connect(self.broker, 1883, 60)
        self.client.loop_start()
        
        # Announce online status
        self.client.publish(
            topic=f"devices/{self.device_id}/status",
            payload=json.dumps({
                "device_id": self.device_id,
                "status": "online",
                "timestamp": time.time()
            }),
            qos=1,
            retain=True
        )
    
    def disconnect(self):
        # Publish offline status before disconnecting (clean shutdown)
        self.client.publish(
            topic=f"devices/{self.device_id}/status",
            payload=json.dumps({
                "device_id": self.device_id,
                "status": "offline",
                "timestamp": time.time()
            }),
            qos=1,
            retain=True
        )
        self.client.loop_stop()
        self.client.disconnect()
```

### Retained Messages for Device State

```python
# Retained messages store the last-known state
# New subscribers immediately receive the retained message

def publish_device_config(device_id, config):
    """Publish device configuration as retained message."""
    client.publish(
        topic=f"devices/{device_id}/config",
        payload=json.dumps(config),
        qos=1,
        retain=True
    )

def get_retained_message(topic, timeout=5):
    """Subscribe and wait for retained message."""
    result = []
    
    def on_message(client, userdata, msg):
        if msg.retain:  # This is a retained message
            result.append(json.loads(msg.payload.decode()))
            client.disconnect()
    
    temp_client = mqtt.Client(client_id="config_reader")
    temp_client.on_message = on_message
    temp_client.connect(broker, 1883, 60)
    temp_client.subscribe(topic, qos=1)
    temp_client.loop_start()
    
    start = time.time()
    while not result and time.time() - start < timeout:
        time.sleep(0.1)
    
    temp_client.loop_stop()
    return result[0] if result else None
```

---

## Production Considerations

### Reconnection Handling

```python
import paho.mqtt.client as mqtt
import time
import random

class RobustMQTTClient:
    def __init__(self, client_id, broker, port=1883):
        self.client_id = client_id
        self.broker = broker
        self.port = port
        self.client = mqtt.Client(client_id=client_id, protocol=mqtt.MQTTv5)
        self.subscriptions = []
        
        # Set callbacks
        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect
        self.client.on_message = self._on_message
        
        # Configure reconnection
        self.client.reconnect_delay_set(min_delay=1, max_delay=120)
    
    def _on_connect(self, client, userdata, flags, rc, properties=None):
        print(f"Connected with result code: {rc}")
        # Resubscribe to all topics
        for topic, qos in self.subscriptions:
            client.subscribe(topic, qos)
    
    def _on_disconnect(self, client, userdata, rc):
        print(f"Disconnected with result code: {rc}")
        if rc != 0:  # Unexpected disconnect
            print("Unexpected disconnect, will auto-reconnect")
    
    def _on_message(self, client, userdata, msg):
        # Override this method
        print(f"Received: {msg.topic} -> {msg.payload.decode()}")
    
    def subscribe(self, topic, qos=1):
        self.subscriptions.append((topic, qos))
        self.client.subscribe(topic, qos)
    
    def connect(self):
        self.client.connect(self.broker, self.port, keepalive=60)
        self.client.loop_start()
    
    def disconnect(self):
        self.client.loop_stop()
        self.client.disconnect()
```

### Message Queueing for Offline Operation

```python
import queue
import threading

class QueuedMQTTPublisher:
    def __init__(self, client, max_queue_size=1000):
        self.client = client
        self.message_queue = queue.Queue(maxsize=max_queue_size)
        self.running = False
        
    def start(self):
        self.running = True
        self.publisher_thread = threading.Thread(target=self._publish_loop)
        self.publisher_thread.daemon = True
        self.publisher_thread.start()
    
    def stop(self):
        self.running = False
        self.publisher_thread.join()
    
    def queue_message(self, topic, payload, qos=1):
        """Queue a message for publication."""
        try:
            self.message_queue.put((topic, payload, qos), timeout=5)
            return True
        except queue.Full:
            print("Message queue full, dropping message")
            return False
    
    def _publish_loop(self):
        while self.running:
            try:
                topic, payload, qos = self.message_queue.get(timeout=1)
                self.client.publish(topic=topic, payload=payload, qos=qos)
            except queue.Empty:
                continue
            except Exception as e:
                print(f"Publish error: {e}")
                # Requeue message on failure
                self.message_queue.put((topic, payload, qos))
```

### TLS/SSL Configuration

```python
import ssl
import paho.mqtt.client as mqtt

client = mqtt.Client(client_id="secure_client")

# Basic TLS (verify server certificate)
client.tls_set(
    ca_certs="/path/to/ca.crt",
    tls_version=ssl.PROTOCOL_TLS
)

# TLS with client certificates (mutual TLS)
client.tls_set(
    ca_certs="/path/to/ca.crt",
    certfile="/path/to/client.crt",
    keyfile="/path/to/client.key",
    tls_version=ssl.PROTOCOL_TLS
)

# Disable certificate verification (for testing only!)
# client.tls_set(cert_reqs=ssl.CERT_NONE)

client.connect("broker.example.com", 8883, 60)
```

---

## Troubleshooting

### Common Issues

```python
# Issue: Connection refused
# Solution: Check broker URL, port, and credentials

# Issue: Client ID conflicts
# Solution: Use unique client IDs, especially for multiple instances
import uuid
client_id = f"device_{uuid.uuid4().hex[:8]}"
client = mqtt.Client(client_id=client_id)

# Issue: Messages not received
# Solution: Check topic subscriptions and wildcards

# Issue: Frequent disconnections
# Solution: Increase keepalive, check network stability
client.connect(broker, 1883, keepalive=120)  # 2 minutes

# Issue: High latency
# Solution: Use QoS 0 for non-critical data, reduce payload size
```

### Debugging Configuration

```python
# Enable logging
import logging
logging.basicConfig(level=logging.DEBUG)

# MQTT client logging
client.enable_logger(logging.getLogger('mqtt'))

# Or use callback-based logging
def on_log(client, userdata, level, buf):
    print(f"[MQTT] {buf}")

client.on_log = on_log
```

### Testing Connectivity

```python
def test_broker_connection(broker, port=1883, timeout=10):
    """Test if MQTT broker is reachable."""
    import socket
    
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        result = sock.connect_ex((broker, port))
        sock.close()
        return result == 0
    except Exception as e:
        print(f"Connection test failed: {e}")
        return False

# Usage
if test_broker_connection("broker.emqx.io"):
    print("Broker is reachable")
else:
    print("Cannot reach broker")
```

---

## References

- [MQTT 5.0 Specification](https://mqtt.org/mqtt5/)
- [Paho MQTT Python Client](https://www.eclipse.org/paho/python.html)
- [MQTT Broker Comparison](https://mqtt.org/software/)
- [EMQX Documentation](https://www.emqx.io/docs/en/latest/)
- [Mosquitto Documentation](https://mosquitto.org/documentation/)
