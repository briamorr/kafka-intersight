# Kafka Intersight Poller

> **Note**: This is a sample/example program designed to demonstrate integration between Cisco Intersight and Apache Kafka. It is provided as a reference implementation and not intended for production use without additional hardening, testing, and security review.

A Python-based event poller that fetches alarms and audit logs from Cisco Intersight and streams them to Apache Kafka topics for downstream consumption.

## Features

- **Independent topic watermarks**: Alarms and audits are tracked separately to prevent cross-stream watermark skipping.
- **Delivery confirmation**: Each Kafka message is confirmed before advancing the watermark.
- **Structured logging**: Comprehensive logging with configurable levels (INFO, DEBUG, etc.).
- **Classified retry strategy**: Auth errors fail fast; transient errors use exponential backoff with jitter.
- **Per-run metrics**: Fetched, published, and delivery-failed counters per poll cycle and cumulative totals.

## Prerequisites

- **Python 3.11+**
- **Kafka broker** running on `localhost:9092` (can be configured)
- **Cisco Intersight account** with API credentials (key ID and private key)
- **macOS** (Homebrew recommended for Kafka)

## Installation

### 1. Set up Python Virtual Environment

```bash
cd /path/to/kafka-intersight

# Create and activate virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### 2. Configure Environment Variables

Copy the example file and fill in your Intersight credentials:

```bash
cp .env.example .env
```

Edit `.env` and set:
- `INTERSIGHT_KEY_ID`: Your Intersight API key ID
- `INTERSIGHT_SECRET_KEY_FILE`: Path to your private key file (default: `.secretkey`)
- `INTERSIGHT_ENDPOINT`: Intersight API endpoint (default: `https://intersight.com`)

Optional tuning variables (defaults are safe):
- `LOG_LEVEL`: `INFO` (default) or `DEBUG`
- `DELIVERY_TIMEOUT_SECONDS`: 10.0 (default)
- `RETRY_BASE_SECONDS`: 2.0 (default)
- `RETRY_MAX_SECONDS`: 60.0 (default)
- `RETRY_JITTER_SECONDS`: 1.0 (default)
- `COLD_START_MINUTES`: 5 (default, how far back to look on first run)

### 3. Install and Start Kafka (macOS)

#### Install Kafka with Homebrew

```bash
brew install kafka
```

#### Start Zookeeper (required by Kafka)

```bash
zkServer start
```

You should see:
```
ZooKeeper server started.
```

#### Start Kafka Broker

In a new terminal:

```bash
kafka-server-start /opt/homebrew/etc/kafka/server.properties
```

You should see:
```
[main] INFO kafka.server.KafkaServer - started (kafka.server.KafkaServer)
```

**Verify Kafka is listening:**

```bash
lsof -i :9092
```

Should show the Kafka process listening on port 9092.

## Running the Poller

Activate the virtual environment (if not already active):

```bash
source .venv/bin/activate
```

Start the poller:

```bash
.venv/bin/python kafka-intersight.py
```

You should see logs like:

```
2026-04-03 14:10:20,123 INFO kafka-intersight poller-started endpoint=https://intersight.com kafka_bootstrap=['localhost:9092'] delivery_timeout_s=10.0 retry_base_s=2.0 retry_max_s=60.0
2026-04-03 14:10:21,456 INFO kafka-intersight poll-start topic=intersight-alarms since=2026-04-03T14:05:20.123Z
2026-04-03 14:10:22,789 INFO kafka-intersight poll-start topic=intersight-audits since=2026-04-03T14:05:20.123Z
2026-04-03 14:10:23,012 INFO kafka-intersight run-summary alarms_published=5 audits_published=12 alarms_fetched=5 audits_fetched=12
2026-04-03 14:10:23,034 INFO kafka-intersight metrics-total fetched=17 published=17 delivery_failed=0
2026-04-03 14:10:23,045 INFO kafka-intersight poll-success next_poll_seconds=60
```

The poller starts a continuous loop polling Intersight every 60 seconds (default `POLL_INTERVAL`).

**To stop the poller**, press `Ctrl+C`.

## Verifying Kafka Topics and Data

### List all Kafka topics

```bash
kafka-topics --bootstrap-server localhost:9092 --list
```

You should see:
```
intersight-alarms
intersight-audits
```

### Read all data from a topic (from beginning)

**To see all alarms ever published to the topic:**

```bash
kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic intersight-alarms \
  --from-beginning
```

**To see all audits:**

```bash
kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic intersight-audits \
  --from-beginning
```

Press `Ctrl+C` to stop consuming.

### Read new messages only (tail mode)

**To see only new messages as they arrive (no backlog):**

```bash
kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic intersight-alarms
```

Run this in a terminal, then run the poller in another terminal. New messages will appear in the consumer.

### View messages with metadata (keys, timestamps)

```bash
kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic intersight-alarms \
  --from-beginning \
  --property print.key=true \
  --property print.timestamp=true \
  --property key.separator=" | "
```

Output example:
```
alarm-moid-123 | 1712152200000 | {"moid":"alarm-moid-123","create_time":"2026-04-03T14:10:00Z",...}
alarm-moid-456 | 1712152260000 | {"moid":"alarm-moid-456","create_time":"2026-04-03T14:11:00Z",...}
```

### Pretty-print JSON messages with `jq`

If you have `jq` installed:

```bash
kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic intersight-alarms \
  --from-beginning | jq .
```

Output example:
```json
{
  "moid": "alarm-moid-123",
  "create_time": "2026-04-03T14:10:00Z",
  "acknowledge_time": null,
  "status": "active"
}
```

### Get topic metadata and partition info

```bash
kafka-topics --bootstrap-server localhost:9092 --describe --topic intersight-alarms
```

Output:
```
Topic: intersight-alarms	TopicId: xxxxx	PartitionCount: 1	ReplicationFactor: 1	Configs: 
	Topic: intersight-alarms	Partition: 0	Leader: 0	Replicas: 0	Isr: 0
```

### Count messages in a topic

```bash
kafka-run-class kafka.tools.JmxTool \
  --object-name kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec \
  --reporting-interval 1000 \
  --attributes Count
```

Alternatively, consume all messages and count:

```bash
kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic intersight-alarms \
  --from-beginning \
  --max-messages 1000 | wc -l
```

## Stopping Kafka

In the Kafka terminal, press `Ctrl+C` or:

```bash
kafka-server-stop
```

Stop Zookeeper:

```bash
zkServer stop
```

## Configuration Reference

| Variable | Default | Description |
|---|---|---|
| `INTERSIGHT_KEY_ID` | Required | Intersight API key ID |
| `INTERSIGHT_SECRET_KEY_FILE` | `.secretkey` | Path to Intersight private key file |
| `INTERSIGHT_ENDPOINT` | `https://intersight.com` | Intersight API endpoint |
| `LOG_LEVEL` | `INFO` | Logging level: `INFO`, `DEBUG`, `WARNING`, `ERROR` |
| `DELIVERY_TIMEOUT_SECONDS` | `10.0` | Kafka message delivery confirmation timeout |
| `RETRY_BASE_SECONDS` | `2.0` | Base delay for exponential backoff on transient errors |
| `RETRY_MAX_SECONDS` | `60.0` | Maximum delay for backoff |
| `RETRY_JITTER_SECONDS` | `1.0` | Random jitter added to backoff delay |
| `COLD_START_MINUTES` | `5` | How far back to look on first run (minutes) |
| `BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker(s) (set in code, not .env) |
| `POLL_INTERVAL` | `60` | Poll frequency in seconds (set in code, not .env) |

## Error Handling

The poller classifies errors and responds accordingly:

### Auth/Config Errors (401, 403, invalid key)
- **Behavior**: Fail fast and exit immediately with error log.
- **Action**: Fix credentials/config and restart.

### Transient Errors (timeout, connection reset, service unavailable)
- **Behavior**: Exponential backoff with jitter, retries up to `RETRY_MAX_SECONDS`.
- **Action**: Automatically recovers when service is available.

### Kafka Delivery Errors
- **Behavior**: Logged per record with topic and key; raises immediately.
- **Action**: Check Kafka broker logs and broker connectivity.

## Troubleshooting

### Kafka topics not created
- Verify Kafka is running: `lsof -i :9092`
- Topics are created automatically when the first message is published.
- Check poller logs for errors using `LOG_LEVEL=DEBUG`.

### No messages appearing in topics
- Verify Intersight API credentials in `.env`
- Check poller logs for auth errors: `grep -i "unauthorized\|invalid\|401" <logfile>`
- Verify the Intersight account has alarms/audit logs in the time window (`COLD_START_MINUTES`)

### Consumer hangs or times out
- Verify Kafka broker is running: `lsof -i :9092`
- Try with `--timeout-ms 5000` to prevent infinite wait

### "Connection refused" or "No brokers available"
- Start Kafka: `kafka-server-start /opt/homebrew/etc/kafka/server.properties`
- Verify it listens on 9092: `lsof -i :9092`


## Development

To enable debug logging:

```bash
LOG_LEVEL=DEBUG .venv/bin/python kafka-intersight.py
```

To adjust retry/timeout settings:

```bash
RETRY_BASE_SECONDS=1.0 RETRY_MAX_SECONDS=30.0 DELIVERY_TIMEOUT_SECONDS=5.0 \
  .venv/bin/python kafka-intersight.py
```Example/Sample Status

This is a **reference implementation** and sample program. It demonstrates:
- Loading credentials from environment variables
- Calling the Intersight API with authentication
- Publishing to Kafka topics
- Error classification and retry strategies
- Structured logging

**Before production use**, consider:
- Adding persistence for checkpoints (database, Redis, etc.)
- Implementing comprehensive monitoring and alerting
- Adding secrets management (Vault, Secrets Manager, etc.)
- Conducting security audits and penetration testing
- Performance testing under expected load
- Adding integration and end-to-end tests
- Setting up proper backup and disaster recovery procedures

