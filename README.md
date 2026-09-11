# Kafka Order Processing Assignment

## Setup

1. Install Docker and Python 3.9+.
2. `pip install -r requirements.txt`
3. `docker compose up -d` — starts Kafka on `localhost:9092` and Schema
   Registry on `localhost:8081`.
4. Create the topics:
   ```
   docker exec broker kafka-topics --create --topic orders \
     --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
   docker exec broker kafka-topics --create --topic orders-dlq \
     --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
   ```

## Running

Open three terminals:

```
python consumer.py      # terminal 1
python producer.py      # terminal 2
python dlq_reader.py    # terminal 3 (optional, for the demo)
```

You should see the consumer print a running average per product, and
every 8th order (deliberately invalid, negative price) land in the DLQ.
Roughly 1 in 4 valid orders will also hit a simulated transient failure
and retry up to 3 times before succeeding or (rarely) landing in the DLQ.

## Files

- `order.avsc` — Avro schema for order messages.
- `order_dlq.avsc` — Avro schema for the DLQ envelope (original order + failure metadata).
- `producer.py` — generates and sends random Avro-serialized orders.
- `consumer.py` — consumes, computes running average, retries transient failures, sends permanent failures to DLQ.
- `dlq_reader.py` — reads and prints DLQ contents.
- `docker-compose.yml` — local Kafka broker (KRaft mode) + Schema Registry.