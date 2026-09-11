"""
consumer.py
Consumes Avro order messages, maintains a running average price per
product, and demonstrates:
  - Retry with backoff for TRANSIENT failures (simulated).
  - A Dead Letter Queue for PERMANENT failures (validation errors, or
    transient failures that exhausted all retries).
"""

import datetime
import random
import time

from confluent_kafka import Consumer, Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer, AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField

BOOTSTRAP_SERVERS = "localhost:9092"
SCHEMA_REGISTRY_URL = "http://localhost:8081"
ORDERS_TOPIC = "orders"
DLQ_TOPIC = "orders-dlq"
GROUP_ID = "order-processor"

MAX_RETRIES = 3
BACKOFF_SECONDS = [0.5, 1, 2]  # exponential-ish backoff between attempts

# --- Custom exceptions to distinguish failure types ---
class ValidationError(Exception):
    """Permanent failure: the message itself is bad. Retrying won't help."""


class TransientError(Exception):
    """Temporary failure: e.g. a flaky downstream call. Worth retrying."""


# --- Schema Registry + Avro (de)serializer setup ---
schema_registry_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})

with open("order.avsc") as f:
    order_schema_str = f.read()
with open("order_dlq.avsc") as f:
    dlq_schema_str = f.read()

avro_deserializer = AvroDeserializer(schema_registry_client, order_schema_str)
dlq_serializer = AvroSerializer(
    schema_registry_client, dlq_schema_str, to_dict=lambda obj, ctx: obj
)

consumer = Consumer({
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "group.id": GROUP_ID,
    "auto.offset.reset": "earliest",
})
consumer.subscribe([ORDERS_TOPIC])

dlq_producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})

# Running average state, kept in memory per product.
running_stats = {}  # product -> {"count": int, "avg": float}


def update_running_average(product: str, price: float) -> float:
    stats = running_stats.setdefault(product, {"count": 0, "avg": 0.0})
    stats["count"] += 1
    # Incremental mean: avg_new = avg_old + (price - avg_old) / count
    stats["avg"] += (price - stats["avg"]) / stats["count"]
    return stats["avg"]


def validate(order: dict):
    """Permanent-failure checks. Raise ValidationError if the message
    itself can never succeed, no matter how many times we retry."""
    if order["price"] <= 0:
        raise ValidationError(f"Invalid price: {order['price']}")


def process_order(order: dict):
    """Simulates the 'real work' done per message. Randomly raises a
    TransientError ~25% of the time to emulate a flaky downstream
    dependency (e.g. a database write or an external API call)."""
    if random.random() < 0.25:
        raise TransientError("Simulated downstream timeout")

    avg = update_running_average(order["product"], order["price"])
    print(f"[CONSUMER] Processed {order} -> running avg for "
          f"{order['product']}: {avg:.2f}")


def send_to_dlq(order: dict, reason: str, retry_count: int):
    dlq_record = {
        "orderId": order["orderId"],
        "product": order["product"],
        "price": order["price"],
        "errorReason": reason,
        "retryCount": retry_count,
        "failedAt": datetime.datetime.utcnow().isoformat(),
    }
    dlq_producer.produce(
        topic=DLQ_TOPIC,
        key=order["orderId"],
        value=dlq_serializer(
            dlq_record, SerializationContext(DLQ_TOPIC, MessageField.VALUE)
        ),
    )
    dlq_producer.flush()
    print(f"[CONSUMER] -> Sent to DLQ ({reason}): {order}")


def handle_message(order: dict):
    # Step 1: permanent validation check. No point retrying these.
    try:
        validate(order)
    except ValidationError as e:
        send_to_dlq(order, str(e), retry_count=0)
        return

    # Step 2: retry loop for transient failures.
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            process_order(order)
            return  # success
        except TransientError as e:
            print(f"[CONSUMER] Attempt {attempt}/{MAX_RETRIES} failed "
                  f"for {order['orderId']}: {e}")
            if attempt < MAX_RETRIES:
                time.sleep(BACKOFF_SECONDS[attempt - 1])
            else:
                # Retries exhausted -> now it's a permanent failure.
                send_to_dlq(order, f"Exhausted retries: {e}", attempt)


if __name__ == "__main__":
    print("Starting consumer. Press Ctrl+C to stop.")
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"[CONSUMER] Kafka error: {msg.error()}")
                continue

            order = avro_deserializer(
                msg.value(), SerializationContext(ORDERS_TOPIC, MessageField.VALUE)
            )
            handle_message(order)
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        print("Consumer shut down cleanly.")