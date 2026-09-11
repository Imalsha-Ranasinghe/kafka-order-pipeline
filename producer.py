"""
producer.py
Generates random order messages, serializes them as Avro, and sends them
to the 'orders' topic. Occasionally emits an intentionally invalid order
(negative price) so the consumer's DLQ path can be demonstrated.
"""

import random
import time
import uuid

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField

BOOTSTRAP_SERVERS = "localhost:9092"
SCHEMA_REGISTRY_URL = "http://localhost:8081"
TOPIC = "orders"

PRODUCTS = ["Item1", "Item2", "Item3", "Item4", "Item5"]

# --- Schema Registry + Avro serializer setup ---
schema_registry_conf = {"url": SCHEMA_REGISTRY_URL}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)

with open("order.avsc", "r") as f:
    order_schema_str = f.read()

avro_serializer = AvroSerializer(
    schema_registry_client,
    order_schema_str,
    # This function tells the serializer how to turn our Python dict
    # into the fields defined in order.avsc. For a plain dict matching
    # field names 1:1, the default behaviour already works, but being
    # explicit here makes it obvious what's happening.
    to_dict=lambda obj, ctx: obj,
)

producer_conf = {"bootstrap.servers": BOOTSTRAP_SERVERS}
producer = Producer(producer_conf)


def delivery_report(err, msg):
    """Called once per message to report success/failure of delivery
    to the broker. This is Kafka's own delivery guarantee, separate
    from our own retry/DLQ logic which runs in the consumer."""
    if err is not None:
        print(f"[PRODUCER] Delivery failed for {msg.key()}: {err}")
    else:
        print(f"[PRODUCER] Delivered to {msg.topic()} [{msg.partition()}]")


def make_order(force_invalid: bool = False) -> dict:
    price = round(random.uniform(10.0, 500.0), 2)
    if force_invalid:
        price = -price  # invalid: negative price -> will fail validation downstream
    return {
        "orderId": str(uuid.uuid4())[:8],
        "product": random.choice(PRODUCTS),
        "price": price,
    }


def produce_order(order: dict):
    producer.produce(
        topic=TOPIC,
        key=order["orderId"],
        value=avro_serializer(
            order, SerializationContext(TOPIC, MessageField.VALUE)
        ),
        on_delivery=delivery_report,
    )
    producer.poll(0)


if __name__ == "__main__":
    print("Starting producer. Press Ctrl+C to stop.")
    count = 0
    try:
        while True:
            count += 1
            # Roughly 1 in 8 messages is deliberately invalid, to prove
            # the DLQ path works during the live demo.
            invalid = (count % 8 == 0)
            order = make_order(force_invalid=invalid)
            produce_order(order)
            print(f"[PRODUCER] Sent {'INVALID ' if invalid else ''}order: {order}")
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        producer.flush()
        print("Producer shut down cleanly.")