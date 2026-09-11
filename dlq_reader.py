"""
dlq_reader.py
Small standalone script to prove the DLQ actually contains failed
messages. Run this during the demo after a few permanent failures
have occurred, to print out what landed in 'orders-dlq'.
"""

from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField

BOOTSTRAP_SERVERS = "localhost:9092"
SCHEMA_REGISTRY_URL = "http://localhost:8081"
DLQ_TOPIC = "orders-dlq"

schema_registry_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
with open("order_dlq.avsc") as f:
    dlq_schema_str = f.read()

dlq_deserializer = AvroDeserializer(schema_registry_client, dlq_schema_str)

consumer = Consumer({
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "group.id": "dlq-reader",
    "auto.offset.reset": "earliest",
})
consumer.subscribe([DLQ_TOPIC])

print(f"Reading '{DLQ_TOPIC}'... Press Ctrl+C to stop.")
try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Kafka error: {msg.error()}")
            continue
        record = dlq_deserializer(
            msg.value(), SerializationContext(DLQ_TOPIC, MessageField.VALUE)
        )
        print(f"[DLQ] {record}")
except KeyboardInterrupt:
    pass
finally:
    consumer.close()