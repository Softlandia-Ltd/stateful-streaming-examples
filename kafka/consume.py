"""Consume Kafka messages."""

import json

import plac
from kafka import KafkaConsumer


@plac.opt("addr", "address")
@plac.opt("topic", "topic")
def main(addr: str = "127.0.0.1:9092", topic: str = "iot"):
    """Run the consumer."""
    # Consume latest messages and auto-commit offsets
    consumer = KafkaConsumer(
        # group_id="my-group",
        api_version=(3, 3),
        bootstrap_servers=[addr],
        auto_offset_reset="latest",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )
    consumer.subscribe(topics=[topic])

    print("waiting")

    for message in consumer:
        # e.g., for unicode: `message.value.decode('utf-8')`
        print(
            "%s:%d:%d: key=%s value=%s"
            % (
                message.topic,
                message.partition,
                message.offset,
                message.key,
                message.value,
            )
        )


if __name__ == "__main__":
    plac.call(main)
