"""Produce IoT events to the Kafka stream."""

import json
import time
from typing import List

import numpy as np
import plac
from kafka import KafkaProducer


def json_serializer(msg):
    """Serialize to JSON."""
    return json.dumps(msg).encode("utf-8")


def generate_messages(
    rng: np.random.Generator, n_sensors: int = 3, lambda_: int = 2
) -> List[dict]:
    """Generate a list of messages to send."""
    values = [v.item() for v in rng.poisson(lambda_, n_sensors)]
    ids = range(n_sensors)
    msgs = [{"id": i, "time": time.time(), "value": values[i]} for i in ids]
    return msgs


@plac.opt("addr", "Address")
@plac.opt("topic", "Kafka topic")
@plac.opt("sensors", "Number of sensors")
def main(addr: str = "127.0.0.1:9092", topic: str = "iot", sensors: int = 3):
    """Run the producer."""
    producer = KafkaProducer(
        bootstrap_servers=[addr], api_version=(3, 3), value_serializer=json_serializer
    )

    rng = np.random.default_rng(0)
    # Asynchronous by default
    while True:
        msgs = generate_messages(rng, n_sensors=sensors)
        print(msgs)
        for m in msgs:
            producer.send(topic, m)

        time.sleep(1)


if __name__ == "__main__":
    plac.call(main)
