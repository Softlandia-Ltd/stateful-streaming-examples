"""Example of stateful stream processing with Bytewax."""

from datetime import timedelta
import json

import plac
from bytewax.dataflow import Dataflow
from bytewax.inputs import KafkaInputConfig
from bytewax.outputs import StdOutputConfig
from bytewax.execution import run_main
from bytewax.window import SystemClockConfig, TumblingWindowConfig


def deserialize(key_bytes__payload_bytes: tuple) -> tuple[str, dict]:
    """Deserialize Kafka messages.

    Will return tuples of (id, msg).
    """
    _key_bytes, payload_bytes = key_bytes__payload_bytes
    payload = json.loads(payload_bytes.decode("utf-8"))
    return payload["id"], payload


@plac.opt("addr", "Broker address")
@plac.opt("topic", "Kafka topic")
@plac.opt("win", "Window length in seconds")
def main(addr: str = "127.0.0.1:9092", topic: str = "iot", win: int = 10):
    """Run the stream processing flow."""
    clock_config = SystemClockConfig()

    # We'll operate in 10 second windows
    window_config = TumblingWindowConfig(length=timedelta(seconds=win))

    # Initialize a flow
    flow = Dataflow()
    # Input is our Kafka stream
    flow.input(
        "input",
        KafkaInputConfig(brokers=[addr], topic=topic, tail=True, starting_offset="end"),
    )
    # Extract dictionaries from JSON messages
    flow.map(deserialize)
    # Extract (key, value) pairs with the data we want to operate on as the
    # value
    flow.map(lambda x: (x[0], x[1]["value"]))
    # reduce each key according to our reducer function
    flow.reduce_window("sum", clock_config, window_config, lambda x, y: x + y)
    # flow.fold_window("running_average", clock_config, window_config, list, acc_values)
    flow.capture(StdOutputConfig())

    run_main(flow)


if __name__ == "__main__":
    plac.call(main)
