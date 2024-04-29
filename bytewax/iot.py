"""Example of stateful stream processing with Bytewax."""

from datetime import timedelta, datetime, timezone
import orjson as json

import plac

from bytewax.dataflow import Dataflow
import bytewax.operators as op
from bytewax.operators.window import reduce_window, EventClockConfig, TumblingWindow
from bytewax.connectors.kafka import KafkaSourceMessage
from bytewax.connectors.kafka import operators as kop
from bytewax.connectors.stdio import StdOutSink

from bytewax.testing import run_main


def deserialize(msg: KafkaSourceMessage) -> tuple[str, dict]:
    """Deserialize Kafka messages.

    Will return tuples of (id, msg).
    """
    payload = json.loads(msg.value.decode("utf-8"))
    return payload


def get_event_time(event):
    """Extract event-time from data."""
    # Remember timezone info!
    return datetime.fromtimestamp(event["time"], timezone.utc)

@plac.opt("addr", "Broker address")
@plac.opt("topic", "Kafka topic")
@plac.opt("win", "Window length in seconds")
def main(addr: str = "127.0.0.1:9092", topic: str = "iot", win: int = 10):
    """Run the stream processing flow."""
    # We want to do windowing based on event times!
    clock_config = EventClockConfig(get_event_time, timedelta(milliseconds=500))

    align_to = datetime(2023, 1, 1, tzinfo=timezone.utc)
    # We'll operate in 10 second windows
    window_config = TumblingWindow(align_to=align_to, length=timedelta(seconds=win))

    # Initialize a flow
    flow = Dataflow("iot")
    # Input is our Kafka stream
    kinp = kop.input(
        "input", flow, brokers=[addr], topics=[topic], tail=False)
    errs = op.inspect("errors", kinp.errs).then(op.raises, "crash-on-err")

    # Extract dictionaries from JSON messages
    stream = op.map("serde", kinp.oks, deserialize)
    # Extract (key, value) pairs with the data we want to operate on as the
    # value
    keyed_stream = op.key_on("key", stream, lambda x: str(x['id']))

    # reduce each key according to our reducer function, bytewax will pass only
    # the values to the reduce function. Since we output dicts from the
    # previous map, we need to output dicts from the reducer
    windowed_stream = reduce_window(
            "sum",keyed_stream, clock_config, window_config, lambda x, y: {"value": x["value"] + y["value"]}
    )

    op.output("print", windowed_stream, StdOutSink())

    run_main(flow)



if __name__ == "__main__":
    plac.call(main)
