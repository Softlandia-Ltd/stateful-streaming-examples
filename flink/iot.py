"""Example of stateful streaming with Flink."""
import json

import plac
from pyflink.common import Time, WatermarkStrategy, SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.datastream.window import TumblingProcessingTimeWindows


@plac.pos("addr", "Broker address")
@plac.pos("topic", "Kafka topic")
@plac.opt("win", "Window length in seconds")
def main(addr: str, topic: str, win: int = 10):
    """Analyze the Kafka stream."""
    env = StreamExecutionEnvironment.get_execution_environment()

    # Kafka source will run in unbounded mode by default
    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(addr)
        .set_topics(topic)
        .set_group_id("iot-flink")
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema(charset="utf-8"))
        .build()
    )

    (
        # Ingest from our Kafka source
        env.from_source(source, WatermarkStrategy.no_watermarks(), "Kafka Source")
        # Read the json-strings into dictionaries
        .map(json.loads)
        # Map each dictionary into (key, value) tuples
        .map(lambda x: (x["id"], x["value"]))
        # Group by unique keys
        .key_by(lambda x: x[0])
        # ...and collect each key into their Tumbling windows
        .window(TumblingProcessingTimeWindows.of(Time.seconds(win)))
        # ...which are reduced to the sum of value-fields inside each window
        .reduce(lambda x, y: (x[0], x[1] + y[1]))
        .print()
    )

    # submit for execution
    env.execute()


if __name__ == "__main__":
    plac.call(main)
