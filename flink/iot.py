"""Example of stateful streaming with Flink."""

import json
from datetime import datetime
import time

import plac
from pyflink.common import Time, WatermarkStrategy, SimpleStringSchema, Duration
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.datastream.window import (
    TumblingProcessingTimeWindows,
    TumblingEventTimeWindows,
)
from pyflink.common.watermark_strategy import TimestampAssigner


class SensorTimestampAssigner(TimestampAssigner):
    """Custom event-time extraction."""

    def extract_timestamp(self, value, record_timestamp) -> int:
        """Get event time in milliseconds"""
        return int(value["time"]*1e3)


@plac.opt("addr", "Broker address")
@plac.opt("topic", "Kafka topic")
@plac.opt("win", "Window length in seconds")
def main(addr: str = "127.0.0.1:9092", topic: str = "iot", win: int = 10):
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
        env.from_source(
            source,
            WatermarkStrategy.no_watermarks(),
            "Kafka Source",
        )
        # Read the json-strings into dictionaries
        .map(json.loads)
        # Assign watermarks here, since we can access the event time from our
        # json-data. Watermarks are needed for event-time windowing below.
        .assign_timestamps_and_watermarks(
            WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(1))
            # Don't wait too long for idle data sources
            .with_idleness(Duration.of_millis(500))
            .with_timestamp_assigner(SensorTimestampAssigner())
        )
        # Map each dictionary into (key, value) tuples
        .map(lambda x: (x["id"], x["value"]))
        # Group by unique keys
        .key_by(lambda x: x[0])
        # ...and collect each key into their Tumbling windows
        .window(TumblingEventTimeWindows.of(Time.seconds(win)))
        # ...which are reduced to the sum of value-fields inside each window
        .reduce(lambda x, y: (x[0], x[1] + y[1])).print()
    )

    # submit for execution
    env.execute()


if __name__ == "__main__":
    plac.call(main)
