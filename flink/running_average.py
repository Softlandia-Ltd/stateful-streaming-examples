"""Example of stateful streaming with Flink."""

import time
from typing import Tuple

import plac
from pyflink.common import Time, WatermarkStrategy, SimpleStringSchema, Duration
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.datastream import AggregateFunction
from pyflink.datastream.window import (
    TumblingProcessingTimeWindows,
    ProcessingTimeSessionWindows,
    CountTrigger,
)
from pyflink.common.watermark_strategy import TimestampAssigner

from softlandia.stateful_streaming.dependencies import generate_data, cumulative_average


class AverageAggregate(AggregateFunction):

    def create_accumulator(self) -> Tuple[int, int]:
        """Create an accumulator for the average.

        Initially value is 0 and count is 0.
        """
        return 0, 0

    def add(self, value: Tuple[str, int], accumulator: Tuple[int, int]) -> Tuple[int, int]:
        """Add new value.

        Note that value has key as first element.
        """
        print("Old accumulator is ", accumulator)
        print("value is ", value)
        accumulator = cumulative_average(accumulator[0], value[1], accumulator[1]), accumulator[1] + 1
        print("New accumulator is ", accumulator)
        return accumulator

    def get_result(self, accumulator: Tuple[int, int]) -> float:
        return accumulator

    def merge(self, a: Tuple[int, int], b: Tuple[int, int]) -> Tuple[int, int]:
        raise NotImplementedError("Merge not allowed!")
        # return a


@plac.opt("rows", "Number of rows to generate")
@plac.opt("cols", "Number of columns to generate")
@plac.opt("ids", "Number of unique IDs to generate")
def main(rows: int=1000, cols: int=100, ids: int=5):
    """Analyze the Kafka stream."""
    ids, data = generate_data(rows, cols, ids)

    env = StreamExecutionEnvironment.get_execution_environment()
    (
        # Ingest from our collection. Docs say this will not support
        # parallelism.
        env.from_collection(list(zip(ids, data)))
        .key_by(lambda x: x[0])
        .window(ProcessingTimeSessionWindows.with_gap(Time.milliseconds(500)))
        .aggregate(AverageAggregate())
        .print()
    )

    # submit for execution
    start = time.time()
    env.execute()
    print(f"Time taken: {time.time() - start} seconds")
    time.sleep(2)

if __name__ == "__main__":
    plac.call(main)
