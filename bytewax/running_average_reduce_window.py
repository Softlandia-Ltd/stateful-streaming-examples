"""Calculate the running average of a large dataset."""

import time
from datetime import timedelta

import plac
import numpy as np
from bytewax.dataflow import Dataflow
from bytewax.execution import run_main, spawn_cluster
from bytewax.inputs import ManualInputConfig
from bytewax.window import TumblingWindowConfig, SystemClockConfig
from bytewax.outputs import ManualOutputConfig, StdOutputConfig

from softlandia.stateful_streaming.dependencies import generate_data, cumulative_average


def get_input_builder(num_rows: int, num_cols: int, num_ids: int):
    """Generate an input_builder function with custom parameters."""

    def input_builder(worker_index: int, worker_count: int, resume_state):
        """Build the input stream."""
        state = None
        rows_per_worker = int(num_rows / worker_count)
        # We will yield data with (key, (value, is_complete)) tuples
        print("worker", worker_index, rows_per_worker)
        ids, data = generate_data(rows_per_worker, num_cols, num_ids)

        for i, id in enumerate(ids):
            yield state, (id, (data[i, :], 1))

    return input_builder


def reducer(state, array):
    """Map the value to the new state."""
    new_count = state[1] + 1
    return (cumulative_average(state[0], array[0], state[1]), new_count)


@plac.opt("rows", "Number of rows to generate")
@plac.opt("cols", "Number of columns to generate")
@plac.opt("ids", "Number of unique IDs to generate")
def main(rows: int = 1000, cols: int = 100, ids: int = 5):
    """Run the stream processing."""
    # We can use a time-based window to collect all events and end the flow
    # when there are no more events. It's a "trick" so that we don't have to
    # keep track of when the items are done streaming. Just need to use a very
    # large time window so that all data fits in.
    clock_config = SystemClockConfig()
    window_config = TumblingWindowConfig(length=timedelta(hours=2))
    flow = Dataflow()
    input_builder = get_input_builder(rows, cols, ids)
    flow.input("input", ManualInputConfig(input_builder))
    flow.reduce_window("reduce", clock_config, window_config, reducer)
    flow.capture(StdOutputConfig())

    start = time.time()
    # run_main(flow)
    spawn_cluster(flow, proc_count=2, worker_count_per_proc=2)

    print("Time taken:", time.time() - start, "seconds")


if __name__ == "__main__":
    plac.call(main)
