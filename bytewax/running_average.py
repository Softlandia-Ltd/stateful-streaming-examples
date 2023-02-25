"""Calculate the running average of a large dataset."""

import time

import plac
import numpy as np
from bytewax.dataflow import Dataflow
from bytewax.execution import run_main, spawn_cluster
from bytewax.inputs import ManualInputConfig
from bytewax.outputs import StdOutputConfig

from softlandia.stateful_streaming.dependencies import generate_data, cumulative_average


def get_input_builder(num_rows: int, num_cols: int, num_ids: int):
    """Generate an input_builder function with custom parameters."""
    ids, data = generate_data(num_rows, num_cols, num_ids)

    def input_builder(worker_index: int, worker_count: int, resume_state):
        """Build the input stream."""
        state = None
        # We will yield data with (key, (value, is_complete)) tuples
        for i, id in enumerate(ids):
            yield state, (id, (data[i, :], True))
        # yield stop signals. With this method, we don't have to keep track of
        # the item count
        for uniques in set(ids):
            yield state, (uniques, (None, False))

    return input_builder


def is_complete(state):
    """Check if the stream is complete.

    The state passed here will be the output from reducer."""
    # Check the last element of the state tuple, which contains the stop signal
    if not state[-1]:
        return True
    return False


def reducer(session, array):
    """Calculate the running average.

    We must keep track of the number of items for each item, as well as the
    current running average.

    In session, we have (running_average, count, stop_signal).
    In array we have (value, 1, stop_signal)
    """
    # Check the last element of the state tuple, which contains the stop signal
    if array[-1]:
        session = (cumulative_average(session[0], array[0], session[1]), session[1] + 1, True)
    else:
        # Stop sign received, output the current state, count and stop signal
        session = (session[0], session[1], False)
    return session


@plac.opt("rows", "Number of rows to generate")
@plac.opt("cols", "Number of columns to generate")
@plac.opt("ids", "Number of unique IDs to generate")
def main(rows: int=1000, cols: int=100, ids: int=5):
    """Run the stream processing."""
    flow = Dataflow()
    flow.input("input", ManualInputConfig(get_input_builder(rows, cols, ids)))
    # Keep the running average of each key. This map is used because Bytewax
    # will use the first seen value as an initial value to the accumulator in
    # the reduce-step. So the data looks like
    # (id, (array, count, is_complete))
    flow.map(lambda x: (x[0], (x[1][0], 1, x[1][1])))
    flow.reduce("sum", reducer, is_complete)
    flow.capture(StdOutputConfig())

    start = time.time()
    run_main(flow)
    print("Time taken: ", time.time() - start, "seconds")

if __name__ == "__main__":
    plac.call(main)
