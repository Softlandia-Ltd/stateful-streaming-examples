"""Calculate the running average of a large dataset."""

import time
import queue
from multiprocessing import Queue

import plac
import numpy as np
from bytewax.dataflow import Dataflow
from bytewax.execution import run_main, spawn_cluster
from bytewax.inputs import ManualInputConfig
from bytewax.outputs import ManualOutputConfig, StdOutputConfig

from softlandia.stateful_streaming.dependencies import generate_data, cumulative_average


def get_input_builder(num_rows: int, num_cols: int, num_ids: int):
    """Generate an input_builder function with custom parameters."""
    print("Building input builder")

    def input_builder(worker_index: int, worker_count: int, resume_state):
        """Build the input stream."""
        state = None
        rows_per_worker = int(num_rows / worker_count)
        # We will yield data with (key, (value, is_complete)) tuples
        print("worker", worker_index, rows_per_worker)
        # This way data is generated once the flow gets the run command. If
        # generating earlier, must get it to correct processes once the flow
        # starts.
        ids, data = generate_data(rows_per_worker, num_cols, num_ids)

        for i, id in enumerate(ids):
            yield state, (id, data[i, :])

    return input_builder


def output_builder(worker_index: int, worker_count: int):
    """Build the output stream."""

    print(f"Building output for worker {worker_index}")
    f = open(f"output_{worker_index}.npy", "wb")

    def update_result(item):
        """Update the output dictionary."""
        # print("output worker", worker_index)
        # with open(f"output_{worker_index}.npy", "wb") as f:
        np.save(f, item[1])

    return update_result


def builder():
    """Build the new initial state."""
    # Current average, numer of items
    return (0, 0)


def mapper(state, item):
    """Map the value to the new state."""
    return (cumulative_average(state[0], item[0], state[1]), state[1] + 1), item


@plac.opt("rows", "Number of rows to generate")
@plac.opt("cols", "Number of columns to generate")
@plac.opt("ids", "Number of unique IDs to generate")
def main(rows: int = 1000, cols: int = 100, ids: int = 5):
    """Run the stream processing."""
    flow = Dataflow()
    input_builder = get_input_builder(rows, cols, ids)
    flow.input("input", ManualInputConfig(input_builder))
    # The stateful_map works very well for this use case, but it will emit the
    # state after each update.
    flow.stateful_map("cumulative_average", builder=builder, mapper=mapper)
    flow.capture(ManualOutputConfig(output_builder))
    # flow.capture(StdOutputConfig())

    start = time.time()
    # run_main(flow)
    spawn_cluster(flow, proc_count=2, worker_count_per_proc=2)

    print("Time taken:", time.time() - start, "seconds")


if __name__ == "__main__":
    plac.call(main)