"""Calculate the running average of a large dataset."""

import time

import plac
import numpy as np
from bytewax.dataflow import Dataflow
from bytewax.execution import run_main
from bytewax.inputs import ManualInputConfig
from bytewax.outputs import ManualOutputConfig

from softlandia.stateful_streaming.dependencies import generate_data, cumulative_average


def get_input_builder(num_rows: int, num_cols: int, num_ids: int):
    """Generate an input_builder function with custom parameters."""

    def input_builder(worker_index: int, worker_count: int, resume_state):
        """Build the input stream."""
        state = None
        ids, data = generate_data(num_rows, num_cols, num_ids)
        # We will yield data with (key, (value, is_complete)) tuples
        for i, id in enumerate(ids):
            yield state, (id, (data[i, :]))

    return input_builder


def get_output_builder(output_dict: dict):
    """Generate an output builder with custom parameters.

    This way we can pass in a dictionary that will always store the latest
    output.
    """

    def output_builder(worker_index: int, worker_count: int):
        """Build the output stream."""

        def update_dict(item):
            """Update the output dictionary."""
            output_dict[item[0]] = item[1]

        return update_dict

    return output_builder


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
def main(rows: int=1000, cols: int=100, ids: int=5):
    """Run the stream processing."""
    result_dict = {}
    flow = Dataflow()
    flow.input("input", ManualInputConfig(get_input_builder(rows, cols, ids)))
    flow.stateful_map("cumulative_average", builder=builder, mapper=mapper)
    # Capturing is quite slow, since stateful map emits after each update
    flow.capture(ManualOutputConfig(get_output_builder(result_dict)))

    start = time.time()
    run_main(flow)

    for key, value in result_dict.items():
        print(key, value)
    print("Time taken:", time.time() - start, "seconds")

if __name__ == "__main__":
    plac.call(main)
