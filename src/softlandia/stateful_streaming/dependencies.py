"""Common dependencies."""

from typing import Tuple

import numpy as np

def generate_data(num_rows: int=int(1e4), num_cols: int=int(1e2), num_ids: int=5) -> Tuple:
    """Generate random data and add IDs to each row.

    The IDs can simulate different data sources, like sensors.
    """
    # Generate a matrix with random values
    matrix = np.random.randn(num_rows, num_cols)

    # Generate random IDs for each row
    ids = np.random.randint(0, num_ids, size=num_rows).astype(str)

    return ids, matrix


def cumulative_average(state, observation, count):
    """Calculate the cumulative average."""
    return (observation + count * state) / (count + 1)
