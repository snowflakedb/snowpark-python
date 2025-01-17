#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import pandas as native_pd
import numpy as np
import pytest

from tests.integ.modin.utils import assert_frame_equal
from tests.integ.utils.sql_counter import sql_count_checker


@sql_count_checker(query_count=1)
def test_from_records_structured_ndarray():
    data = np.array(
        [(3, "a"), (2, "b"), (1, "c"), (0, "d")],
        dtype=[("col_1", "i4"), ("col_2", "U1")],
    )
    assert_frame_equal(
        pd.DataFrame.from_records(data),
        native_pd.DataFrame.from_records(data),
        check_dtype=False,
    )


@sql_count_checker(query_count=1)
def test_from_records_list_of_dicts():
    data = [
        {"col_1": 3, "col_2": "a"},
        {"col_1": 2, "col_2": "b"},
        {"col_1": 1, "col_2": "c"},
        {"col_1": 0, "col_2": "d"},
    ]

    assert_frame_equal(
        pd.DataFrame.from_records(data),
        native_pd.DataFrame.from_records(data),
        check_dtype=False,
    )


@sql_count_checker(query_count=1)
def test_from_records_list_of_records():
    data = [(3, "a"), (2, "b"), (1, "c"), (0, "d")]

    assert_frame_equal(
        pd.DataFrame.from_records(data),
        native_pd.DataFrame.from_records(data),
        check_dtype=False,
    )


@sql_count_checker(query_count=0)
def test_from_records_neg():
    data = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]})

    with pytest.raises(
        NotImplementedError,
        match="Snowpark pandas 'DataFrame.from_records' method does not yet support 'data' parameter of type 'DataFrame'",
    ):
        pd.DataFrame.from_records(data),
