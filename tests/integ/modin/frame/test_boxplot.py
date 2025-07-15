#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import matplotlib.pyplot as plt
import pandas as native_pd
import pytest

from matplotlib.testing.compare import compare_images
from tests.integ.utils.sql_counter import sql_count_checker


@sql_count_checker(query_count=1)
@pytest.mark.parametrize("grid", [True, False])
@pytest.mark.parametrize("column", [None, "A", ["A", "B"]])
def test_boxplot(grid, column, tmp_path):
    data = {
        "A": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        "B": [2, 4, 6, 8, 10, 12, 14, 16, 18, 20],
        "C": [1, 3, 5, 7, 9, 11, 13, 15, 17, 19],
    }
    native_df = native_pd.DataFrame(data)
    snow_df = pd.DataFrame(native_df)

    native_fig = plt.figure()
    snow_fig = plt.figure()

    native_fig.add_axes(
        native_df.boxplot(ax=native_fig.gca(), grid=grid, column=column)
    )
    native_file_path = f"{tmp_path}/test_boxplot_native.png"
    native_fig.savefig(native_file_path)

    snow_fig.add_axes(snow_df.boxplot(ax=snow_fig.gca(), grid=grid, column=column))
    snow_file_path = f"{tmp_path}/test_boxplot_snow.png"
    snow_fig.savefig(snow_file_path)

    assert compare_images(native_file_path, snow_file_path, 0) is None
