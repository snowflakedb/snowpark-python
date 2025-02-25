#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import matplotlib.pyplot as plt
import pandas as native_pd
import pytest

from matplotlib.testing.compare import compare_images
from tests.integ.utils.sql_counter import sql_count_checker


@sql_count_checker(query_count=2)
@pytest.mark.parametrize("grid", [True, False])
@pytest.mark.parametrize("bins", [None, 1, 5, 10])
def test_hist(grid, bins, tmp_path):
    lst = ["a", "a", "a", "b", "b", "b"]
    native_ser = native_pd.Series([1, 2, 2, 4, 6, 6], index=lst)
    snow_ser = pd.Series(native_ser)

    native_fig = plt.figure()
    snow_fig = plt.figure()

    native_fig.add_axes(
        native_ser.hist(figure=native_fig, ax=native_fig.gca(), grid=grid, bins=bins)
    )
    native_file_path = f"{tmp_path}/test_hist_native.png"
    native_fig.savefig(native_file_path)

    snow_fig.add_axes(
        snow_ser.hist(figure=snow_fig, ax=snow_fig.gca(), grid=grid, bins=bins)
    )
    snow_file_path = f"{tmp_path}/test_hist_snow.png"
    snow_fig.savefig(snow_file_path)

    assert compare_images(native_file_path, snow_file_path, 0) is None


@pytest.mark.parametrize(
    "xlabelsize, xrot, ylabelsize, yrot, figsize, bins, backend, legend, error",
    [
        [1, None, None, None, None, None, None, False, NotImplementedError],
        [None, 1.0, None, None, None, None, None, False, NotImplementedError],
        [None, None, 1, None, None, None, None, False, NotImplementedError],
        [None, None, None, 1.0, None, None, None, False, NotImplementedError],
        [None, None, None, None, [1.0, 1.0], None, None, False, NotImplementedError],
        [None, None, None, None, None, 0, None, False, ValueError],
        [None, None, None, None, None, -1, None, False, ValueError],
        [None, None, None, None, None, [1, 2], None, False, NotImplementedError],
        [None, None, None, None, None, None, "matplotlib", False, NotImplementedError],
        [None, None, None, None, None, None, None, True, NotImplementedError],
    ],
)
@sql_count_checker(query_count=0)
def test_hist_params_neg(
    xlabelsize, xrot, ylabelsize, yrot, figsize, bins, backend, legend, error
):
    lst = ["a", "a", "a", "b", "b", "b"]
    native_ser = native_pd.Series([1, 2, 2, 4, 6, 6], index=lst)
    snow_ser = pd.Series(native_ser)
    with pytest.raises(error):
        snow_ser.hist(
            xlabelsize=xlabelsize,
            xrot=xrot,
            ylabelsize=ylabelsize,
            yrot=yrot,
            figsize=figsize,
            bins=bins,
            backend=backend,
            legend=legend,
        )


@sql_count_checker(query_count=0)
def test_hist_by_unsupported():
    # The 'by' parameter is not supported yet.
    lst = ["a", "a", "a", "b", "b", "b"]
    native_ser = native_pd.Series([1, 2, 2, 4, 6, 6], index=lst)
    snow_ser = pd.Series(native_ser)
    with pytest.raises(NotImplementedError):
        snow_ser.hist(by=snow_ser % 2)
