#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import matplotlib.pyplot as plt
import pandas as native_pd
import pytest

from matplotlib.testing.decorators import check_figures_equal
from tests.integ.utils.sql_counter import sql_count_checker

fig_test = plt.figure()
fig_ref = plt.figure()


@check_figures_equal()
@sql_count_checker(query_count=2)
@pytest.mark.parametrize("grid", [True, False])
@pytest.mark.parametrize("bins", [None, 1, 5, 10])
def test_hist(fig_test, fig_ref, grid, bins):
    lst = ["a", "a", "a", "b", "b", "b"]
    native_ser = native_pd.Series([1, 2, 2, 4, 6, 6], index=lst)
    snow_ser = pd.Series(native_ser)
    fig_test.add_axes(
        snow_ser.hist(figure=fig_test, ax=fig_test.gca(), grid=grid, bins=bins)
    )
    fig_ref.add_axes(
        native_ser.hist(figure=fig_ref, ax=fig_ref.gca(), grid=grid, bins=bins)
    )


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
def test_hist_by_neg():
    lst = ["a", "a", "a", "b", "b", "b"]
    native_ser = native_pd.Series([1, 2, 2, 4, 6, 6], index=lst)
    snow_ser = pd.Series(native_ser)
    with pytest.raises(NotImplementedError):
        snow_ser.hist(by=snow_ser % 2)
