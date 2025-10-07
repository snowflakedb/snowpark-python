#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import io

import modin.pandas as pd
import pandas as npd
import pytest

from tests.integ.utils.sql_counter import sql_count_checker
from tests.utils import IS_WINDOWS

species = ["adelie"] * 3 + ["chinstrap"] * 3 + ["gentoo"] * 3
measurements = ["bill_length", "flipper_length", "bill_depth"] * 3
values = [37.3, 187.1, 17.7, 46.6, 191.7, 17.6, 45.5, 212.7, 14.2]


@pytest.mark.skipif(
    IS_WINDOWS,
    reason="A usable init.tcl cannot be found on windows",
)
@sql_count_checker(query_count=1, join_count=0)
def test_simple_plot_method():
    ndf = npd.DataFrame(
        {"species": species, "measurement": measurements, "value": values}
    )
    df = pd.DataFrame(
        {"species": species, "measurement": measurements, "value": values}
    )
    df_plot_buf = io.BytesIO()
    ndf_plot_buf = io.BytesIO()

    # plots the values column and method call
    df.plot(color="C2").get_figure().savefig(df_plot_buf)
    ndf.plot(color="C2").get_figure().savefig(ndf_plot_buf)

    assert df_plot_buf.getvalue() == ndf_plot_buf.getvalue()


@sql_count_checker(query_count=1, join_count=0)
def test_simple_plot_accessor():
    ndf = npd.DataFrame(
        {"species": species, "measurement": measurements, "value": values}
    )
    df = pd.DataFrame(
        {"species": species, "measurement": measurements, "value": values}
    )
    df_plot_buf = io.BytesIO()
    ndf_plot_buf = io.BytesIO()

    # plot using the property accessor
    ndf.plot.area().get_figure().savefig(ndf_plot_buf)
    df.plot.area().get_figure().savefig(df_plot_buf)

    assert df_plot_buf.getvalue() == ndf_plot_buf.getvalue()


@sql_count_checker(query_count=2, join_count=0)
def test_simple_plot_accessor_series():
    ser = pd.Series([1, 2, 3, 3])
    # plot using the property accessor
    # subsequent plots from a series cannot be compared
    # within the same process, so unlike the DataFrame
    # tests above we do not verify that the plots are
    # the same. Performing plots with series/matplotlib
    # appears to use parts of the subplot logic which
    # we do not want to test.
    #
    # We *do* want to verify that the property accessors
    # work correctly through modin however.
    ser.plot(kind="hist", title="My plot")
    ser.plot.area()
