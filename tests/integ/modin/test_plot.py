#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import io

import modin.pandas as pd
import pandas as npd


def test_simple_plot():
    species = ["adelie"] * 3 + ["chinstrap"] * 3 + ["gentoo"] * 3
    measurements = ["bill_length", "flipper_length", "bill_depth"] * 3
    values = [37.3, 187.1, 17.7, 46.6, 191.7, 17.6, 45.5, 212.7, 14.2]
    ndf = npd.DataFrame(
        {"species": species, "measurement": measurements, "value": values}
    )
    df = pd.DataFrame(
        {"species": species, "measurement": measurements, "value": values}
    )
    df_plot_buf = io.BytesIO()
    ndf_plot_buf = io.BytesIO()

    # plots the values column
    df.plot().get_figure().savefig(df_plot_buf)
    ndf.plot().get_figure().savefig(ndf_plot_buf)

    assert df_plot_buf.getvalue() == ndf_plot_buf.getvalue()
