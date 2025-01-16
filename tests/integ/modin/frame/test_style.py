#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import pandas as npd

from tests.integ.utils.sql_counter import sql_count_checker

species = ["adelie"] * 3 + ["chinstrap"] * 3 + ["gentoo"] * 3
measurements = ["bill_length", "flipper_length", "bill_depth"] * 3
values = [37.3, 187.1, 17.7, 46.6, 191.7, 17.6, 45.5, 212.7, 14.2]


@sql_count_checker(query_count=1, join_count=0)
def test_simple_style_accessor():
    ndf = npd.DataFrame(
        {"species": species, "measurement": measurements, "value": values}
    )
    df = pd.DataFrame(
        {"species": species, "measurement": measurements, "value": values}
    )

    assert df.style.to_string() == ndf.style.to_string()
