#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import pandas as native_pd


# modin index class, wrapper to native pandas for now
class Index:

    # same fields as native pandas index constructor
    def __new__(
        cls,
        data=None,
        dtype=None,
        copy=False,
        name=None,
        tupleize_cols=True,
    ) -> native_pd.Index:

        # TODO: SNOW-1359041: Switch to lazy index implementation
        return native_pd.Index(
            data=data,
            dtype=dtype,
            copy=copy,
            name=name,
            tupleize_cols=tupleize_cols,
        )
