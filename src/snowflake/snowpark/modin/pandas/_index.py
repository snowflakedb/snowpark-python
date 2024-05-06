#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import pandas as native_pd


class Index:
    def __new__(
        cls,
        data=None,
        dtype=None,
        copy=False,
        name=None,
        tupleize_cols=True,
    ) -> native_pd.Index:
        return native_pd.Index(
            data=data,
            dtype=dtype,
            copy=copy,
            name=name,
            tupleize_cols=tupleize_cols,
        )
