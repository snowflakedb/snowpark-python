#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import numpy as np
import pandas as native_pd


class Index:
    def __init__(
        self,
        data=None,
        dtype=None,
        copy=False,
        name=None,
        tupleize_cols=True,
        query_compiler=None,
    ) -> None:

        self._index = native_pd.Index(
            data=data,
            dtype=dtype,
            copy=copy,
            name=name,
            tupleize_cols=tupleize_cols,
        )

    def __array__(self, dtype=None) -> np.ndarray:
        """
        The array interface, return my values.
        """
        return self._index.__array__(dtype)
