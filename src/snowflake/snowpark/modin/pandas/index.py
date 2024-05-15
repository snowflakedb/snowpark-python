#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import numpy as np
import pandas as native_pd

import snowflake.snowpark.modin.plugin  # noqa: F401


# modin index class, wrapper to native pandas for now
class Index:
    """
    Immutable sequence used for indexing and alignment.

    The basic object storing axis labels for all pandas objects.

    .. versionchanged:: 2.0.0

       Index can hold all numpy numeric dtypes (except float16). Previously only
       int64/uint64/float64 dtypes were accepted.

    Parameters
    ----------
    data : array-like (1-dimensional)
    dtype : str, numpy.dtype, or ExtensionDtype, optional
        Data type for the output Index. If not specified, this will be
        inferred from `data`.
        See the :ref:`user guide <basics.dtypes>` for more usages.
    copy : bool, default False
        Copy input data.
    name : object
        Name to be stored in the index.
    tupleize_cols : bool (default: True)
        When True, attempt to create a MultiIndex if possible.

    Notes
    -----
    An Index instance can **only** contain hashable objects.
    An Index instance *can not* hold numpy float16 dtype.

    Examples
    --------
    >>> pd.Index([1, 2, 3])
    Index([1, 2, 3], dtype='int64')

    >>> pd.Index(list('abc'))
    Index(['a', 'b', 'c'], dtype='object')

    >>> pd.Index([1, 2, 3], dtype="uint8")
    Index([1, 2, 3], dtype='uint8')
    """

    # same fields as native pandas index constructor
    def __init__(
        self,
        data=None,
        dtype=None,
        copy=False,
        name=None,
        tupleize_cols=True,
        query_compiler=None,
    ) -> None:
        self._index = None

        # TODO: SNOW-1359041: Switch to lazy index implementation
        if isinstance(data, (pd.DataFrame, pd.Series)):
            qc = data._query_compiler
        elif isinstance(data, pd.Index):
            self = data
            return
        elif query_compiler is not None:
            qc = query_compiler
        else:
            index_as_native_df = native_pd.Index(
                data=data,
                dtype=dtype,
                copy=copy,
                name=name,
                tupleize_cols=tupleize_cols,
            ).to_frame()
            from snowflake.snowpark.modin.plugin.compiler.snowflake_query_compiler import (
                SnowflakeQueryCompiler,
            )

            qc = SnowflakeQueryCompiler.from_pandas(index_as_native_df)

        self._query_compiler = qc if qc else None

    @property
    def _column_names(self):
        return self._query_compiler.get_index_names()

    @property
    def _ordered_dataframe(self):
        return self._query_compiler._modin_frame.ordered_dataframe

    @property
    def _snowflake_quoted_identifiers(self):
        return (
            self._query_compiler._modin_frame.index_column_snowflake_quoted_identifiers
        )

    @property
    def is_multi_index(self, axis=0):
        return self._query_compiler.is_multiindex(axis=axis)

    @property
    def values(self):
        from snowflake.snowpark.modin.plugin._internal.utils import (
            snowpark_to_pandas_helper,
        )

        values = snowpark_to_pandas_helper(
            self._ordered_dataframe.select(self._snowflake_quoted_identifiers)
        ).values

        if self.is_multi_index:
            tuples = [tuple(v) for v in values]
            values = np.empty(len(tuples), dtype=object)
            values[:] = tuples
        else:
            values = values.flatten()
        return values

        return snowpark_to_pandas_helper(
            self._ordered_dataframe.select(self._snowflake_quoted_identifiers)
        ).values.flatten()

    @property
    def names(self):
        return self._column_names

    @property
    def name(self):
        return self.names[0]

    def to_pandas(self):
        return self._query_compiler.index

    def __array__(self, dtype=None) -> np.ndarray:
        """
        The array interface, return my values.
        """
        return self.to_pandas().__array__(dtype=dtype)

    def __repr__(self):
        return self.to_pandas().__repr__()

    def __iter__(self):
        # TODO: SNOW-1359041: Do we need to update this in the next PR to not call to_pandas()
        return self.to_pandas().__iter__()
