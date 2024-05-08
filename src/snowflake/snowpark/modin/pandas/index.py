#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import pandas as native_pd


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
    pd.Index([1, 2, 3])
    Index([1, 2, 3], dtype='int64')

    pd.Index(list('abc'))
    Index(['a', 'b', 'c'], dtype='object')

    pd.Index([1, 2, 3], dtype="uint8")
    Index([1, 2, 3], dtype='uint8')
    """

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
