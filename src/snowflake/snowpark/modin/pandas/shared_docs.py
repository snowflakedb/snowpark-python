#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#
#
# Code in this file may constitute partial or total reimplementation, or modification of
# existing code originally distributed by the pandas project, under the BSD 3-Clause License

from __future__ import annotations

_shared_docs: dict[str, str] = {}

_shared_docs[
    "aggregate"
] = """
Aggregate using one or more operations over the specified axis.

Parameters
----------
func : function, str, list or dict
    Function to use for aggregating the data. If a function, must either
    work when passed a {klass} or when passed to {klass}.apply.

    Accepted combinations are:

    - function
    - string function name
    - list of functions and/or function names, e.g. ``[np.sum, 'mean']``
    - dict of axis labels -> functions, function names or list of such.
{axis}
*args
    Positional arguments to pass to `func`.
**kwargs
    Keyword arguments to pass to `func`.

Returns
-------
scalar, Snowpark pandas :class:`~snowflake.snowpark.modin.pandas.Series` or Snowpark pandas :class:`~snowflake.snowpark.modin.pandas.DataFrame`

    The return can be:

    * scalar : when Snowpark pandas Series.agg is called with single function
    * Snowpark pandas :class:`~snowflake.snowpark.modin.pandas.Series` : when Snowpark pandas DataFrame.agg is called with a single function
    * Snowpark pandas :class:`~snowflake.snowpark.modin.pandas.DataFrame` : when Snowpark pandas DataFrame.agg is called with several functions

    Return scalar, Snowpark pandas :class:`~snowflake.snowpark.modin.pandas.Series` or Snowpark pandas :class:`~snowflake.snowpark.modin.pandas.DataFrame`.

Notes
-----
The aggregation operations are always performed over an axis, either the
index (default) or the column axis. This behavior is different from
`numpy` aggregation functions (`mean`, `median`, `prod`, `sum`, `std`,
`var`), where the default is to compute the aggregation of the flattened
array, e.g., ``numpy.mean(arr_2d)`` as opposed to
``numpy.mean(arr_2d, axis=0)``.

`agg` is an alias for `aggregate`. Use the alias.

Functions that mutate the passed object can produce unexpected
behavior or errors and are not supported.

A passed user-defined-function will be passed a Series for evaluation.
{examples}"""
