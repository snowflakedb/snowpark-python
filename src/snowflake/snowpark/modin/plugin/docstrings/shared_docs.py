#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

#
# Code in this file may constitute partial or total reimplementation, or modification of
# existing code originally distributed by the pandas project, under the BSD 3-Clause License

from __future__ import annotations

from pandas.util._decorators import doc

from snowflake.snowpark.modin.plugin.utils.error_message import ErrorMessage

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
scalar, Snowpark pandas :class:`~modin.pandas.Series` or Snowpark pandas :class:`~modin.pandas.DataFrame`

    The return can be:

    * scalar : when Snowpark pandas Series.agg is called with single function
    * Snowpark pandas :class:`~modin.pandas.Series` : when Snowpark pandas DataFrame.agg is called with a single function
    * Snowpark pandas :class:`~modin.pandas.DataFrame` : when Snowpark pandas DataFrame.agg is called with several functions

    Return scalar, Snowpark pandas :class:`~modin.pandas.Series` or Snowpark pandas :class:`~modin.pandas.DataFrame`.

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


_doc_binary_operation = """
Return {operation} of {left} and `{right}` (binary operator `{bin_op}`).

Parameters
----------
{right} : {right_type}
    The second operand to perform computation.

Returns
-------
{returns}
"""


def _doc_binary_op(operation, bin_op, left="Series", right="right", returns="Series"):
    """
    Return callable documenting `Series` or `DataFrame` binary operator.

    Parameters
    ----------
    operation : str
        Operation name.
    bin_op : str
        Binary operation name.
    left : str, default: 'Series'
        The left object to document.
    right : str, default: 'right'
        The right operand name.
    returns : str, default: 'Series'
        Type of returns.

    Returns
    -------
    callable
    """
    if left == "Series":
        right_type = "Series or scalar value"
    elif left == "DataFrame":
        right_type = "DataFrame, Series or scalar value"
    elif left == "BasePandasDataset":
        right_type = "BasePandasDataset or scalar value"
    else:
        ErrorMessage.not_implemented(
            f"Only 'BasePandasDataset', `DataFrame` and 'Series' `left` are allowed, actually passed: {left}"
        )  # pragma: no cover
    doc_op = doc(
        _doc_binary_operation,
        operation=operation,
        right=right,
        right_type=right_type,
        bin_op=bin_op,
        returns=returns,
        left=left,
    )

    return doc_op
