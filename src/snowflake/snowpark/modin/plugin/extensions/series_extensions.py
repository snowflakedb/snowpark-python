#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

"""
File containing Series APIs defined in Snowpark pandas but not the Modin API layer, such
as `Series.to_snowflake`.
"""

from collections.abc import Iterable
from typing import Any, Literal, Optional, Union

import modin.pandas as pd
import pandas
from modin.pandas.api.extensions import register_series_accessor
from pandas._typing import Axis, IndexLabel

from snowflake.snowpark.dataframe import DataFrame as SnowparkDataFrame
from snowflake.snowpark.modin.plugin.extensions.utils import add_cache_result_docstring
from snowflake.snowpark.modin.plugin.utils.warning_message import (
    materialization_warning,
)


@register_series_accessor("_set_axis_name")
def _set_axis_name(
    self, name: Union[str, Iterable[str]], axis: Axis = 0, inplace: bool = False
) -> Union[pd.Series, None]:
    """
    Alter the name or names of the axis.

    Parameters
    ----------
    name : str or list of str
        Name for the Index, or list of names for the MultiIndex.
    axis : str or int, default: 0
        The axis to set the label.
        0 or 'index' for the index, 1 or 'columns' for the columns.
    inplace : bool, default: False
        Whether to modify `self` directly or return a copy.

    Returns
    -------
    DataFrame or None
    """
    assert axis == 0, f"Expected 'axis=0', got 'axis={axis}'"
    renamed = self if inplace else self.copy()
    renamed.index = renamed.index.set_names(name)
    if not inplace:
        return renamed


@register_series_accessor("to_snowflake")
def to_snowflake(
    self,
    name: Union[str, Iterable[str]],
    if_exists: Optional[Literal["fail", "replace", "append"]] = "fail",
    index: bool = True,
    index_label: Optional[IndexLabel] = None,
    table_type: Literal["", "temp", "temporary", "transient"] = "",
) -> None:
    """
    Save the Snowpark pandas Series as a Snowflake table.

    Args:
        name:
            Name of the SQL table or fully-qualified object identifier
        if_exists:
            How to behave if table already exists. default 'fail'
                - fail: Raise ValueError.
                - replace: Drop the table before inserting new values.
                - append: Insert new values to the existing table. The order of insertion is not guaranteed.
        index: default True
            If true, save Series index columns as table columns.
        index_label:
            Column label for index column(s). If None is given (default) and index is True,
            then the index names are used. A sequence should be given if the DataFrame uses MultiIndex.
        table_type:
            The table type of table to be created. The supported values are: ``temp``, ``temporary``,
            and ``transient``. An empty string means to create a permanent table. Learn more about table
            types `here <https://docs.snowflake.com/en/user-guide/tables-temp-transient.html>`_.

    See Also:
        - :func:`to_snowflake <modin.pandas.to_snowflake>`
        - :func:`DataFrame.to_snowflake <DataFrame.to_snowflake>`
        - :func:`read_snowflake <modin.pandas.read_snowflake>`
    """
    self._query_compiler.to_snowflake(name, if_exists, index, index_label, table_type)


@register_series_accessor("to_snowpark")
def to_snowpark(
    self, index: bool = True, index_label: Optional[IndexLabel] = None
) -> SnowparkDataFrame:
    """
    Convert the Snowpark pandas Series to a Snowpark DataFrame.
    Note that once converted to a Snowpark DataFrame, no ordering information will be preserved. You can call
    reset_index to generate a default index column that is the same as the row position before the call to_snowpark.

    Args:
        index: bool, default True.
            Whether to keep the index columns in the result Snowpark DataFrame. If True, the index columns
            will be the first set of columns. Otherwise, no index column will be included in the final Snowpark
            DataFrame.
        index_label: IndexLabel, default None.
            Column label(s) to use for the index column(s). If None is given (default) and index is True,
            then the original index column labels are used. A sequence should be given if the DataFrame uses
            MultiIndex, and the length of the given sequence should be the same as the number of index columns.

    Returns:
       Snowpark :class:`~snowflake.snowpark.dataframe.DataFrame`
            A Snowpark DataFrame contains the index columns if index=True and all data columns of the Snowpark pandas
            DataFrame. The identifier for the Snowpark DataFrame will be the normalized quoted identifier with
            the same name as the pandas label.

    Raises:
         ValueError if duplicated labels occur among the index and data columns.
         ValueError if the label used for a index or data column is None.

    See also:
        - :func:`to_snowpark <modin.pandas.to_snowpark>`
        - :func:`Series.to_snowpark <modin.pandas.Series.to_snowpark>`

    Note:
        The labels of the Snowpark pandas DataFrame or index_label provided will be used as Normalized Snowflake
        Identifiers of the Snowpark DataFrame.
        For details about Normalized Snowflake Identifiers, please refer to the Note in :func:`~modin.pandas.read_snowflake`

    Examples::

        >>> ser = pd.Series([390., 350., 30., 20.],
        ...                 index=['Falcon', 'Falcon', 'Parrot', 'Parrot'],
        ...                 name="Max Speed")
        >>> ser
        Falcon    390.0
        Falcon    350.0
        Parrot     30.0
        Parrot     20.0
        Name: Max Speed, dtype: float64
        >>> snowpark_df = ser.to_snowpark(index_label="Animal")
        >>> snowpark_df.order_by('"Max Speed"').show()
        --------------------------
        |"Animal"  |"Max Speed"  |
        --------------------------
        |Parrot    |20.0         |
        |Parrot    |30.0         |
        |Falcon    |350.0        |
        |Falcon    |390.0        |
        --------------------------
        <BLANKLINE>
        >>> snowpark_df = ser.to_snowpark(index=False)
        >>> snowpark_df.order_by('"Max Speed"').show()
        ---------------
        |"Max Speed"  |
        ---------------
        |20.0         |
        |30.0         |
        |350.0        |
        |390.0        |
        ---------------
        <BLANKLINE>

        MultiIndex usage
        >>> ser = pd.Series([390., 350., 30., 20.],
        ...                 index=pd.MultiIndex.from_tuples([('bar', 'one'), ('foo', 'one'), ('bar', 'two'), ('foo', 'three')], names=['first', 'second']),
        ...                 name="Max Speed")
        >>> ser
        first  second
        bar    one       390.0
        foo    one       350.0
        bar    two        30.0
        foo    three      20.0
        Name: Max Speed, dtype: float64
        >>> snowpark_df = ser.to_snowpark(index=True, index_label=['A', 'B'])
        >>> snowpark_df.order_by('"A"', '"B"').show()
        -----------------------------
        |"A"  |"B"    |"Max Speed"  |
        -----------------------------
        |bar  |one    |390.0        |
        |bar  |two    |30.0         |
        |foo  |one    |350.0        |
        |foo  |three  |20.0         |
        -----------------------------
        <BLANKLINE>
        >>> snowpark_df = ser.to_snowpark(index=False)
        >>> snowpark_df.order_by('"Max Speed"').show()
        ---------------
        |"Max Speed"  |
        ---------------
        |20.0         |
        |30.0         |
        |350.0        |
        |390.0        |
        ---------------
        <BLANKLINE>
    """
    return self._query_compiler.to_snowpark(index, index_label)


@register_series_accessor("to_pandas")
@materialization_warning
def to_pandas(
    self,
    *,
    statement_params: Optional[dict[str, str]] = None,
    **kwargs: Any,
) -> pandas.Series:
    """
    Convert Snowpark pandas Series to `pandas.Series <https://pandas.pydata.org/docs/reference/api/pandas.Series.html>`_

    Args:
        statement_params: Dictionary of statement level parameters to be set while executing this action.

    See Also:
        - :func:`to_pandas <modin.pandas.to_pandas>`

    Returns:
        pandas Series

    >>> s = pd.Series(['Falcon', 'Falcon',
    ...                 'Parrot', 'Parrot'],
    ...                 name = 'Animal')
    >>> s.to_pandas()
    0    Falcon
    1    Falcon
    2    Parrot
    3    Parrot
    Name: Animal, dtype: object
    """
    return self._to_pandas(statement_params=statement_params, **kwargs)


@register_series_accessor("cache_result")
@add_cache_result_docstring
@materialization_warning
def cache_result(self, inplace: bool = False) -> Optional[pd.Series]:
    """
    Persists the Snowpark pandas Series to a temporary table for the duration of the session.
    """
    new_qc = self._query_compiler.cache_result()
    if inplace:
        self._update_inplace(new_qc)
    else:
        return pd.Series(query_compiler=new_qc)
