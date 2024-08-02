from .column import SnowparkPandasColumnOrName, SnowparkPandasColumn
from snowflake.snowpark.types import LongType, DataType
from snowflake.snowpark.functions import count as count_, min as min_, max as max_, cast as cast_, row_number as row_number_
from snowflake.snowpark.column import Column
from typing import Union


def snowpark_pandas_col(name):
    def to_type_and_snowpark_column(input_snowpark_pandas_columns_getter):
        snowpark_label_to_type = input_snowpark_pandas_columns_getter()
        # special case for "*"
        if name == "*":
            return None, Column(name)
        return snowpark_label_to_type[name], Column(name)
    return SnowparkPandasColumn(to_type_and_snowpark_column=to_type_and_snowpark_column)


def _to_col_if_str(col: SnowparkPandasColumnOrName, func_name: str) -> SnowparkPandasColumn:
    if isinstance(col, SnowparkPandasColumn):
        return col
    elif isinstance(col, str):
        return snowpark_pandas_col(col)
    else:
        raise TypeError(
            f"'{func_name.upper()}' expected Column or str, got: {type(col)}"
        )


def count(e: SnowparkPandasColumnOrName):
    c = _to_col_if_str(e, "count")
    return SnowparkPandasColumn(
        to_type_and_snowpark_column=(lambda getter: (LongType(), count_(c.to_snowpark_column(getter))))
    )

def min(e: SnowparkPandasColumnOrName):
    c = _to_col_if_str(e, "min")
    def to_type_and_snowpark_column(input_snowpark_pandas_columns_getter):
        snowpark_type = c.datatype(input_snowpark_pandas_columns_getter)
        snowpark_column = c.to_snowpark_column(input_snowpark_pandas_columns_getter)
        return snowpark_type, min_(snowpark_column)
    return SnowparkPandasColumn(
        to_type_and_snowpark_column=to_type_and_snowpark_column
    )

def max(e: SnowparkPandasColumnOrName):
    c = _to_col_if_str(e, "max")
    def to_type_and_snowpark_column(input_snowpark_pandas_columns_getter):
        snowpark_type = c.datatype(input_snowpark_pandas_columns_getter)
        snowpark_column = c.to_snowpark_column(input_snowpark_pandas_columns_getter)
        return snowpark_type, max_(snowpark_column)
    return SnowparkPandasColumn(
        to_type_and_snowpark_column=to_type_and_snowpark_column
    )

def cast(column: SnowparkPandasColumnOrName, to: Union[str, DataType]) -> SnowparkPandasColumnOrName:
    c = _to_col_if_str(column, "cast")
    def to_type_and_snowpark_column(input_snowpark_pandas_columns_getter):
        return to, cast_(c.to_snowpark_column(input_snowpark_pandas_columns_getter))
    return SnowparkPandasColumn(to_type_and_snowpark_column)

def row_number() -> SnowparkPandasColumn:
    def to_type_and_snowpark_column(input_snowpark_pandas_columns_getter):
        return LongType(), row_number_()
    return SnowparkPandasColumn(to_type_and_snowpark_column)

def lit(v):
    from snowflake.snowpark.modin.plugin._internal.utils import pandas_lit    
    return pandas_lit(v)