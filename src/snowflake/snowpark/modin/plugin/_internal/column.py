from typing import Union
from snowflake.snowpark.types import TimestampType, DataType
from snowflake.snowpark.functions import dateadd, datediff
from .window import SnowparkPandasWindow
from snowflake.snowpark._internal.type_utils import LiteralType
from typing import Optional



class SnowparkPandasColumn:
    def __init__(self, to_type_and_snowpark_column):
        if not callable(to_type_and_snowpark_column):
            raise ValueError
        self._to_type_and_snowpark_column = to_type_and_snowpark_column

    def to_snowpark_column(self, input_snowpark_pandas_columns_getter):
        return self._to_type_and_snowpark_column(input_snowpark_pandas_columns_getter)[1]

    def datatype(self, input_snowpark_pandas_columns_getter):
        return self._to_type_and_snowpark_column(input_snowpark_pandas_columns_getter)[0]        

    def __sub__(self, other: "SnowparkPandasColumnOrLiteral"):
        from snowflake.snowpark.modin.plugin._internal.type_utils import TimedeltaType        
        if not isinstance(other, SnowparkPandasColumn):
            from .functions import lit            
            other = lit(other)
        def to_type_and_snowpark_column(input_snowpark_pandas_columns_getter):

            other_type, other_column = other._to_type_and_snowpark_column(input_snowpark_pandas_columns_getter)
            self_type, self_column = self._to_type_and_snowpark_column(input_snowpark_pandas_columns_getter)

            # code comes from compute_binary_op_between_snowpark_columns
            if isinstance(self_type, TimestampType) and isinstance(other_type, TimestampType):
                return TimedeltaType(), datediff("ns", other_column, self_column)

            if isinstance(self_type, TimedeltaType) and isinstance(other_type, TimedeltaType):
                return TimedeltaType(), self_column.__sub__(other_column)

            # default behavior:
            return None, self_column.__sub__(other_column)

        return SnowparkPandasColumn(to_type_and_snowpark_column=to_type_and_snowpark_column)

    def __add__(self, other: "SnowparkPandasColumnOrLiteral"): 
        from snowflake.snowpark.modin.plugin._internal.type_utils import TimedeltaType        
        def to_type_and_snowpark_column(input_snowpark_pandas_columns_getter):

            other_type, other_column = other._to_type_and_snowpark_column(input_snowpark_pandas_columns_getter)
            self_type, self_column = self._to_type_and_snowpark_column(input_snowpark_pandas_columns_getter)

            if isinstance(self_type, TimedeltaType) and isinstance(other_type, TimedeltaType):
                return TimedeltaType(), self_column.__add__(other_column)

            if isinstance(self_type, TimestampType) and isinstance(other_type, TimedeltaType):
                return TimestampType(), dateadd("ns", other_column, self_column)

            if isinstance(self_type, TimedeltaType) and isinstance(other_type, TimestampType):
                return TimestampType(), dateadd("ns", self_column, other_column)

            # default behavior:
            return None, self_column.__add__(other_column)

        return SnowparkPandasColumn(to_type_and_snowpark_column=to_type_and_snowpark_column)


    def over(self, *args, **kwargs):
        def to_type_and_snowpark_column(getter):
            my_type, snowpark_column = self._to_type_and_snowpark_column(getter)
            return my_type, snowpark_column.over(*args, **kwargs)     
        return SnowparkPandasColumn(
            to_type_and_snowpark_column         
        )

    def as_(self, *args, **kwargs):
        def to_type_and_snowpark_column(getter):
            my_type, snowpark_column = self._to_type_and_snowpark_column(getter)
            return my_type, snowpark_column.as_(*args, **kwargs)     
        return SnowparkPandasColumn(
            to_type_and_snowpark_column)  
        

    def cast(self, to: Union[str, DataType]) -> "SnowparkPandasColumn":
        """Casts the value of the Column to the specified data type.
        It raises an error when  the conversion can not be performed.
        """
        def to_type_and_snowpark_column(getter):
            return to, self.to_snowpark_column(getter).cast(to)

        return SnowparkPandasColumn(to_type_and_snowpark_column)

    def over(self, window: Optional[SnowparkPandasWindow] = None) -> SnowparkPandasWindow:
        def to_type_and_snowpark_column(getter):
            self_resolved = self.to_snowpark_column(getter)
            self_datatype = self.datatype(getter)
            if window is not None:
                breakpoint()                
                return self_datatype, self_resolved.over(window.to_snowpark_window_spec(getter))
            return self_datatype, self_resolved.over(None)
        return SnowparkPandasColumn(to_type_and_snowpark_column=to_type_and_snowpark_column)
    

SnowparkPandasColumnOrName = Union[SnowparkPandasColumn, str]

SnowparkPandasColumnOrLiteral = Union[SnowparkPandasColumn, LiteralType]