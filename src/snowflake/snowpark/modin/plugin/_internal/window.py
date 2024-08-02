from typing import Union, Iterable
from snowflake.snowpark.window import WindowSpec, Window
from snowflake.snowpark._internal.utils import parse_positional_args_to_list


class SnowparkPandasWindow:
    @staticmethod
    def order_by(
        *cols: Union[
            "SnowparkPandasColumnOrName",
            Iterable["SnowparkPandasColumnOrName"],
        ]
    ) -> "WindowSpec":
        snowpark_pandas_exprs = parse_positional_args_to_list(cols)
        def to_snowpark_window_spec(getter):
            return Window.order_by(e.to_snowpark_column(getter) for e in snowpark_pandas_exprs)
        return SnowparkPandasWindowSpec(to_snowpark_window_spec)
    
class SnowparkPandasWindowSpec:
    def __init__(self, to_snowpark_window_spec):
        self._to_snowpark_window_spec = to_snowpark_window_spec
    def to_snowpark_window_spec(self, getter):
        return self._to_snowpark_window_spec(getter)