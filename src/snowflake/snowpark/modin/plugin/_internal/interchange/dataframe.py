from snowflake.snowpark.modin.plugin.compiler.snowflake_query_compiler import SnowflakeQueryCompiler
from pandas.core.interchange.dataframe_protocol import DataFrame as DataFrameXchg
import abc
import pandas as native_pd
from typing import Sequence
from snowflake.snowpark.modin.plugin.extensions.index import Index
from snowflake.snowpark.modin.plugin._internal.interchange.column import SnowparkPandasColumn

class SnowparkPandasDataFrameXchg(DataFrameXchg):
    def __init__(self, query_compiler: SnowflakeQueryCompiler) -> None:
        self._query_compiler = query_compiler.rename(columns_renamer=str)
        self

    @property
    def metadata(self) -> dict[str, Index]:
        # `index` isn't a regular column, and the protocol doesn't support row
        # labels - so we export it as Pandas-specific metadata here.
        return {"pandas.index": self._query_compiler.index.to_pandas()}
    
    def num_columns(self) -> int:
        return len(self._query_compiler.columns)    
    
    def num_rows(self) -> int:
        return self._query_compiler.get_axis_len(0)

    def num_chunks(self) -> int:
        return 1
    
    def column_names(self) -> native_pd.Index:
        return self._query_compiler.columns
    
    def get_column(self, i: int) -> SnowparkPandasColumn:
        return SnowparkPandasColumn(self._query_compiler.take_2d_positional(index=slice(None), columns=[i]))

    def get_column_by_name(self, name: str) -> SnowparkPandasColumn:
        if len([c for c in self._query_compiler.columns if c == name]) > 2:
            raise NotImplementedError("multiple columns share name")
        return SnowparkPandasColumn(self._query_compiler.take_2d_labels(index=slice(None), columns=[name]))

    def get_columns(self) -> list[SnowparkPandasColumn]:
        return [
            self.get_column(i) for i in range(self.num_columns())
        ]
    
    def select_columns(self, indices: Sequence[int]):

        if not isinstance(indices, abc.Sequence):
            raise ValueError("`indices` is not a sequence")
        if not isinstance(indices, list):
            indices = list(indices)

        return SnowparkPandasDataFrameXchg(
            self._query_compiler.take_2d_positional(slice(None),  indices)
        )
    
    def select_columns_by_name(self, names: list[str]):
        if not isinstance(names, abc.Sequence):
            raise ValueError("`names` is not a sequence")
        if not isinstance(names, list):
            names = list(names)

        return SnowparkPandasDataFrameXchg(self._query_compiler.take_2d_labels(slice(None), names))
    

    def get_chunks(self, n_chunks = None):
        yield self

    def __dataframe__(
        self, nan_as_null: bool = False, allow_copy: bool = True):
        return self