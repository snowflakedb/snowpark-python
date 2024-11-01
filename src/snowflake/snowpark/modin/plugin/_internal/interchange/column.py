from snowflake.snowpark.modin.plugin.compiler.snowflake_query_compiler import SnowflakeQueryCompiler
from pandas.core.interchange.dataframe_protocol import (
    Column,
    ColumnBuffers,
    ColumnNullType,
    DtypeKind,
)
from pandas.core.dtypes.dtypes import BaseMaskedDtype

from pandas import (
    ArrowDtype,
    DatetimeTZDtype,
)
from pandas.core.interchange.column import _NP_KINDS, _NULL_DESCRIPTION
from pandas.core.interchange.utils import (
    ArrowCTypes,
    Endianness,
    dtype_to_arrow_c_fmt,
)
import modin.pandas as pd
from pandas.api.types import is_string_dtype, infer_dtype

class SnowparkPandasColumn(Column):
    def __init__(self, column: SnowflakeQueryCompiler):
        assert len(column.columns) == 1
        self._query_compiler = column

    def size(self) -> int:
        return self._query_compiler.get_axis_len(0)
    
    @property
    def offset(self) -> int:
        """
        Offset of first element. Always zero.
        """
        return 0

    @property
    def dtype(self) -> tuple[DtypeKind, int, str, str]:
        pandas_dtype = self._query_compiler.dtypes.iloc[0]

        if is_string_dtype(pandas_dtype):
            raise NotImplementedError("avoiding materialization")
            # if infer_dtype(self._query_compiler) in ("string", "empty"):
            #     return (
            #         DtypeKind.STRING,
            #         8,
            #         dtype_to_arrow_c_fmt(pandas_dtype),
            #         Endianness.NATIVE,
            #     )
            # raise NotImplementedError("Non-string object dtypes are not supported yet")

        else:
            return self._dtype_from_pandasdtype(pandas_dtype)        
        
    def _dtype_from_pandasdtype(self, dtype) -> tuple[DtypeKind, int, str, str]:
        """
        See `self.dtype` for details.
        """
        # Note: 'c' (complex) not handled yet (not in array spec v1).
        #       'b', 'B' (bytes), 'S', 'a', (old-style string) 'V' (void) not handled
        #       datetime and timedelta both map to datetime (is timedelta handled?)

        kind = _NP_KINDS.get(dtype.kind, None)
        if kind is None:
            # Not a NumPy dtype. Check if it's a categorical maybe
            raise ValueError(f"Data type {dtype} not supported by interchange protocol")
        if isinstance(dtype, ArrowDtype):
            byteorder = dtype.numpy_dtype.byteorder
        elif isinstance(dtype, DatetimeTZDtype):
            byteorder = dtype.base.byteorder  # type: ignore[union-attr]
        elif isinstance(dtype, BaseMaskedDtype):
            byteorder = dtype.numpy_dtype.byteorder
        else:
            byteorder = dtype.byteorder

        if dtype == "bool[pyarrow]":
            # return early to avoid the `* 8` below, as this is a bitmask
            # rather than a bytemask
            return (
                kind,
                dtype.itemsize,  # pyright: ignore[reportAttributeAccessIssue]
                ArrowCTypes.BOOL,
                byteorder,
            )

        return kind, dtype.itemsize * 8, dtype_to_arrow_c_fmt(dtype), byteorder


    @property
    def describe_null(self):
        if isinstance(self._query_compiler.dtypes.iloc[0], BaseMaskedDtype):
            column_null_dtype = ColumnNullType.USE_BYTEMASK
            null_value = 1
            return column_null_dtype, null_value
        if isinstance(self._query_compiler.dtypes.iloc[0], ArrowDtype):
            # We already rechunk (if necessary / allowed) upon initialization, so this
            # is already single-chunk by the time we get here.
            if self._col.array._pa_array.chunks[0].buffers()[0] is None:  # type: ignore[attr-defined]
                return ColumnNullType.NON_NULLABLE, None
            return ColumnNullType.USE_BITMASK, 0
        kind = self.dtype[0]
        try:
            null, value = _NULL_DESCRIPTION[kind]
        except KeyError as err:
            raise NotImplementedError(f"Data type {kind} not yet supported") from err

        return null, value
    

    @property
    def null_count(self) -> int:
        """
        Number of null elements. Should always be known.
        """
        raise NotImplementedError

    @property
    def metadata(self) -> dict[str, pd.Index]:
        """
        Store specific metadata of the column.
        """
        return {"pandas.index": self._query_compiler.index.to_pandas()}

    def get_buffers(self):
        return self._query_compiler.to_pandas().__dataframe__().get_column(0).get_buffers()
    
    def num_chunks(self) -> int:
        """
        Return the number of chunks the column consists of.
        """
        return 1


    def get_chunks(self, n_chunks = None):
        yield self

    def __dataframe__(
        self, nan_as_null: bool = False, allow_copy: bool = True):
        return self        
    
    def describe_categorical(self, *args, **kwargs):
        raise NotImplementedError
    
    # DO NOT MERGE: 