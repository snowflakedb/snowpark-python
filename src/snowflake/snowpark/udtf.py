#!/usr/bin/env python3
#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

"""User-defined table functions (UDTFs) in Snowpark. Please see `Python UDTF <https://docs.snowflake.com/en/developer-guide/snowpark/python/creating-udtfs>`_ for details.
There is also vectorized UDTF. Compared to the default row-by-row processing pattern of a normal UDTF, which sometimes is inefficient, vectorized Python UDTFs (user-defined table functions) enable seamless partition-by-partition processing
by operating on partitions as `pandas DataFrames <https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.html>`_ and returning results as `pandas DataFrames <https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.html>`_ or lists of
`pandas arrays <https://pandas.pydata.org/docs/reference/api/pandas.array.html>`_ or `pandas Series <https://pandas.pydata.org/docs/reference/series.html>`_.

In addition, vectorized Python UDTFs allow for easy integration with libraries that operate on pandas DataFrames or pandas arrays.

A vectorized UDTF handler class:
    - defines an :code:`end_partition` method that takes in a DataFrame argument and returns a :code:`pandas.DataFrame` or a tuple of :code:`pandas.Series` or :code:`pandas.arrays` where each array is a column.
    - does NOT define a :code:`process` method.
    - optionally defines a handler class with an :code:`__init__` method which will be invoked before processing each partition.

Note:
    A vectorized UDTF must be called with :meth:`~snowflake.snowpark.Window.partition_by` to build the partitions.

Refer to :class:`~snowflake.snowpark.udtf.UDTFRegistration` for details and sample code on how to create regular and vectorized UDTFs using Snowpark Python API.
"""
import sys
from types import ModuleType
from typing import Any, Callable, Dict, List, Optional, Tuple, Type, Union

import snowflake.snowpark
from snowflake.connector import ProgrammingError
from snowflake.snowpark._internal.error_message import SnowparkClientExceptionMessages
from snowflake.snowpark._internal.open_telemetry import (
    open_telemetry_udf_context_manager,
)
from snowflake.snowpark._internal.type_utils import ColumnOrName
from snowflake.snowpark._internal.udf_utils import (
    UDFColumn,
    check_python_runtime_version,
    check_register_args,
    cleanup_failed_permanent_registration,
    create_python_udf_or_sp,
    process_file_path,
    process_registration_inputs,
    resolve_imports_and_packages,
)
from snowflake.snowpark._internal.utils import TempObjectType, validate_object_name
from snowflake.snowpark.table_function import TableFunctionCall
from snowflake.snowpark.types import DataType, PandasDataFrameType, StructType

# Python 3.8 needs to use typing.Iterable because collections.abc.Iterable is not subscriptable
# Python 3.9 can use both
# Python 3.10 needs to use collections.abc.Iterable because typing.Iterable is removed
if sys.version_info <= (3, 9):
    from typing import Iterable
else:
    from collections.abc import Iterable


class UserDefinedTableFunction:
    """
    Encapsulates a user defined table function that is returned by
    :func:`~snowflake.snowpark.functions.udtf`, :meth:`UDTFRegistration.register` or
    :meth:`UDTFRegistration.register_from_file`. The constructor of this class is not supposed
    to be called directly.

    Call an instance of :class:`UserDefinedTableFunction` to generate a
    :class:`~snowflake.snowpark.table_function.TableFunctionCall` instance. The input type can be
    a column name as a :class:`str`, or a :class:`~snowflake.snowpark.Column` object.

    See Also:
        - :class:`UDTFRegistration`
        - :func:`~snowflake.snowpark.functions.udtf`
    """

    def __init__(
        self,
        handler: Union[Callable, Tuple[str, str]],
        output_schema: Union[StructType, Iterable[str], "PandasDataFrameType"],
        input_types: List[DataType],
        name: str,
        packages: Optional[List[Union[str, ModuleType]]] = None,
    ) -> None:
        #: The Python class or a tuple containing the Python file path and the function name.
        self.handler: Union[Callable, Tuple[str, str]] = handler
        #: The UDTF name.
        self.name: str = name

        self._output_schema = output_schema
        self._input_types = input_types

        self._packages = packages

    def __call__(
        self,
        *arguments: Union[ColumnOrName, Iterable[ColumnOrName]],
        **named_arguments,
    ) -> TableFunctionCall:
        table_function_call = TableFunctionCall(
            self.name, *arguments, **named_arguments
        )
        table_function_call._set_api_call_source("UserDefinedTableFunction.__call__")
        return table_function_call


class UDTFRegistration:
    """
    Provides methods to register classes as UDTFs in the Snowflake database.
    For more information about Snowflake Python UDTFs, see `Python UDTFs <https://docs.snowflake.com/en/developer-guide/udf/python/udf-python-tabular-functions.html>`__.

    :attr:`session.udtf <snowflake.snowpark.Session.udtf>` returns an object of this class.
    You can use this object to register UDTFs that you plan to use in the current session or
    permanently. The methods that register a UDTF return a :class:`UserDefinedTableFunction` object,
    which you can also use to call the UDTF.

    Registering a UDTF is like registering a scalar UDF, you can use :meth:`register` or :func:`snowflake.snowpark.functions.udtf`
    to explicitly register it. You can also use the decorator `@udtf`. They all use ``cloudpickle`` to transfer the code from the client to the server.
    Another way is to use :meth:`register_from_file`. Refer to module :class:`snowflake.snowpark.udtf.UDTFRegistration` for when to use them.

    To query a registered UDTF is the same as to query other table functions.
    Refer to :meth:`~snowflake.snowpark.Session.table_function` and :meth:`~snowflake.snowpark.DataFrame.join_table_function`.
    If you want to query a UDTF right after it's created, you can call the created :class:`UserDefinedTableFunction` instance like in Example 1 below.

    Example 1
        Create a temporary UDTF and call it:

            >>> from snowflake.snowpark.types import IntegerType, StructField, StructType
            >>> from snowflake.snowpark.functions import udtf, lit
            >>> class GeneratorUDTF:
            ...     def process(self, n):
            ...         for i in range(n):
            ...             yield (i, )
            >>> generator_udtf = udtf(GeneratorUDTF, output_schema=StructType([StructField("number", IntegerType())]), input_types=[IntegerType()])
            >>> session.table_function(generator_udtf(lit(3))).collect()  # Query it by calling it
            [Row(NUMBER=0), Row(NUMBER=1), Row(NUMBER=2)]
            >>> session.table_function(generator_udtf.name, lit(3)).collect()  # Query it by using the name
            [Row(NUMBER=0), Row(NUMBER=1), Row(NUMBER=2)]
            >>> # Or you can lateral-join a UDTF like any other table functions
            >>> df = session.create_dataframe([2, 3], schema=["c"])
            >>> df.join_table_function(generator_udtf(df["c"])).sort("c", "number").show()
            ------------------
            |"C"  |"NUMBER"  |
            ------------------
            |2    |0         |
            |2    |1         |
            |3    |0         |
            |3    |1         |
            |3    |2         |
            ------------------
            <BLANKLINE>

    Example 2
        Create a UDTF with type hints and ``@udtf`` decorator and query it:

            >>> from snowflake.snowpark.types import IntegerType, StructField, StructType
            >>> from snowflake.snowpark.functions import udtf, lit
            >>> @udtf(output_schema=["number"])
            ... class generator_udtf:
            ...     def process(self, n: int) -> Iterable[Tuple[int]]:
            ...         for i in range(n):
            ...             yield (i, )
            >>> session.table_function(generator_udtf(lit(3))).collect()  # Query it by calling it
            [Row(NUMBER=0), Row(NUMBER=1), Row(NUMBER=2)]
            >>> session.table_function(generator_udtf.name, lit(3)).collect()  # Query it by using the name
            [Row(NUMBER=0), Row(NUMBER=1), Row(NUMBER=2)]

    Example 3
        Create a permanent UDTF with a name and call it in SQL:

            >>> from snowflake.snowpark.types import IntegerType, StructField, StructType
            >>> from snowflake.snowpark.functions import udtf, lit
            >>> _ = session.sql("create or replace temp stage mystage").collect()
            >>> class GeneratorUDTF:
            ...     def process(self, n):
            ...         for i in range(n):
            ...             yield (i, )
            >>> generator_udtf = udtf(
            ...     GeneratorUDTF, output_schema=StructType([StructField("number", IntegerType())]), input_types=[IntegerType()],
            ...     is_permanent=True, name="generator_udtf", replace=True, stage_location="@mystage"
            ... )
            >>> session.sql("select * from table(generator_udtf(3))").collect()
            [Row(NUMBER=0), Row(NUMBER=1), Row(NUMBER=2)]

    Example 4
        Create a UDTF with type hints:

            >>> from snowflake.snowpark.types import IntegerType, StructField, StructType
            >>> from snowflake.snowpark.functions import udtf, lit
            >>> @udtf(output_schema=["n1", "n2"])
            ... class generator_udtf:
            ...     def process(self, n: int) -> Iterable[Tuple[int, int]]:
            ...         for i in range(n):
            ...             yield (i, i+1)
            >>> session.table_function(generator_udtf(lit(3))).collect()
            [Row(N1=0, N2=1), Row(N1=1, N2=2), Row(N1=2, N2=3)]

    Example 5
        Create a UDTF with type hints by using ``...`` for multiple columns of the same type:

            >>> from snowflake.snowpark.types import IntegerType, StructField, StructType
            >>> from snowflake.snowpark.functions import udtf, lit
            >>> @udtf(output_schema=["n1", "n2"])
            ... class generator_udtf:
            ...     def process(self, n: int) -> Iterable[Tuple[int, ...]]:
            ...         for i in range(n):
            ...             yield (i, i+1)
            >>> session.table_function(generator_udtf(lit(3))).collect()
            [Row(N1=0, N2=1), Row(N1=1, N2=2), Row(N1=2, N2=3)]

    Example 6
        Create a UDTF with UDF-level imports and type hints:

            >>> from resources.test_udf_dir.test_udf_file import mod5
            >>> from snowflake.snowpark.types import IntegerType, StructField, StructType
            >>> from snowflake.snowpark.functions import udtf, lit
            >>> @udtf(output_schema=["number"], imports=[("tests/resources/test_udf_dir/test_udf_file.py", "resources.test_udf_dir.test_udf_file")])
            ... class generator_udtf:
            ...     def process(self, n: int) -> Iterable[Tuple[int]]:
            ...         for i in range(n):
            ...             yield (mod5(i), )
            >>> session.table_function(generator_udtf(lit(6))).collect()
            [Row(NUMBER=0), Row(NUMBER=1), Row(NUMBER=2), Row(NUMBER=3), Row(NUMBER=4), Row(NUMBER=0)]

    Example 7
        Create a UDTF with UDF-level packages and type hints:

            >>> from snowflake.snowpark.types import IntegerType, StructField, StructType
            >>> from snowflake.snowpark.functions import udtf, lit
            >>> import numpy as np
            >>> @udtf(output_schema=["number"], packages=["numpy"])
            ... class generator_udtf:
            ...     def process(self, n: int) -> Iterable[Tuple[int]]:
            ...         for i in np.arange(n):
            ...             yield (i, )
            >>> session.table_function(generator_udtf(lit(3))).collect()
            [Row(NUMBER=0), Row(NUMBER=1), Row(NUMBER=2)]

    Example 8
        Creating a UDTF with the constructor and ``end_partition`` method.

            >>> from collections import Counter
            >>> from typing import Iterable, Tuple
            >>> from snowflake.snowpark.functions import lit
            >>> class MyWordCount:
            ...     def __init__(self) -> None:
            ...         self._total_per_partition = 0
            ...
            ...     def process(self, s1: str) -> Iterable[Tuple[str, int]]:
            ...         words = s1.split()
            ...         self._total_per_partition = len(words)
            ...         counter = Counter(words)
            ...         yield from counter.items()
            ...
            ...     def end_partition(self):
            ...         yield ("partition_total", self._total_per_partition)

            >>> udtf_name = "word_count_udtf"
            >>> word_count_udtf = session.udtf.register(
            ...     MyWordCount, ["word", "count"], name=udtf_name, is_permanent=False, replace=True
            ... )
            >>> # Call it by its name
            >>> df1 = session.table_function(udtf_name, lit("w1 w2 w2 w3 w3 w3"))
            >>> df1.show()
            -----------------------------
            |"WORD"           |"COUNT"  |
            -----------------------------
            |w1               |1        |
            |w2               |2        |
            |w3               |3        |
            |partition_total  |6        |
            -----------------------------
            <BLANKLINE>

            >>> # Call it by the returned callable instance
            >>> df2 = session.table_function(word_count_udtf(lit("w1 w2 w2 w3 w3 w3")))
            >>> df2.show()
            -----------------------------
            |"WORD"           |"COUNT"  |
            -----------------------------
            |w1               |1        |
            |w2               |2        |
            |w3               |3        |
            |partition_total  |6        |
            -----------------------------
            <BLANKLINE>

    Example 9
        Creating a UDTF from a local Python file:

            >>> from snowflake.snowpark.types import IntegerType, StructField, StructType
            >>> from snowflake.snowpark.functions import udtf, lit
            >>> generator_udtf = session.udtf.register_from_file(
            ...     file_path="tests/resources/test_udtf_dir/test_udtf_file.py",
            ...     handler_name="GeneratorUDTF",
            ...     output_schema=StructType([StructField("number", IntegerType())]),
            ...     input_types=[IntegerType()]
            ... )
            >>> session.table_function(generator_udtf(lit(3))).collect()
            [Row(NUMBER=0), Row(NUMBER=1), Row(NUMBER=2)]

    Example 10
        Creating a UDTF from a Python file on an internal stage:

            >>> from snowflake.snowpark.types import IntegerType, StructField, StructType
            >>> from snowflake.snowpark.functions import udtf, lit
            >>> _ = session.sql("create or replace temp stage mystage").collect()
            >>> _ = session.file.put("tests/resources/test_udtf_dir/test_udtf_file.py", "@mystage", auto_compress=False)
            >>> generator_udtf = session.udtf.register_from_file(
            ...     file_path="@mystage/test_udtf_file.py",
            ...     handler_name="GeneratorUDTF",
            ...     output_schema=StructType([StructField("number", IntegerType())]),
            ...     input_types=[IntegerType()]
            ... )
            >>> session.table_function(generator_udtf(lit(3))).collect()
            [Row(NUMBER=0), Row(NUMBER=1), Row(NUMBER=2)]

    You can use :func:`~snowflake.snowpark.functions.udtf`, :meth:`register` or
    :func:`~snowflake.snowpark.functions.pandas_udtf` to create a vectorized UDTF by providing
    appropriate return and input types. If you would like to use :meth:`register_from_file` to
    create a vectorized UDTF, you would need to explicitly mark the handler method as vectorized using
    either the decorator ``@vectorized(input=pandas.DataFrame)`` or setting
    ``<class>.end_partition._sf_vectorized_input = pandas.DataFrame``

    Example 11
        Creating a vectorized UDTF by specifying a ``PandasDataFrameType`` as ``input_types`` and a
        ``PandasDataFrameType`` with column names as ``output_schema``.

            >>> from snowflake.snowpark.types import PandasDataFrameType, IntegerType, StringType, FloatType
            >>> class multiply:
            ...     def __init__(self):
            ...         self.multiplier = 10
            ...     def end_partition(self, df):
            ...         df.col1 = df.col1*self.multiplier
            ...         df.col2 = df.col2*self.multiplier
            ...         yield df
            >>> multiply_udtf = session.udtf.register(
            ...     multiply,
            ...     output_schema=PandasDataFrameType([StringType(), IntegerType(), FloatType()], ["id_", "col1_", "col2_"]),
            ...     input_types=[PandasDataFrameType([StringType(), IntegerType(), FloatType()])],
            ...     input_names = ['"id"', '"col1"', '"col2"'],
            ... )
            >>> df = session.create_dataframe([['x', 3, 35.9],['x', 9, 20.5]], schema=["id", "col1", "col2"])
            >>> df.select(multiply_udtf("id", "col1", "col2").over(partition_by=["id"])).sort("col1_").show()
            -----------------------------
            |"ID_"  |"COL1_"  |"COL2_"  |
            -----------------------------
            |x      |30       |359.0    |
            |x      |90       |205.0    |
            -----------------------------
            <BLANKLINE>

    Example 12
        Creating a vectorized UDTF by specifying ``PandasDataFrame`` with nested types as type hints.

            >>> from snowflake.snowpark.types import PandasDataFrame
            >>> class multiply:
            ...     def __init__(self):
            ...         self.multiplier = 10
            ...     def end_partition(self, df: PandasDataFrame[str, int, float]) -> PandasDataFrame[str, int, float]:
            ...         df.col1 = df.col1*self.multiplier
            ...         df.col2 = df.col2*self.multiplier
            ...         yield df
            >>> multiply_udtf = session.udtf.register(
            ...     multiply,
            ...     output_schema=["id_", "col1_", "col2_"],
            ...     input_names = ['"id"', '"col1"', '"col2"'],
            ... )
            >>> df = session.create_dataframe([['x', 3, 35.9],['x', 9, 20.5]], schema=["id", "col1", "col2"])
            >>> df.select(multiply_udtf("id", "col1", "col2").over(partition_by=["id"])).sort("col1_").show()
            -----------------------------
            |"ID_"  |"COL1_"  |"COL2_"  |
            -----------------------------
            |x      |30       |359.0    |
            |x      |90       |205.0    |
            -----------------------------
            <BLANKLINE>

    Example 13
        Creating a vectorized UDTF by specifying a ``pandas.DataFrame`` as type hints and a ``StructType`` with type information and column names as ``output_schema``.

            >>> import pandas as pd
            >>> from snowflake.snowpark.types import IntegerType, StringType, FloatType, StructType, StructField
            >>> class multiply:
            ...     def __init__(self):
            ...         self.multiplier = 10
            ...     def end_partition(self, df: pd.DataFrame) -> pd.DataFrame:
            ...         df.col1 = df.col1*self.multiplier
            ...         df.col2 = df.col2*self.multiplier
            ...         yield df
            >>> multiply_udtf = session.udtf.register(
            ...     multiply,
            ...     output_schema=StructType([StructField("id_", StringType()), StructField("col1_", IntegerType()), StructField("col2_", FloatType())]),
            ...     input_types=[StringType(), IntegerType(), FloatType()],
            ...     input_names = ['"id"', '"col1"', '"col2"'],
            ... )
            >>> df = session.create_dataframe([['x', 3, 35.9],['x', 9, 20.5]], schema=["id", "col1", "col2"])
            >>> df.select(multiply_udtf("id", "col1", "col2").over(partition_by=["id"])).sort("col1_").show()
            -----------------------------
            |"ID_"  |"COL1_"  |"COL2_"  |
            -----------------------------
            |x      |30       |359.0    |
            |x      |90       |205.0    |
            -----------------------------
            <BLANKLINE>

    Example 14
        Same as Example 12, but does not specify `input_names` and instead set the column names in `end_partition`.

            >>> from snowflake.snowpark.types import PandasDataFrameType, IntegerType, StringType, FloatType
            >>> class multiply:
            ...     def __init__(self):
            ...         self.multiplier = 10
            ...     def end_partition(self, df):
            ...         df.columns = ["id", "col1", "col2"]
            ...         df.col1 = df.col1*self.multiplier
            ...         df.col2 = df.col2*self.multiplier
            ...         yield df
            >>> multiply_udtf = session.udtf.register(
            ...     multiply,
            ...     output_schema=PandasDataFrameType([StringType(), IntegerType(), FloatType()], ["id_", "col1_", "col2_"]),
            ...     input_types=[PandasDataFrameType([StringType(), IntegerType(), FloatType()])],
            ... )
            >>> df = session.create_dataframe([['x', 3, 35.9],['x', 9, 20.5]], schema=["id", "col1", "col2"])
            >>> df.select(multiply_udtf("id", "col1", "col2").over(partition_by=["id"])).sort("col1_").show()
            -----------------------------
            |"ID_"  |"COL1_"  |"COL2_"  |
            -----------------------------
            |x      |30       |359.0    |
            |x      |90       |205.0    |
            -----------------------------
            <BLANKLINE>

    [Preview Feature] The syntax for declaring UDTF with a vectorized process method is similar to above.
    Defining ``__init__`` and ``end_partition`` methods are optional. The ``process`` method only accepts one
    argument which is the pandas Dataframe object, and outputs the same number of rows as is in the given input.
    Both ``__init__`` and ``end_partition`` do not take any additional arguments.

    Example 15
        Vectorized UDTF process method without end_partition

            >>> class multiply:
            ...     def process(self, df: PandasDataFrame[str,int, float]) -> PandasDataFrame[int]:
            ...         return (df['col1'] * 10, )
            >>> multiply_udtf = session.udtf.register(
            ...     multiply,
            ...     output_schema=["col1x10"],
            ...     input_names=['"id"', '"col1"', '"col2"']
            ... )
            >>> df = session.create_dataframe([['x', 3, 35.9],['x', 9, 20.5]], schema=["id", "col1", "col2"])
            >>> df.select("id", "col1", "col2", multiply_udtf("id", "col1", "col2")).order_by("col1").show()
            --------------------------------------
            |"ID"  |"COL1"  |"COL2"  |"COL1X10"  |
            --------------------------------------
            |x     |3       |35.9    |30         |
            |x     |9       |20.5    |90         |
            --------------------------------------
            <BLANKLINE>


    Example 16
        Vectorized UDTF process method with end_partition

            >>> class mean:
            ...     def __init__(self) -> None:
            ...         self.sum = 0
            ...         self.len = 0
            ...     def process(self, df: pd.DataFrame) -> pd.DataFrame:
            ...         self.sum += df['value'].sum()
            ...         self.len += len(df)
            ...         return ([None] * len(df),)
            ...     def end_partition(self):
            ...         return ([self.sum / self.len],)
            >>> mean_udtf = session.udtf.register(mean,
            ...                       output_schema=StructType([StructField("mean", FloatType())]),
            ...                       input_types=[StringType(), IntegerType()],
            ...                       input_names=['"name"', '"value"'])
            >>> df = session.create_dataframe([["x", 10], ["x", 20], ["x", 33], ["y", 10], ["y", 25], ], schema=["name", "value"])
            >>> df.select("name", "value", mean_udtf("name", "value").over(partition_by="name")).order_by("name", "value").show()
            -----------------------------
            |"NAME"  |"VALUE"  |"MEAN"  |
            -----------------------------
            |x       |NULL     |21.0    |
            |x       |10       |NULL    |
            |x       |20       |NULL    |
            |x       |33       |NULL    |
            |y       |NULL     |17.5    |
            |y       |10       |NULL    |
            |y       |25       |NULL    |
            -----------------------------
            <BLANKLINE>

    Example 17
        Vectorized UDTF process method with end_partition and max_batch_size

            >>> class sum:
            ...     def __init__(self):
            ...         self.sum = None
            ...     def process(self, df):
            ...         if self.sum is None:
            ...             self.sum = df
            ...         else:
            ...             self.sum += df
            ...         return df
            ...     def end_partition(self):
            ...         return self.sum
            >>> sum_udtf = session.udtf.register(sum,
            ...         output_schema=PandasDataFrameType([StringType(), IntegerType()], ["id_", "col1_"]),
            ...         input_types=[PandasDataFrameType([StringType(), IntegerType()])],
            ...         max_batch_size=1)
            >>> df = session.create_dataframe([["x", 10], ["x", 20], ["x", 33], ["y", 10], ["y", 25], ], schema=["id", "col1"])
            >>> df.select("id", "col1", sum_udtf("id", "col1").over(partition_by="id")).order_by("id", "col1").show()
            -----------------------------------
            |"ID"  |"COL1"  |"ID_"  |"COL1_"  |
            -----------------------------------
            |x     |NULL    |xxx    |63       |
            |x     |10      |x      |10       |
            |x     |20      |x      |20       |
            |x     |33      |x      |33       |
            |y     |NULL    |yy     |35       |
            |y     |10      |y      |10       |
            |y     |25      |y      |25       |
            -----------------------------------
            <BLANKLINE>

    See Also:
        - :func:`~snowflake.snowpark.functions.udtf`
        - :meth:`register`
        - :meth:`register_from_file`
        - :meth:`~snowflake.snowpark.Session.add_import`
        - :meth:`~snowflake.snowpark.Session.add_packages`
        - :meth:`~snowflake.snowpark.Session.table_function`
        - :meth:`~snowflake.snowpark.DataFrame.join_table_function`
    """

    def __init__(self, session: Optional["snowflake.snowpark.Session"]) -> None:
        self._session = session

    def register(
        self,
        handler: Type,
        output_schema: Union[StructType, Iterable[str], "PandasDataFrameType"],
        input_types: Optional[List[DataType]] = None,
        input_names: Optional[List[str]] = None,
        name: Optional[Union[str, Iterable[str]]] = None,
        is_permanent: bool = False,
        stage_location: Optional[str] = None,
        imports: Optional[List[Union[str, Tuple[str, str]]]] = None,
        packages: Optional[List[Union[str, ModuleType]]] = None,
        replace: bool = False,
        if_not_exists: bool = False,
        parallel: int = 4,
        strict: bool = False,
        secure: bool = False,
        external_access_integrations: Optional[List[str]] = None,
        secrets: Optional[Dict[str, str]] = None,
        immutable: bool = False,
        max_batch_size: Optional[int] = None,
        comment: Optional[str] = None,
        *,
        statement_params: Optional[Dict[str, str]] = None,
        **kwargs,
    ) -> UserDefinedTableFunction:
        """
        Registers a Python class as a Snowflake Python UDTF and returns the UDTF.
        The usage, input arguments, and return value of this method are the same as
        they are for :func:`~snowflake.snowpark.functions.udtf`, but :meth:`register`
        cannot be used as a decorator. See examples in
        :class:`~snowflake.snowpark.udtf.UDTFRegistration`.

        Args:
            handler: A Python class used for creating the UDTF.
            output_schema: A list of column names, or a :class:`~snowflake.snowpark.types.StructType` instance that represents the table function's columns, or a ``PandasDataFrameType`` instance for vectorized UDTF.
             If a list of column names is provided, the ``process`` method of the handler class must have a return type hint to indicate the output schema data types.
            input_types: A list of :class:`~snowflake.snowpark.types.DataType`
                representing the input data types of the UDTF. Optional if
                type hints are provided.
            input_names: A list of `str` representing the input column names of the UDTF, this only applies to vectorized UDTF and is essentially a noop for regular UDTFs. If unspecified, default column names will be
                ARG1, ARG2, etc.
            name: A string or list of strings that specify the name or fully-qualified
                object identifier (database name, schema name, and function name) for
                the UDTF in Snowflake.
                If it is not provided, a name will be automatically generated for the UDTF.
                A name must be specified when ``is_permanent`` is ``True``.
            is_permanent: Whether to create a permanent UDTF. The default is ``False``.
                If it is ``True``, a valid ``stage_location`` must be provided.
            stage_location: The stage location where the Python file for the UDTF
                and its dependencies should be uploaded. The stage location must be specified
                when ``is_permanent`` is ``True``, and it will be ignored when
                ``is_permanent`` is ``False``. It can be any stage other than temporary
                stages and external stages.
            imports: A list of imports that only apply to this UDTF. You can use a string to
                represent a file path (similar to the ``path`` argument in
                :meth:`~snowflake.snowpark.Session.add_import`) in this list, or a tuple of two
                strings to represent a file path and an import path (similar to the ``import_path``
                argument in :meth:`~snowflake.snowpark.Session.add_import`). These UDTF-level imports
                will override the session-level imports added by
                :meth:`~snowflake.snowpark.Session.add_import`.
            packages: A list of packages that only apply to this UDTF. These UDTF-level packages
                will override the session-level packages added by
                :meth:`~snowflake.snowpark.Session.add_packages` and
                :meth:`~snowflake.snowpark.Session.add_requirements`. To use Python packages that are not available
                in Snowflake, refer to :meth:`~snowflake.snowpark.Session.custom_package_usage_config`.
            replace: Whether to replace a UDTF that already was registered. The default is ``False``.
                If it is ``False``, attempting to register a UDTF with a name that already exists
                results in a ``SnowparkSQLException`` exception being thrown. If it is ``True``,
                an existing UDTF with the same name is overwritten.
            if_not_exists: Whether to skip creation of a UDTF when one with the same signature already exists.
                The default is ``False``. ``if_not_exists`` and ``replace`` are mutually exclusive
                and a ``ValueError`` is raised when both are set. If it is ``True`` and a UDTF with
                the same signature exists, the UDTF creation is skipped.
            session: Use this session to register the UDTF. If it's not specified, the session that you created before calling this function will be used.
                You need to specify this parameter if you have created multiple sessions before calling this method.
            parallel: The number of threads to use for uploading UDTF files with the
                `PUT <https://docs.snowflake.com/en/sql-reference/sql/put.html#put>`_
                command. The default value is 4 and supported values are from 1 to 99.
                Increasing the number of threads can improve performance when uploading
                large UDTF files.
            strict: Whether the created UDTF is strict. A strict UDTF will not invoke the UDTF if any input is
                null. Instead, a null value will always be returned for that row. Note that the UDTF might
                still return null for non-null inputs.
            secure: Whether the created UDTF is secure. For more information about secure functions,
                see `Secure UDFs <https://docs.snowflake.com/en/sql-reference/udf-secure.html>`_.
            statement_params: Dictionary of statement level parameters to be set while executing this action.
            external_access_integrations: The names of one or more external access integrations. Each
                integration you specify allows access to the external network locations and secrets
                the integration specifies.
            secrets: The key-value pairs of string types of secrets used to authenticate the external network location.
                The secrets can be accessed from handler code. The secrets specified as values must
                also be specified in the external access integration and the keys are strings used to
                retrieve the secrets using secret API.
            immutable: Whether the UDTF result is deterministic or not for the same input.
            max_batch_size: The maximum number of rows per input pandas DataFrame or pandas Series
                inside a vectorized UDTF. Because a vectorized UDTF will be executed within a time limit,
                which is `60` seconds, this optional argument can be used to reduce the running time of
                every batch by setting a smaller batch size. Note that setting a larger value does not
                guarantee that Snowflake will encode batches with the specified number of rows. It will
                be ignored when registering a non-vectorized UDTF.
            comment: Adds a comment for the created object. See
                `COMMENT <https://docs.snowflake.com/en/sql-reference/sql/comment>`_

        See Also:
            - :func:`~snowflake.snowpark.functions.udtf`
            - :meth:`register_from_file`
        """
        with open_telemetry_udf_context_manager(
            self.register, handler=handler, name=name
        ):
            if not callable(handler):
                raise TypeError(
                    "Invalid function: not a function or callable "
                    f"(__call__ is not defined): {type(handler)}"
                )

            check_register_args(
                TempObjectType.TABLE_FUNCTION,
                name,
                is_permanent,
                stage_location,
                parallel,
            )

            native_app_params = kwargs.get("native_app_params", None)

            # register udtf
            return self._do_register_udtf(
                handler,
                output_schema,
                input_types,
                input_names,
                name,
                stage_location,
                imports,
                packages,
                replace,
                if_not_exists,
                parallel,
                strict,
                secure,
                external_access_integrations=external_access_integrations,
                secrets=secrets,
                immutable=immutable,
                max_batch_size=max_batch_size,
                comment=comment,
                statement_params=statement_params,
                api_call_source="UDTFRegistration.register",
                is_permanent=is_permanent,
                native_app_params=native_app_params,
            )

    def register_from_file(
        self,
        file_path: str,
        handler_name: str,
        output_schema: Union[StructType, Iterable[str], "PandasDataFrameType"],
        input_types: Optional[List[DataType]] = None,
        input_names: Optional[List[str]] = None,
        name: Optional[Union[str, Iterable[str]]] = None,
        is_permanent: bool = False,
        stage_location: Optional[str] = None,
        imports: Optional[List[Union[str, Tuple[str, str]]]] = None,
        packages: Optional[List[Union[str, ModuleType]]] = None,
        replace: bool = False,
        if_not_exists: bool = False,
        parallel: int = 4,
        strict: bool = False,
        secure: bool = False,
        external_access_integrations: Optional[List[str]] = None,
        secrets: Optional[Dict[str, str]] = None,
        immutable: bool = False,
        comment: Optional[str] = None,
        *,
        statement_params: Optional[Dict[str, str]] = None,
        skip_upload_on_content_match: bool = False,
    ) -> UserDefinedTableFunction:
        """
        Registers a Python class as a Snowflake Python UDTF from a Python or zip file,
        and returns the UDTF. Apart from ``file_path`` and ``func_name``, the input arguments
        of this method are the same as :meth:`register`. See examples in
        :class:`~snowflake.snowpark.udtf.UDTFRegistration`.

        Args:
            file_path: The path of a local file or a remote file in the stage. See
                more details on ``path`` argument of
                :meth:`session.add_import() <snowflake.snowpark.Session.add_import>`.
                Note that unlike ``path`` argument of
                :meth:`session.add_import() <snowflake.snowpark.Session.add_import>`,
                here the file can only be a Python file or a compressed file
                (e.g., .zip file) containing Python modules.
            handler_name: The Python class name in the file that the UDTF will use as the handler.
            output_schema: A list of column names, or a :class:`~snowflake.snowpark.types.StructType` instance that represents the table function's columns, or a ``PandasDataFrameType`` instance for vectorized UDTF.
            input_types: A list of :class:`~snowflake.snowpark.types.DataType`
                representing the input data types of the UDTF. Optional if
                type hints are provided.
            input_names: A list of `str` representing the input column names of the UDTF, this only applies to vectorized UDTF and is essentially a noop for regular UDTFs. If unspecified, default column names will be
                ARG1, ARG2, etc.
            name: A string or list of strings that specify the name or fully-qualified
                object identifier (database name, schema name, and function name) for
                the UDTF in Snowflake, which allows you to call this UDTF in a SQL
                command or via :func:`~snowflake.snowpark.functions.call_udtf`.
                If it is not provided, a name will be automatically generated for the UDTF.
                A name must be specified when ``is_permanent`` is ``True``.
            is_permanent: Whether to create a permanent UDTF. The default is ``False``.
                If it is ``True``, a valid ``stage_location`` must be provided.
            stage_location: The stage location where the Python file for the UDTF
                and its dependencies should be uploaded. The stage location must be specified
                when ``is_permanent`` is ``True``, and it will be ignored when
                ``is_permanent`` is ``False``. It can be any stage other than temporary
                stages and external stages.
            imports: A list of imports that only apply to this UDTF. You can use a string to
                represent a file path (similar to the ``path`` argument in
                :meth:`~snowflake.snowpark.Session.add_import`) in this list, or a tuple of two
                strings to represent a file path and an import path (similar to the ``import_path``
                argument in :meth:`~snowflake.snowpark.Session.add_import`). These UDTF-level imports
                will override the session-level imports added by
                :meth:`~snowflake.snowpark.Session.add_import`.
            packages: A list of packages that only apply to this UDTF. These UDTF-level packages
                will override the session-level packages added by
                :meth:`~snowflake.snowpark.Session.add_packages` and
                :meth:`~snowflake.snowpark.Session.add_requirements`. To use Python packages that are not
                available in Snowflake, refer to :meth:`~snowflake.snowpark.Session.custom_package_usage_config`.
            replace: Whether to replace a UDTF that already was registered. The default is ``False``.
                If it is ``False``, attempting to register a UDTF with a name that already exists
                results in a ``SnowparkSQLException`` exception being thrown. If it is ``True``,
                an existing UDTF with the same name is overwritten.
            if_not_exists: Whether to skip creation of a UDTF when one with the same signature already exists.
                The default is ``False``. ``if_not_exists`` and ``replace`` are mutually exclusive
                and a ``ValueError`` is raised when both are set. If it is ``True`` and a UDTF with
                the same signature exists, the UDTF creation is skipped.
            session: Use this session to register the UDTF. If it's not specified, the session that you created before calling this function will be used.
                You need to specify this parameter if you have created multiple sessions before calling this method.
            parallel: The number of threads to use for uploading UDTF files with the
                `PUT <https://docs.snowflake.com/en/sql-reference/sql/put.html#put>`_
                command. The default value is 4 and supported values are from 1 to 99.
                Increasing the number of threads can improve performance when uploading
                large UDTF files.
            strict: Whether the created UDTF is strict. A strict UDTF will not invoke the UDTF if any input is
                null. Instead, a null value will always be returned for that row. Note that the UDTF might
                still return null for non-null inputs.
            secure: Whether the created UDTF is secure. For more information about secure functions,
                see `Secure UDFs <https://docs.snowflake.com/en/sql-reference/udf-secure.html>`_.
            statement_params: Dictionary of statement level parameters to be set while executing this action.
            skip_upload_on_content_match: When set to ``True`` and a version of source file already exists on stage, the given source
                file will be uploaded to stage only if the contents of the current file differ from the remote file on stage. Defaults
                to ``False``.
            external_access_integrations: The names of one or more external access integrations. Each
                integration you specify allows access to the external network locations and secrets
                the integration specifies.
            secrets: The key-value pairs of string types of secrets used to authenticate the external network location.
                The secrets can be accessed from handler code. The secrets specified as values must
                also be specified in the external access integration and the keys are strings used to
                retrieve the secrets using secret API.
            immutable: Whether the UDTF result is deterministic or not for the same input.
            comment: Adds a comment for the created object. See
                `COMMENT <https://docs.snowflake.com/en/sql-reference/sql/comment>`_

        Note::
            The type hints can still be extracted from the local source Python file if they
            are provided, but currently are not working for a zip file or a remote file. Therefore,
            you have to provide ``output_schema`` and ``input_types`` when ``path``
            points to a zip file or a remote file.

        See Also:
            - :func:`~snowflake.snowpark.functions.udtf`
            - :meth:`register`
        """
        with open_telemetry_udf_context_manager(
            self.register_from_file,
            file_path=file_path,
            handler_name=handler_name,
            name=name,
        ):
            file_path = process_file_path(file_path)
            check_register_args(
                TempObjectType.TABLE_FUNCTION,
                name,
                is_permanent,
                stage_location,
                parallel,
            )

            # register udtf
            return self._do_register_udtf(
                (file_path, handler_name),
                output_schema,
                input_types,
                input_names,
                name,
                stage_location,
                imports,
                packages,
                replace,
                if_not_exists,
                parallel,
                strict,
                secure,
                external_access_integrations=external_access_integrations,
                secrets=secrets,
                immutable=immutable,
                comment=comment,
                statement_params=statement_params,
                api_call_source="UDTFRegistration.register_from_file",
                skip_upload_on_content_match=skip_upload_on_content_match,
                is_permanent=is_permanent,
            )

    def _do_register_udtf(
        self,
        handler: Union[Callable, Tuple[str, str]],
        output_schema: Union[StructType, Iterable[str], "PandasDataFrameType"],
        input_types: Optional[List[DataType]],
        input_names: Optional[List[str]],
        name: Optional[str],
        stage_location: Optional[str] = None,
        imports: Optional[List[Union[str, Tuple[str, str]]]] = None,
        packages: Optional[List[Union[str, ModuleType]]] = None,
        replace: bool = False,
        if_not_exists: bool = False,
        parallel: int = 4,
        strict: bool = False,
        secure: bool = False,
        external_access_integrations: Optional[List[str]] = None,
        secrets: Optional[Dict[str, str]] = None,
        immutable: bool = False,
        max_batch_size: Optional[int] = None,
        comment: Optional[str] = None,
        *,
        native_app_params: Optional[Dict[str, Any]] = None,
        statement_params: Optional[Dict[str, str]] = None,
        api_call_source: str,
        skip_upload_on_content_match: bool = False,
        is_permanent: bool = False,
    ) -> UserDefinedTableFunction:

        if isinstance(output_schema, StructType):
            _validate_output_schema_names(output_schema.names)
            return_type = output_schema
            output_schema = None
        elif isinstance(output_schema, PandasDataFrameType):
            _validate_output_schema_names(output_schema.col_names)
            return_type = output_schema
            output_schema = None
        elif isinstance(
            output_schema, Iterable
        ):  # with column names instead of StructType. Read type hints to infer column types.
            output_schema = tuple(output_schema)
            _validate_output_schema_names(output_schema)
            return_type = None
        else:
            raise ValueError(
                f"'output_schema' must be a list of column names or StructType or PandasDataFrameType instance to create a UDTF. Got {type(output_schema)}."
            )

        # get the udtf name, input types
        (
            udtf_name,
            is_pandas_udf,
            is_dataframe_input,
            output_schema,
            input_types,
        ) = process_registration_inputs(
            self._session,
            TempObjectType.TABLE_FUNCTION,
            handler,
            return_type,
            input_types,
            name,
            output_schema=output_schema,
        )

        arg_names = input_names or [f"arg{i + 1}" for i in range(len(input_types))]
        input_args = [
            UDFColumn(dt, arg_name) for dt, arg_name in zip(input_types, arg_names)
        ]
        (
            handler_name,
            code,
            all_imports,
            all_packages,
            upload_file_stage_location,
            custom_python_runtime_version_allowed,
        ) = resolve_imports_and_packages(
            self._session,
            TempObjectType.TABLE_FUNCTION,
            handler,
            arg_names,
            udtf_name,
            stage_location,
            imports,
            packages,
            parallel,
            is_pandas_udf,
            is_dataframe_input,
            max_batch_size,
            statement_params=statement_params,
            skip_upload_on_content_match=skip_upload_on_content_match,
            is_permanent=is_permanent,
        )

        if (not custom_python_runtime_version_allowed) and (self._session is not None):
            check_python_runtime_version(
                self._session._runtime_version_from_requirement
            )

        raised = False
        try:
            create_python_udf_or_sp(
                session=self._session,
                func=handler,
                return_type=output_schema,
                input_args=input_args,
                handler=handler_name,
                object_type=TempObjectType.FUNCTION,
                object_name=udtf_name,
                all_imports=all_imports,
                all_packages=all_packages,
                raw_imports=imports,
                is_permanent=is_permanent,
                replace=replace,
                if_not_exists=if_not_exists,
                inline_python_code=code,
                api_call_source=api_call_source,
                strict=strict,
                secure=secure,
                external_access_integrations=external_access_integrations,
                secrets=secrets,
                immutable=immutable,
                statement_params=statement_params,
                comment=comment,
                native_app_params=native_app_params,
            )
        # an exception might happen during registering a udtf
        # (e.g., a dependency might not be found on the stage),
        # then for a permanent udtf, we should delete the uploaded
        # python file and raise the exception
        except ProgrammingError as pe:
            raised = True
            tb = sys.exc_info()[2]
            ne = SnowparkClientExceptionMessages.SQL_EXCEPTION_FROM_PROGRAMMING_ERROR(
                pe
            )
            raise ne.with_traceback(tb) from None
        except BaseException:
            raised = True
            raise
        finally:
            if raised:
                cleanup_failed_permanent_registration(
                    self._session, upload_file_stage_location, stage_location
                )

        return UserDefinedTableFunction(
            handler, output_schema, input_types, udtf_name, packages=packages
        )


def _validate_output_schema_names(names: Iterable[str]) -> None:
    for name in names:
        validate_object_name(name)
