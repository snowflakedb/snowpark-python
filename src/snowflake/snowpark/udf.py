#!/usr/bin/env python3
#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#
"""User-defined functions (UDFs) in Snowpark."""
import sys
from types import ModuleType
from typing import Callable, Dict, Iterable, List, Optional, Tuple, Union

import snowflake.snowpark
from snowflake.connector import ProgrammingError
from snowflake.snowpark._internal.analyzer.expression import Expression, SnowflakeUDF
from snowflake.snowpark._internal.error_message import SnowparkClientExceptionMessages
from snowflake.snowpark._internal.telemetry import TelemetryField
from snowflake.snowpark._internal.type_utils import ColumnOrName, convert_sp_to_sf_type
from snowflake.snowpark._internal.udf_utils import (
    UDFColumn,
    check_register_args,
    cleanup_failed_permanent_registration,
    create_python_udf_or_sp,
    process_file_path,
    process_registration_inputs,
    resolve_imports_and_packages,
)
from snowflake.snowpark._internal.utils import (
    TempObjectType,
    parse_positional_args_to_list,
    warning,
)
from snowflake.snowpark.column import Column
from snowflake.snowpark.types import DataType


class UserDefinedFunction:
    """
    Encapsulates a user defined lambda or function that is returned by
    :func:`~snowflake.snowpark.functions.udf`, :meth:`UDFRegistration.register` or
    :meth:`UDFRegistration.register_from_file`. The constructor of this class is not supposed
    to be called directly.

    Call an instance of :class:`UserDefinedFunction` to generate
    :class:`~snowflake.snowpark.Column` expressions. The input type can be
    a column name as a :class:`str`, or a :class:`~snowflake.snowpark.Column` object.

    See Also:
        - :class:`UDFRegistration`
        - :func:`~snowflake.snowpark.functions.udf`
    """

    def __init__(
        self,
        func: Union[Callable, Tuple[str, str]],
        return_type: DataType,
        input_types: List[DataType],
        name: str,
        is_return_nullable: bool = False,
    ) -> None:
        #: The Python function or a tuple containing the Python file path and the function name.
        self.func: Union[Callable, Tuple[str, str]] = func
        #: The UDF name.
        self.name: str = name

        self._return_type = return_type
        self._input_types = input_types
        self._is_return_nullable = is_return_nullable

    def __call__(
        self,
        *cols: Union[ColumnOrName, Iterable[ColumnOrName]],
    ) -> Column:
        if len(cols) >= 1 and isinstance(cols[0], (list, tuple)):
            warning(
                "udf.__call__",
                "Passing arguments to a UDF with a list or tuple is deprecated. We still respect this "
                "invocation but please consider passing variable-length arguments without a list or tuple.",
            )

        exprs = []
        for c in parse_positional_args_to_list(*cols):
            if isinstance(c, Column):
                exprs.append(c._expression)
            elif isinstance(c, str):
                exprs.append(Column(c)._expression)
            else:
                raise TypeError(
                    f"The input of UDF {self.name} must be Column, column name, or a list of them"
                )

        session = snowflake.snowpark.context.get_active_session()
        session._conn._telemetry_client.send_function_usage_telemetry(
            "UserDefinedFunction.__call__", TelemetryField.FUNC_CAT_USAGE.value
        )
        return Column(self._create_udf_expression(exprs))

    def _create_udf_expression(self, exprs: List[Expression]) -> SnowflakeUDF:
        if len(exprs) != len(self._input_types):
            raise ValueError(
                f"Incorrect number of arguments passed to the UDF:"
                f" Expected: {len(exprs)}, Found: {len(self._input_types)}"
            )
        return SnowflakeUDF(
            self.name,
            exprs,
            self._return_type,
            nullable=self._is_return_nullable,
            api_call_source="UserDefinedFunction.__call__",
        )


class UDFRegistration:
    """
    Provides methods to register lambdas and functions as UDFs in the Snowflake database.
    For more information about Snowflake Python UDFs, see `Python UDFs <https://docs.snowflake.com/en/developer-guide/udf/python/udf-python.html>`__.

    :attr:`session.udf <snowflake.snowpark.Session.udf>` returns an object of this class.
    You can use this object to register UDFs that you plan to use in the current session or
    permanently. The methods that register a UDF return a :class:`UserDefinedFunction` object,
    which you can also use in :class:`~snowflake.snowpark.Column` expressions.

    There are two ways to register a UDF with Snowpark:

        - Use :func:`~snowflake.snowpark.functions.udf` or :meth:`register`. By pointing to a
          `runtime Python function`, Snowpark uses `cloudpickle <https://github.com/cloudpipe/cloudpickle>`_
          to serialize this function to bytecode, and deserialize the bytecode to a Python
          function on the Snowflake server during UDF creation. During the serialization, the
          global variables used in the Python function will be serialized into the bytecode,
          but only the name of the module object or any objects from a module that are used in the
          Python function will be serialized. During the deserialization, Python will look up the
          corresponding modules and objects by names. For example::

                >>> import numpy
                >>> from resources.test_udf_dir.test_udf_file import mod5
                >>> a = 1
                >>> def f():
                ...     return 2
                >>>
                >>> from snowflake.snowpark.functions import udf
                >>> session.add_import("tests/resources/test_udf_dir/test_udf_file.py", import_path="resources.test_udf_dir.test_udf_file")
                >>> session.add_packages("numpy")
                >>> @udf
                ... def g(x: int) -> int:
                ...     return mod5(numpy.square(x)) + a + f()
                >>> df = session.create_dataframe([4], schema=["a"])
                >>> df.select(g("a")).to_df("col1").show()
                ----------
                |"COL1"  |
                ----------
                |4       |
                ----------
                <BLANKLINE>

          Here the variable ``a`` and function ``f`` will be serialized into the bytecode, but
          only the name of ``numpy`` and ``mod5`` will be included in the bytecode. Therefore,
          in order to have these modules on the server side, you can use
          :meth:`~snowflake.snowpark.Session.add_import` and :meth:`~snowflake.snowpark.Session.add_packages`
          to add your first-party and third-party libraries.

          After deserialization, this function will be executed and applied to every row of your
          dataframe or table during UDF execution. This approach is very flexible because you can
          either create a UDF from a function in your current file/notebook, and you can also import
          the function from elsewhere. However, the limitations of this approach are:

            * All code inside the function will be executed on every row, so you are not able to
              perform some initializations before executing this function. For example, if you want
              to read a file from a stage in a UDF, this file will be read on every row. However,
              we still have a workaround for this scenario, which can be found in Example 8 here.

            * If the runtime function references some very large global variables (e.g., a machine
              learning model with a large number of parameters), they will also be serialized and
              the size of bytecode can be very large, which will take more time for uploading files.
              Also, the UDF creation will fail if the referenced global variables cannot be pickled
              (e.g., ``weakref`` object). In this case, you usually have to save such objects in the
              local environment first, add it to the UDF using  :meth:`~snowflake.snowpark.Session.add_import`,
              and read it from the UDF (see Example 8 here).

        - Use :meth:`register_from_file`. By pointing to a `Python file` or a `zip file containing
          Python source code` and the target function name, Snowpark uploads this file to a stage
          (which can also be customized), and load the corresponding function from this file to
          the Python runtime on the Snowflake server during UDF creation. Then this function will be
          executed and applied to every row of your dataframe or table when executing this UDF.
          This approach can address the deficiency of the previous approach that uses cloudpickle,
          because the source code in this file other than the target function will be loaded
          during UDF creation, and will not be executed on every row during UDF execution.
          Therefore, this approach is useful and efficient when all your Python code is already in
          source files.

    Compared to the default row-by-row processing pattern of a normal UDF, which sometimes is
    inefficient, a vectorized UDF allows vectorized operations on a dataframe, with the input as a
    `Pandas DataFrame <https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.html>`_
    or `Pandas Series <https://pandas.pydata.org/docs/reference/api/pandas.Series.html>`_. In a
    vectorized UDF, you can operate on a batches of rows by handling Pandas DataFrame or Pandas
    Series. You can use :func:`~snowflake.snowpark.functions.udf`, :meth:`register` or
    :func:`~snowflake.snowpark.functions.pandas_udf` to create a vectorized UDF by providing
    appropriate return and input types. If you would like to use :meth:`register_from_file` to
    create a vectorized UDF, you should follow the guide of
    `Python UDF Batch API <https://docs.snowflake.com/en/developer-guide/udf/python/udf-python-batch.html>`_ in
    your Python source files. See Example 9, 10 and 11 here for registering a vectorized UDF.

    Snowflake supports the following data types for the parameters for a UDF:

    =============================================  ======================================================= ============
    Python Type                                    Snowpark Type                                           SQL Type
    =============================================  ======================================================= ============
    ``int``                                        :class:`~snowflake.snowpark.types.LongType`             NUMBER
    ``decimal.Decimal``                            :class:`~snowflake.snowpark.types.DecimalType`          NUMBER
    ``float``                                      :class:`~snowflake.snowpark.types.FloatType`            FLOAT
    ``str``                                        :class:`~snowflake.snowpark.types.StringType`           STRING
    ``bool``                                       :class:`~snowflake.snowpark.types.BooleanType`          BOOL
    ``datetime.time``                              :class:`~snowflake.snowpark.types.TimeType`             TIME
    ``datetime.date``                              :class:`~snowflake.snowpark.types.DateType`             DATE
    ``datetime.datetime``                          :class:`~snowflake.snowpark.types.TimestampType`        TIMESTAMP
    ``bytes`` or ``bytearray``                     :class:`~snowflake.snowpark.types.BinaryType`           BINARY
    ``list``                                       :class:`~snowflake.snowpark.types.ArrayType`            ARRAY
    ``dict``                                       :class:`~snowflake.snowpark.types.MapType`              OBJECT
    Dynamically mapped to the native Python type   :class:`~snowflake.snowpark.types.VariantType`          VARIANT
    ``dict``                                       :class:`~snowflake.snowpark.types.GeographyType`        GEOGRAPHY
    ``pandas.Series``                              :class:`~snowflake.snowpark.types.PandasSeriesType`     No SQL type
    ``pandas.DataFrame``                           :class:`~snowflake.snowpark.types.PandasDataFrameType`  No SQL type
    =============================================  ======================================================= ============

    Note:
        1. Data with the VARIANT SQL type will be converted to a Python type
        dynamically inside a UDF. The following SQL types are converted to :class:`str`
        in UDFs rather than native Python types: TIME, DATE, TIMESTAMP and BINARY.

        2. Data returned as :class:`~snowflake.snowpark.types.ArrayType` (``list``),
        :class:`~snowflake.snowpark.types.MapType` (``dict``) or
        :class:`~snowflake.snowpark.types.VariantType` (:attr:`~snowflake.snowpark.types.Variant`)
        by a UDF will be represented as a json string. You can call ``eval()`` or ``json.loads()``
        to convert the result to a native Python object. Data returned as
        :class:`~snowflake.snowpark.types.GeographyType` (:attr:`~snowflake.snowpark.types.Geography`)
        by a UDF will be represented as a `GeoJSON <https://datatracker.ietf.org/doc/html/rfc7946>`_
        string.

        3. :class:`~snowflake.snowpark.types.PandasSeriesType` and
        :class:`~snowflake.snowpark.types.PandasDataFrameType` are used when creating a Pandas
        (vectorized) UDF, so they are not mapped to any SQL types. ``element_type`` in
        :class:`~snowflake.snowpark.types.PandasSeriesType` and ``col_types`` in
        :class:`~snowflake.snowpark.types.PandasDataFrameType` indicate the SQL types
        in a Pandas Series and a Pandas DataFrame.

    Example 1
        Create a temporary UDF from a lambda and apply it to a dataframe::

            >>> from snowflake.snowpark.types import IntegerType
            >>> from snowflake.snowpark.functions import udf
            >>> add_one_udf = udf(lambda x: x+1, return_type=IntegerType(), input_types=[IntegerType()])
            >>> session.range(1, 8, 2).select(add_one_udf("id")).to_df("col1").collect()
            [Row(COL1=2), Row(COL1=4), Row(COL1=6), Row(COL1=8)]

    Example 2
        Create a UDF with type hints and ``@udf`` decorator and apply it to a dataframe::

            >>> from snowflake.snowpark.functions import udf
            >>> @udf
            ... def add_udf(x: int, y: int) -> int:
            ...        return x + y
            >>> df = session.create_dataframe([[1, 2], [3, 4]], schema=["x", "y"])
            >>> df.select(add_udf("x", "y")).to_df("add_result").collect()
            [Row(ADD_RESULT=3), Row(ADD_RESULT=7)]

    Example 3
        Create a permanent UDF with a name and call it in SQL::

            >>> from snowflake.snowpark.types import IntegerType
            >>> _ = session.sql("create or replace temp stage mystage").collect()
            >>> _ = session.udf.register(
            ...     lambda x, y: x * y, return_type=IntegerType(),
            ...     input_types=[IntegerType(), IntegerType()],
            ...     is_permanent=True, name="mul", replace=True,
            ...     stage_location="@mystage",
            ... )
            >>> session.sql("select mul(5, 6)").show()
            ---------------
            |"MUL(5, 6)"  |
            ---------------
            |30           |
            ---------------
            <BLANKLINE>

    Example 4
        Create a UDF with UDF-level imports and apply it to a dataframe::

            >>> from resources.test_udf_dir.test_udf_file import mod5
            >>> from snowflake.snowpark.functions import udf
            >>> @udf(imports=[("tests/resources/test_udf_dir/test_udf_file.py", "resources.test_udf_dir.test_udf_file")])
            ... def mod5_and_plus1_udf(x: int) -> int:
            ...     return mod5(x) + 1
            >>> session.range(1, 8, 2).select(mod5_and_plus1_udf("id")).to_df("col1").collect()
            [Row(COL1=2), Row(COL1=4), Row(COL1=1), Row(COL1=3)]

    Example 5
        Create a UDF with UDF-level packages and apply it to a dataframe::

            >>> from snowflake.snowpark.functions import udf
            >>> import numpy as np
            >>> import math
            >>> @udf(packages=["numpy"])
            ... def sin_udf(x: float) -> float:
            ...     return np.sin(x)
            >>> df = session.create_dataframe([0.0, 0.5 * math.pi], schema=["d"])
            >>> df.select(sin_udf("d")).to_df("col1").collect()
            [Row(COL1=0.0), Row(COL1=1.0)]

    Example 6
        Creating a UDF from a local Python file::

            >>> # mod5() in that file has type hints
            >>> mod5_udf = session.udf.register_from_file(
            ...     file_path="tests/resources/test_udf_dir/test_udf_file.py",
            ...     func_name="mod5",
            ... )
            >>> session.range(1, 8, 2).select(mod5_udf("id")).to_df("col1").collect()
            [Row(COL1=1), Row(COL1=3), Row(COL1=0), Row(COL1=2)]

    Example 7
        Creating a UDF from a Python file on an internal stage::

            >>> from snowflake.snowpark.types import IntegerType
            >>> _ = session.sql("create or replace temp stage mystage").collect()
            >>> _ = session.file.put("tests/resources/test_udf_dir/test_udf_file.py", "@mystage", auto_compress=False)
            >>> mod5_udf = session.udf.register_from_file(
            ...     file_path="@mystage/test_udf_file.py",
            ...     func_name="mod5",
            ...     return_type=IntegerType(),
            ...     input_types=[IntegerType()],
            ... )
            >>> session.range(1, 8, 2).select(mod5_udf("id")).to_df("col1").collect()
            [Row(COL1=1), Row(COL1=3), Row(COL1=0), Row(COL1=2)]

    Example 8
        Use cache to read a file once from a stage in a UDF::

            >>> import sys
            >>> import os
            >>> import cachetools
            >>> from snowflake.snowpark.types import StringType
            >>> @cachetools.cached(cache={})
            ... def read_file(filename):
            ...     import_dir = sys._xoptions.get("snowflake_import_directory")
            ...     if import_dir:
            ...         with open(os.path.join(import_dir, filename), "r") as f:
            ...             return f.read()
            >>>
            >>> # create a temporary text file for test
            >>> temp_file_name = "/tmp/temp.txt"
            >>> with open(temp_file_name, "w") as t:
            ...     _ = t.write("snowpark")
            >>> session.add_import(temp_file_name)
            >>> session.add_packages("cachetools")
            >>> concat_file_content_with_str_udf = session.udf.register(
            ...     lambda s: f"{read_file(os.path.basename(temp_file_name))}-{s}",
            ...     return_type=StringType(),
            ...     input_types=[StringType()]
            ... )
            >>>
            >>> df = session.create_dataframe(["snowflake", "python"], schema=["a"])
            >>> df.select(concat_file_content_with_str_udf("a")).to_df("col1").collect()
            [Row(COL1='snowpark-snowflake'), Row(COL1='snowpark-python')]
            >>> os.remove(temp_file_name)
            >>> session.clear_imports()

        In this example, the file will only be read once during UDF creation, and will not
        be read again during UDF execution. This is achieved with a third-party library
        `cachetools <https://pypi.org/project/cachetools/>`_. You can also use ``LRUCache``
        and ``TTLCache`` in this package to avoid the cache growing too large. Note that Python
        built-in `cache decorators <https://docs.python.org/3/library/functools.html#functools.cache>`_
        are not working when registering UDFs using Snowpark, due to the limitation of cloudpickle.

    Example 9
        Create a vectorized UDF from a lambda with a max batch size and apply it to a dataframe::

            >>> from snowflake.snowpark.functions import udf
            >>> from snowflake.snowpark.types import IntegerType, PandasSeriesType, PandasDataFrameType
            >>> df = session.create_dataframe([[1, 2], [3, 4]]).to_df("a", "b")
            >>> add_udf1 = udf(lambda series1, series2: series1 + series2, return_type=PandasSeriesType(IntegerType()),
            ...               input_types=[PandasSeriesType(IntegerType()), PandasSeriesType(IntegerType())],
            ...               max_batch_size=20)
            >>> df.select(add_udf1("a", "b")).to_df("add_result").collect()
            [Row(ADD_RESULT=3), Row(ADD_RESULT=7)]
            >>> add_udf2 = udf(lambda df: df[0] + df[1], return_type=PandasSeriesType(IntegerType()),
            ...               input_types=[PandasDataFrameType([IntegerType(), IntegerType()])],
            ...               max_batch_size=20)
            >>> df.select(add_udf2("a", "b")).to_df("add_result").collect()
            [Row(ADD_RESULT=3), Row(ADD_RESULT=7)]

    Example 10
        Create a vectorized UDF with type hints and apply it to a dataframe::

            >>> from snowflake.snowpark.functions import udf
            >>> from snowflake.snowpark.types import PandasSeries, PandasDataFrame
            >>> @udf
            ... def apply_mod5_udf(series: PandasSeries[int]) -> PandasSeries[int]:
            ...     return series.apply(lambda x: x % 5)
            >>> session.range(1, 8, 2).select(apply_mod5_udf("id")).to_df("col1").collect()
            [Row(COL1=1), Row(COL1=3), Row(COL1=0), Row(COL1=2)]
            >>> @udf
            ... def mul_udf(df: PandasDataFrame[int, int]) -> PandasSeries[int]:
            ...     return df[0] * df[1]
            >>> df = session.create_dataframe([[1, 2], [3, 4]]).to_df("a", "b")
            >>> df.select(mul_udf("a", "b")).to_df("col1").collect()
            [Row(COL1=2), Row(COL1=12)]

    Example 11
        Create a vectorized UDF with original ``pandas`` types and Snowpark types and apply it to a dataframe::

            >>> # `pandas_udf` is an alias of `udf`, but it can only be used to create a vectorized UDF
            >>> from snowflake.snowpark.functions import pandas_udf
            >>> from snowflake.snowpark.types import IntegerType
            >>> import pandas as pd
            >>> df = session.create_dataframe([[1, 2], [3, 4]]).to_df("a", "b")
            >>> def add1(series1: pd.Series, series2: pd.Series) -> pd.Series:
            ...     return series1 + series2
            >>> add_udf1 = pandas_udf(add1, return_type=IntegerType(),
            ...                       input_types=[IntegerType(), IntegerType()])
            >>> df.select(add_udf1("a", "b")).to_df("add_result").collect()
            [Row(ADD_RESULT=3), Row(ADD_RESULT=7)]
            >>> def add2(df: pd.DataFrame) -> pd.Series:
            ...     return df[0] + df[1]
            >>> add_udf2 = pandas_udf(add2, return_type=IntegerType(),
            ...                       input_types=[IntegerType(), IntegerType()])
            >>> df.select(add_udf2("a", "b")).to_df("add_result").collect()
            [Row(ADD_RESULT=3), Row(ADD_RESULT=7)]

    See Also:
        - :func:`~snowflake.snowpark.functions.udf`
        - :meth:`register`
        - :meth:`register_from_file`
        - :meth:`~snowflake.snowpark.Session.add_import`
        - :meth:`~snowflake.snowpark.Session.add_packages`
    """

    def __init__(self, session: "snowflake.snowpark.session.Session") -> None:
        self._session = session

    def describe(
        self, udf_obj: UserDefinedFunction
    ) -> "snowflake.snowpark.dataframe.DataFrame":
        """
        Returns a :class:`~snowflake.snowpark.DataFrame` that describes the properties of a UDF.

        Args:
            udf_obj: A :class:`UserDefinedFunction` returned by
                :func:`~snowflake.snowpark.functions.udf` or :meth:`register`.
        """
        func_args = [convert_sp_to_sf_type(t) for t in udf_obj._input_types]
        return self._session.sql(
            f"describe function {udf_obj.name}({','.join(func_args)})"
        )

    def register(
        self,
        func: Callable,
        return_type: Optional[DataType] = None,
        input_types: Optional[List[DataType]] = None,
        name: Optional[Union[str, Iterable[str]]] = None,
        is_permanent: bool = False,
        stage_location: Optional[str] = None,
        imports: Optional[List[Union[str, Tuple[str, str]]]] = None,
        packages: Optional[List[Union[str, ModuleType]]] = None,
        replace: bool = False,
        parallel: int = 4,
        max_batch_size: Optional[int] = None,
        *,
        statement_params: Optional[Dict[str, str]] = None,
        source_code_display: bool = True,
        **kwargs,
    ) -> UserDefinedFunction:
        """
        Registers a Python function as a Snowflake Python UDF and returns the UDF.
        The usage, input arguments, and return value of this method are the same as
        they are for :func:`~snowflake.snowpark.functions.udf`, but :meth:`register`
        cannot be used as a decorator. See examples in
        :class:`~snowflake.snowpark.udf.UDFRegistration` and notes in
        :func:`~snowflake.snowpark.functions.udf`.

        Args:
            func: A Python function used for creating the UDF.
            return_type: A :class:`~snowflake.snowpark.types.DataType` representing the return data
                type of the UDF. Optional if type hints are provided.
            input_types: A list of :class:`~snowflake.snowpark.types.DataType`
                representing the input data types of the UDF. Optional if
                type hints are provided.
            name: A string or list of strings that specify the name or fully-qualified
                object identifier (database name, schema name, and function name) for
                the UDF in Snowflake, which allows you to call this UDF in a SQL
                command or via :func:`~snowflake.snowpark.functions.call_udf`.
                If it is not provided, a name will be automatically generated for the UDF.
                A name must be specified when ``is_permanent`` is ``True``.
            is_permanent: Whether to create a permanent UDF. The default is ``False``.
                If it is ``True``, a valid ``stage_location`` must be provided.
            stage_location: The stage location where the Python file for the UDF
                and its dependencies should be uploaded. The stage location must be specified
                when ``is_permanent`` is ``True``, and it will be ignored when
                ``is_permanent`` is ``False``. It can be any stage other than temporary
                stages and external stages.
            imports: A list of imports that only apply to this UDF. You can use a string to
                represent a file path (similar to the ``path`` argument in
                :meth:`~snowflake.snowpark.Session.add_import`) in this list, or a tuple of two
                strings to represent a file path and an import path (similar to the ``import_path``
                argument in :meth:`~snowflake.snowpark.Session.add_import`). These UDF-level imports
                will override the session-level imports added by
                :meth:`~snowflake.snowpark.Session.add_import`. Note that an empty list means
                no import for this UDF, and ``None`` or not specifying this parameter means using
                session-level imports.
            packages: A list of packages that only apply to this UDF. These UDF-level packages
                will override the session-level packages added by
                :meth:`~snowflake.snowpark.Session.add_packages` and
                :meth:`~snowflake.snowpark.Session.add_requirements`. Note that an empty list means
                no package for this UDF, and ``None`` or not specifying this parameter means using
                session-level packages.
            replace: Whether to replace a UDF that already was registered. The default is ``False``.
                If it is ``False``, attempting to register a UDF with a name that already exists
                results in a ``SnowparkSQLException`` exception being thrown. If it is ``True``,
                an existing UDF with the same name is overwritten.
            parallel: The number of threads to use for uploading UDF files with the
                `PUT <https://docs.snowflake.com/en/sql-reference/sql/put.html#put>`_
                command. The default value is 4 and supported values are from 1 to 99.
                Increasing the number of threads can improve performance when uploading
                large UDF files.
            max_batch_size: The maximum number of rows per input Pandas DataFrame or Pandas Series
                inside a vectorized UDF. Because a vectorized UDF will be executed within a time limit,
                which is `60` seconds, this optional argument can be used to reduce the running time of
                every batch by setting a smaller batch size. Note that setting a larger value does not
                guarantee that Snowflake will encode batches with the specified number of rows. It will
                be ignored when registering a non-vectorized UDF.
            statement_params: Dictionary of statement level parameters to be set while executing this action.
            source_code_display: Display the source code of the UDF `func` as comments in the generated script.
                The source code is dynamically generated therefore it may not be identical to how the
                `func` is originally defined. The default is ``True``.
                If it is ``False``, source code will not be generated or displayed.

        See Also:
            - :func:`~snowflake.snowpark.functions.udf`
            - :meth:`register_from_file`
        """
        if not callable(func):
            raise TypeError(
                "Invalid function: not a function or callable "
                f"(__call__ is not defined): {type(func)}"
            )

        check_register_args(
            TempObjectType.FUNCTION, name, is_permanent, stage_location, parallel
        )

        _from_pandas = kwargs.get("_from_pandas_udf_function", False)

        # register udf
        return self._do_register_udf(
            func,
            return_type,
            input_types,
            name,
            stage_location,
            imports,
            packages,
            replace,
            parallel,
            max_batch_size,
            _from_pandas,
            statement_params=statement_params,
            source_code_display=source_code_display,
            api_call_source="UDFRegistration.register"
            + ("[pandas_udf]" if _from_pandas else ""),
        )

    def register_from_file(
        self,
        file_path: str,
        func_name: str,
        return_type: Optional[DataType] = None,
        input_types: Optional[List[DataType]] = None,
        name: Optional[Union[str, Iterable[str]]] = None,
        is_permanent: bool = False,
        stage_location: Optional[str] = None,
        imports: Optional[List[Union[str, Tuple[str, str]]]] = None,
        packages: Optional[List[Union[str, ModuleType]]] = None,
        replace: bool = False,
        parallel: int = 4,
        *,
        statement_params: Optional[Dict[str, str]] = None,
        source_code_display: bool = True,
    ) -> UserDefinedFunction:
        """
        Registers a Python function as a Snowflake Python UDF from a Python or zip file,
        and returns the UDF. Apart from ``file_path`` and ``func_name``, the input arguments
        of this method are the same as :meth:`register`. See examples in
        :class:`~snowflake.snowpark.udf.UDFRegistration`.

        Args:
            file_path: The path of a local file or a remote file in the stage. See
                more details on ``path`` argument of
                :meth:`session.add_import() <snowflake.snowpark.Session.add_import>`.
                Note that unlike ``path`` argument of
                :meth:`session.add_import() <snowflake.snowpark.Session.add_import>`,
                here the file can only be a Python file or a compressed file
                (e.g., .zip file) containing Python modules.
            func_name: The Python function name in the file that will be created
                as a UDF.
            return_type: A :class:`~snowflake.snowpark.types.DataType` representing the return data
                type of the UDF. Optional if type hints are provided.
            input_types: A list of :class:`~snowflake.snowpark.types.DataType`
                representing the input data types of the UDF. Optional if
                type hints are provided.
            name: A string or list of strings that specify the name or fully-qualified
                object identifier (database name, schema name, and function name) for
                the UDF in Snowflake, which allows you to call this UDF in a SQL
                command or via :func:`~snowflake.snowpark.functions.call_udf`.
                If it is not provided, a name will be automatically generated for the UDF.
                A name must be specified when ``is_permanent`` is ``True``.
            is_permanent: Whether to create a permanent UDF. The default is ``False``.
                If it is ``True``, a valid ``stage_location`` must be provided.
            stage_location: The stage location where the Python file for the UDF
                and its dependencies should be uploaded. The stage location must be specified
                when ``is_permanent`` is ``True``, and it will be ignored when
                ``is_permanent`` is ``False``. It can be any stage other than temporary
                stages and external stages.
            imports: A list of imports that only apply to this UDF. You can use a string to
                represent a file path (similar to the ``path`` argument in
                :meth:`~snowflake.snowpark.Session.add_import`) in this list, or a tuple of two
                strings to represent a file path and an import path (similar to the ``import_path``
                argument in :meth:`~snowflake.snowpark.Session.add_import`). These UDF-level imports
                will override the session-level imports added by
                :meth:`~snowflake.snowpark.Session.add_import`. Note that an empty list means
                no import for this UDF, and ``None`` or not specifying this parameter means using
                session-level imports.
            packages: A list of packages that only apply to this UDF. These UDF-level packages
                will override the session-level packages added by
                :meth:`~snowflake.snowpark.Session.add_packages` and
                :meth:`~snowflake.snowpark.Session.add_requirements`. Note that an empty list means
                no package for this UDF, and ``None`` or not specifying this parameter means using
                session-level packages.
            replace: Whether to replace a UDF that already was registered. The default is ``False``.
                If it is ``False``, attempting to register a UDF with a name that already exists
                results in a ``SnowparkSQLException`` exception being thrown. If it is ``True``,
                an existing UDF with the same name is overwritten.
            parallel: The number of threads to use for uploading UDF files with the
                `PUT <https://docs.snowflake.com/en/sql-reference/sql/put.html#put>`_
                command. The default value is 4 and supported values are from 1 to 99.
                Increasing the number of threads can improve performance when uploading
                large UDF files.
            statement_params: Dictionary of statement level parameters to be set while executing this action.
            source_code_display: Display the source code of the UDF `func` as comments in the generated script.
                The source code is dynamically generated therefore it may not be identical to how the
                `func` is originally defined. The default is ``True``.
                If it is ``False``, source code will not be generated or displayed.

        Note::
            The type hints can still be extracted from the source Python file if they
            are provided, but currently are not working for a zip file. Therefore,
            you have to provide ``return_type`` and ``input_types`` when ``path``
            points to a zip file.

        See Also:
            - :func:`~snowflake.snowpark.functions.udf`
            - :meth:`register`
        """
        file_path = process_file_path(file_path)
        check_register_args(
            TempObjectType.FUNCTION, name, is_permanent, stage_location, parallel
        )

        # register udf
        return self._do_register_udf(
            (file_path, func_name),
            return_type,
            input_types,
            name,
            stage_location,
            imports,
            packages,
            replace,
            parallel,
            statement_params=statement_params,
            source_code_display=source_code_display,
            api_call_source="UDFRegistration.register_from_file",
        )

    def _do_register_udf(
        self,
        func: Union[Callable, Tuple[str, str]],
        return_type: Optional[DataType],
        input_types: Optional[List[DataType]],
        name: Optional[str],
        stage_location: Optional[str] = None,
        imports: Optional[List[Union[str, Tuple[str, str]]]] = None,
        packages: Optional[List[Union[str, ModuleType]]] = None,
        replace: bool = False,
        parallel: int = 4,
        max_batch_size: Optional[int] = None,
        from_pandas_udf_function: bool = False,
        *,
        statement_params: Optional[Dict[str, str]] = None,
        source_code_display: bool = True,
        api_call_source: str,
    ) -> UserDefinedFunction:
        # get the udf name, return and input types
        (
            udf_name,
            is_pandas_udf,
            is_dataframe_input,
            return_type,
            input_types,
        ) = process_registration_inputs(
            self._session, TempObjectType.FUNCTION, func, return_type, input_types, name
        )

        arg_names = [f"arg{i + 1}" for i in range(len(input_types))]
        input_args = [
            UDFColumn(dt, arg_name) for dt, arg_name in zip(input_types, arg_names)
        ]

        # allow registering pandas UDF from udf(),
        # but not allow registering non-pandas UDF from pandas_udf()
        if from_pandas_udf_function and not is_pandas_udf:
            raise ValueError(
                "You cannot create a non-vectorized UDF using pandas_udf(). "
                "Use udf() instead."
            )

        (
            handler,
            code,
            all_imports,
            all_packages,
            upload_file_stage_location,
        ) = resolve_imports_and_packages(
            self._session,
            TempObjectType.FUNCTION,
            func,
            arg_names,
            udf_name,
            stage_location,
            imports,
            packages,
            parallel,
            is_pandas_udf,
            is_dataframe_input,
            max_batch_size,
            statement_params=statement_params,
            source_code_display=source_code_display,
        )

        raised = False
        try:
            create_python_udf_or_sp(
                session=self._session,
                return_type=return_type,
                input_args=input_args,
                handler=handler,
                object_type=TempObjectType.FUNCTION,
                object_name=udf_name,
                all_imports=all_imports,
                all_packages=all_packages,
                is_temporary=stage_location is None,
                replace=replace,
                inline_python_code=code,
                api_call_source=api_call_source,
            )
        # an exception might happen during registering a udf
        # (e.g., a dependency might not be found on the stage),
        # then for a permanent udf, we should delete the uploaded
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

        return UserDefinedFunction(func, return_type, input_types, udf_name)
