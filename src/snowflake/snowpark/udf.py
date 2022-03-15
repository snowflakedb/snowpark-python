#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#
"""User-defined functions (UDFs) in Snowpark."""
from logging import getLogger
from types import ModuleType
from typing import Callable, Iterable, List, Optional, Tuple, Union

import snowflake.snowpark
from snowflake.snowpark._internal.sp_expressions import (
    Expression as SPExpression,
    SnowflakeUDF,
)
from snowflake.snowpark._internal.type_utils import ColumnOrName, convert_to_sf_type
from snowflake.snowpark._internal.udf_utils import (
    UDFColumn,
    check_register_args,
    cleanup_failed_permanent_registration,
    create_python_udf_or_sp,
    process_file_path,
    process_registration_inputs,
    resolve_imports_and_packages,
)
from snowflake.snowpark._internal.utils import TempObjectType, Utils
from snowflake.snowpark.column import Column
from snowflake.snowpark.types import DataType

logger = getLogger(__name__)

# the default handler name for generated udf python file
_DEFAULT_HANDLER_NAME = "compute"

# Max code size to inline generated closure. Beyond this threshold, the closure will be uploaded to a stage for imports.
# Current number is the same as scala. We might have the potential to make it larger but that requires further benchmark
# because zip compression ratio is quite high.
_MAX_INLINE_CLOSURE_SIZE_BYTES = 8192


class UserDefinedFunction:
    """
    Encapsulates a user defined lambda or function that is returned by
    :func:`~snowflake.snowpark.functions.udf` or by :func:`UDFRegistration.register`.
    The constructor of this class is not supposed to be called directly.

    Call an instance of :class:`UserDefinedFunction` to generate
    :class:`~snowflake.snowpark.Column` expressions. The input type can be
    a column name as a :class:`str`, or a :class:`~snowflake.snowpark.Column` object.

    Examples::

        from snowflake.snowpark.functions import udf

        # Create an instance of UserDefinedFunction using the @udf decorator
        @udf
        def add_udf(x: int, y: int) -> int:
            return x + y

        def double(x: int) -> int:
            return 2 * x

        # Create an instance of UserDefinedFunction using the udf() function
        double_udf = udf(double)

        # call UDFs on a dataframe
        df.select(add_udf("a", "b"), double_udf(col("a")), double_udf(df["b"]))
    """

    def __init__(
        self,
        func: Union[Callable, Tuple[str, str]],
        return_type: DataType,
        input_types: List[DataType],
        name: str,
        is_return_nullable: bool = False,
    ):
        #: The Python function or a tuple containing the Python file path and the function name.
        self.func: Union[Callable, Tuple[str, str]] = func
        #: The UDF name.
        self.name: str = name

        self._return_type = return_type
        self._input_types = input_types
        self._is_return_nullable = is_return_nullable

    def __call__(
        self,
        *cols: Union[ColumnOrName, List[ColumnOrName], Tuple[ColumnOrName, ...]],
    ) -> Column:
        exprs = []
        for c in Utils.parse_positional_args_to_list(*cols):
            if isinstance(c, Column):
                exprs.append(c.expression)
            elif isinstance(c, str):
                exprs.append(Column(c).expression)
            else:
                raise TypeError(
                    f"The input of UDF {self.name} must be Column, column name, or a list of them"
                )

        return Column(self.__create_udf_expression(exprs))

    def __create_udf_expression(self, exprs: List[SPExpression]) -> SnowflakeUDF:
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
        )


class UDFRegistration:
    """
    Provides methods to register lambdas and functions as UDFs in the Snowflake database.
    For more information about Snowflake Python UDFs, see `Python UDFs <https://docs.snowflake.com/en/LIMITEDACCESS/udf-python.html>`__.

    :attr:`session.udf <snowflake.snowpark.Session.udf>` returns an object of this class.
    You can use this object to register UDFs that you plan to use in the current session or permanently.
    The methods that register a UDF return a :class:`UserDefinedFunction` object,
    which you can also use in :class:`~snowflake.snowpark.Column` expressions.

    Examples::

        def double(x: int) -> int:
            return 2 * x

        double_udf = session.udf.register(double, name="mydoubleudf")
        session.sql(s"SELECT mydoubleudf(c) FROM table")
        df.select(double_udf("c"))

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
        :class:`~snowflake.snowpark.types.PandasDataFrameType` are used when creating a Pandas UDF,
        so they are not mapped to any SQL types. ``element_type`` in
        :class:`~snowflake.snowpark.types.PandasSeriesType` and ``col_types`` in
        :class:`~snowflake.snowpark.types.PandasDataFrameType` indicate the SQL types
        in a Pandas Series and a Panda DataFrame.

    See Also:
        :func:`~snowflake.snowpark.functions.udf`
    """

    def __init__(self, session: "snowflake.snowpark.Session"):
        self._session = session

    def describe(self, udf_obj: UserDefinedFunction) -> "snowflake.snowpark.DataFrame":
        """
        Returns a :class:`~snowflake.snowpark.DataFrame` that describes the properties of a UDF.

        Args:
            udf_obj: A :class:`UserDefinedFunction` returned by
                :func:`~snowflake.snowpark.functions.udf` or :meth:`register`.
        """
        func_args = [convert_to_sf_type(t) for t in udf_obj._input_types]
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
        _from_pandas_udf_function: bool = False,
    ) -> UserDefinedFunction:
        """
        Registers a Python function as a Snowflake Python UDF and returns the UDF.
        The usage, input arguments, and return value of this method are the same as
        they are for :func:`~snowflake.snowpark.functions.udf`, but :meth:`register`
        cannot be used as a decorator.

        See Also:
            :func:`~snowflake.snowpark.functions.udf`
        """
        if not callable(func):
            raise TypeError(
                "Invalid function: not a function or callable "
                f"(__call__ is not defined): {type(func)}"
            )

        check_register_args(
            TempObjectType.FUNCTION, name, is_permanent, stage_location, parallel
        )

        # register udf
        return self.__do_register_udf(
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
            from_pandas_udf_function=_from_pandas_udf_function,
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
    ) -> UserDefinedFunction:
        """
        Registers a Python function as a Snowflake Python UDF from a Python or zip file,
        and returns the UDF. Apart from ``file_path`` and ``func_name``, the input arguments
        of this method are the same as :meth:`register`.

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

        Example::

            # "my_double.py" contains a function "double":
            # def double(x: int) -> int:
            #     return 2 * x
            double_udf = session.udf.register_from_file("my_double.py", "double", name="mydoubleudf")

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
        return self.__do_register_udf(
            (file_path, func_name),
            return_type,
            input_types,
            name,
            stage_location,
            imports,
            packages,
            replace,
            parallel,
        )

    def __do_register_udf(
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
                "You cannot create a non-Pandas UDF using pandas_udf(). "
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
        )

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
            )
        # an exception might happen during registering a stored procedure
        # (e.g., a dependency might not be found on the stage),
        # then for a permanent stored procedure, we should delete the uploaded
        # python file and raise the exception
        except BaseException:
            cleanup_failed_permanent_registration(
                self._session, upload_file_stage_location, stage_location
            )
            raise

        return UserDefinedFunction(func, return_type, input_types, udf_name)
