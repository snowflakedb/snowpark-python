#!/usr/bin/env python3
#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#
"""User-defined table functions (UDTFs) in Snowpark."""
import collections.abc
import sys
from types import ModuleType
from typing import (
    Callable,
    Dict,
    Iterable,
    List,
    Optional,
    Tuple,
    Type,
    Union,
    get_args,
    get_origin,
    get_type_hints,
)

import snowflake.snowpark
from snowflake.connector import ProgrammingError
from snowflake.snowpark._internal import type_utils
from snowflake.snowpark._internal.error_message import SnowparkClientExceptionMessages
from snowflake.snowpark._internal.telemetry import TelemetryField
from snowflake.snowpark._internal.type_utils import (
    ColumnOrName,
    python_type_str_to_object,
    retrieve_func_type_hints_from_source,
)
from snowflake.snowpark._internal.udf_utils import (
    TABLE_FUNCTION_PROCESS_METHOD,
    UDFColumn,
    check_register_args,
    cleanup_failed_permanent_registration,
    create_python_udf_or_sp,
    process_file_path,
    process_registration_inputs,
    resolve_imports_and_packages,
)
from snowflake.snowpark._internal.utils import TempObjectType, validate_object_name
from snowflake.snowpark.table_function import TableFunctionCall
from snowflake.snowpark.types import DataType, StructField, StructType


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
        output_schema: StructType,
        input_types: List[DataType],
        name: str,
    ) -> None:
        #: The Python class or a tuple containing the Python file path and the function name.
        self.handler: Union[Callable, Tuple[str, str]] = handler
        #: The UDTF name.
        self.name: str = name

        self._output_schema = output_schema
        self._input_types = input_types

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

    See Also:
        - :func:`~snowflake.snowpark.functions.udtf`
        - :meth:`register`
        - :meth:`register_from_file`
        - :meth:`~snowflake.snowpark.Session.add_import`
        - :meth:`~snowflake.snowpark.Session.add_packages`
        - :meth:`~snowflake.snowpark.Session.table_function`
        - :meth:`~snowflake.snowpark.DataFrame.join_table_function`
    """

    def __init__(self, session: "snowflake.snowpark.Session") -> None:
        self._session = session

    def register(
        self,
        handler: Type,
        output_schema: Union[StructType, Iterable[str]],
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
    ) -> UserDefinedTableFunction:
        """
        Registers a Python class as a Snowflake Python UDTF and returns the UDTF.
        The usage, input arguments, and return value of this method are the same as
        they are for :func:`~snowflake.snowpark.functions.udtf`, but :meth:`register`
        cannot be used as a decorator. See examples in
        :class:`~snowflake.snowpark.udtf.UDTFRegistration`.

        Args:
            handler: A Python class used for creating the UDTF.
            output_schema: A list of column names, or a :class:`~snowflake.snowpark.types.StructType` instance that represents the table function's columns.
             If a list of column names is provided, the ``process`` method of the handler class must have a return type hint to indicate the output schema data types.
            input_types: A list of :class:`~snowflake.snowpark.types.DataType`
                representing the input data types of the UDTF. Optional if
                type hints are provided.
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
                :meth:`~snowflake.snowpark.Session.add_requirements`.
            replace: Whether to replace a UDTF that already was registered. The default is ``False``.
                If it is ``False``, attempting to register a UDTF with a name that already exists
                results in a ``SnowparkSQLException`` exception being thrown. If it is ``True``,
                an existing UDTF with the same name is overwritten.
            session: Use this session to register the UDTF. If it's not specified, the session that you created before calling this function will be used.
                You need to specify this parameter if you have created multiple sessions before calling this method.
            parallel: The number of threads to use for uploading UDTF files with the
                `PUT <https://docs.snowflake.com/en/sql-reference/sql/put.html#put>`_
                command. The default value is 4 and supported values are from 1 to 99.
                Increasing the number of threads can improve performance when uploading
                large UDTF files.
            statement_params: Dictionary of statement level parameters to be set while executing this action.

        See Also:
            - :func:`~snowflake.snowpark.functions.udtf`
            - :meth:`register_from_file`
        """
        if not callable(handler):
            raise TypeError(
                "Invalid function: not a function or callable "
                f"(__call__ is not defined): {type(handler)}"
            )

        check_register_args(
            TempObjectType.TABLE_FUNCTION, name, is_permanent, stage_location, parallel
        )

        # register udtf
        return self._do_register_udtf(
            handler,
            output_schema,
            input_types,
            name,
            stage_location,
            imports,
            packages,
            replace,
            parallel,
            statement_params=statement_params,
            api_call_source="UDTFRegistration.register",
        )

    def register_from_file(
        self,
        file_path: str,
        handler_name: str,
        output_schema: Union[StructType, Iterable[str]],
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
            output_schema: A list of column names, or a :class:`~snowflake.snowpark.types.StructType` instance that represents the table function's columns.
            input_types: A list of :class:`~snowflake.snowpark.types.DataType`
                representing the input data types of the UDTF. Optional if
                type hints are provided.
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
                :meth:`~snowflake.snowpark.Session.add_requirements`.
            replace: Whether to replace a UDTF that already was registered. The default is ``False``.
                If it is ``False``, attempting to register a UDTF with a name that already exists
                results in a ``SnowparkSQLException`` exception being thrown. If it is ``True``,
                an existing UDTF with the same name is overwritten.
            session: Use this session to register the UDTF. If it's not specified, the session that you created before calling this function will be used.
                You need to specify this parameter if you have created multiple sessions before calling this method.
            parallel: The number of threads to use for uploading UDTF files with the
                `PUT <https://docs.snowflake.com/en/sql-reference/sql/put.html#put>`_
                command. The default value is 4 and supported values are from 1 to 99.
                Increasing the number of threads can improve performance when uploading
                large UDTF files.
            statement_params: Dictionary of statement level parameters to be set while executing this action.

        Note::
            The type hints can still be extracted from the source Python file if they
            are provided, but currently are not working for a zip file. Therefore,
            you have to provide ``output_schema`` and ``input_types`` when ``path``
            points to a zip file.

        See Also:
            - :func:`~snowflake.snowpark.functions.udtf`
            - :meth:`register`
        """
        file_path = process_file_path(file_path)
        check_register_args(
            TempObjectType.TABLE_FUNCTION, name, is_permanent, stage_location, parallel
        )

        # register udtf
        return self._do_register_udtf(
            (file_path, handler_name),
            output_schema,
            input_types,
            name,
            stage_location,
            imports,
            packages,
            replace,
            parallel,
            statement_params=statement_params,
            api_call_source="UDTFRegistration.register_from_file",
        )

    def _do_register_udtf(
        self,
        handler: Union[Callable, Tuple[str, str]],
        output_schema: Union[StructType, Iterable[str]],
        input_types: Optional[List[DataType]],
        name: Optional[str],
        stage_location: Optional[str] = None,
        imports: Optional[List[Union[str, Tuple[str, str]]]] = None,
        packages: Optional[List[Union[str, ModuleType]]] = None,
        replace: bool = False,
        parallel: int = 4,
        *,
        statement_params: Optional[Dict[str, str]] = None,
        api_call_source: str,
    ) -> UserDefinedTableFunction:
        if not isinstance(output_schema, (Iterable, StructType)):
            raise ValueError(
                f"'output_schema' must be a list of column names or StructType instance to create a UDTF. Got {type(output_schema)}."
            )
        if isinstance(output_schema, StructType):
            _validate_output_schema_names(output_schema.names)
        if isinstance(
            output_schema, Iterable
        ):  # with column names instead of StructType. Read type hints to infer column types.
            output_schema = tuple(output_schema)
            _validate_output_schema_names(output_schema)
            # A typical type hint for method process is like Iterable[Tuple[int, str, datetime]], or Iterable[Tuple[str, ...]]
            # The inner Tuple is a single row of the table function result.
            if isinstance(handler, Callable):
                type_hints = get_type_hints(
                    getattr(handler, TABLE_FUNCTION_PROCESS_METHOD)
                )
                return_type_hint = type_hints.get("return")
            else:
                type_hints = retrieve_func_type_hints_from_source(
                    handler[0],
                    func_name=TABLE_FUNCTION_PROCESS_METHOD,
                    class_name=handler[1],
                )
                return_type_hint = python_type_str_to_object(type_hints.get("return"))
            if not return_type_hint:
                raise ValueError(
                    "The return type hint is not set but 'output_schema' has only column names. You can either use a StructType instance for 'output_schema', or use"
                    "a combination of a return type hint for method 'process' and column names for 'output_schema'."
                )
            if get_origin(return_type_hint) not in (
                list,
                tuple,
                collections.abc.Iterable,
                collections.abc.Iterator,
            ):
                raise ValueError(
                    f"The return type hint for a UDTF handler must but a collection type. {return_type_hint} is used."
                )
            row_type_hint = get_args(return_type_hint)[0]  # The inner Tuple
            if get_origin(row_type_hint) != tuple:
                raise ValueError(
                    f"The return type hint of method '{handler.__name__}.process' must be a collection of tuples, for instance, Iterable[Tuple[str, int]], if you specify return type hint."
                )
            column_type_hints = get_args(row_type_hint)
            if len(column_type_hints) > 1 and column_type_hints[1] == Ellipsis:
                output_schema = StructType(
                    [
                        StructField(
                            name,
                            type_utils.python_type_to_snow_type(column_type_hints[0])[
                                0
                            ],
                        )
                        for name in output_schema
                    ]
                )
            else:
                if len(column_type_hints) != len(output_schema):
                    raise ValueError(
                        f"'output_schema' has {len(output_schema)} names while type hints Tuple has only {len(column_type_hints)}."
                    )
                output_schema = StructType(
                    [
                        StructField(
                            name,
                            type_utils.python_type_to_snow_type(column_type)[0],
                        )
                        for name, column_type in zip(output_schema, column_type_hints)
                    ]
                )

        # get the udtf name, input types
        (udtf_name, _, _, _, input_types,) = process_registration_inputs(
            self._session,
            TempObjectType.TABLE_FUNCTION,
            handler,
            output_schema,
            input_types,
            name,
        )

        arg_names = [f"arg{i + 1}" for i in range(len(input_types))]
        input_args = [
            UDFColumn(dt, arg_name) for dt, arg_name in zip(input_types, arg_names)
        ]
        (
            handler_name,
            code,
            all_imports,
            all_packages,
            upload_file_stage_location,
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
            False,
            False,
            statement_params=statement_params,
        )

        raised = False
        try:
            create_python_udf_or_sp(
                session=self._session,
                return_type=output_schema,
                input_args=input_args,
                handler=handler_name,
                object_type=TempObjectType.FUNCTION,
                object_name=udtf_name,
                all_imports=all_imports,
                all_packages=all_packages,
                is_temporary=stage_location is None,
                replace=replace,
                inline_python_code=code,
                api_call_source=api_call_source,
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

        return UserDefinedTableFunction(handler, output_schema, input_types, udtf_name)


def _validate_output_schema_names(names: Iterable[str]) -> None:
    for name in names:
        validate_object_name(name)
