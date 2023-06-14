#!/usr/bin/env python3
#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

"""User-defined aggregate functions (UDAFs) in Snowpark. Refer to :class:`~snowflake.snowpark.udaf.UDAFRegistration` for details and sample code."""

import sys
from types import ModuleType

import snowflake.snowpark
from snowflake.connector import ProgrammingError
from snowflake.snowpark._internal.analyzer.expression import Expression, SnowflakeUDF
from snowflake.snowpark._internal.error_message import SnowparkClientExceptionMessages
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
)
from snowflake.snowpark.column import Column
from snowflake.snowpark.types import DataType

# Python 3.8 needs to use typing.Iterable because collections.abc.Iterable is not subscriptable
# Python 3.9 can use both
# Python 3.10 needs to use collections.abc.Iterable because typing.Iterable is removed
if sys.version_info <= (3, 9):
    from typing import Callable, Dict, Iterable, List, Optional, Tuple, Union
else:
    from collections.abc import Iterable


class UserDefinedAggregateFunction:
    """
    Encapsulates a user defined aggregate function that is returned by
    :func:`~snowflake.snowpark.functions.udaf`, :meth:`UDAFRegistration.register` or
    :meth:`UDAFRegistration.register_from_file`. The constructor of this class is not supposed
    to be called directly.

    Call an instance of :class:`UserDefinedAggregateFunction` to generate a
    :class:`~snowflake.snowpark.Column` instance. The input type can be
    a column name as a :class:`str`, or a :class:`~snowflake.snowpark.Column` object.

    See Also:
        - :class:`UDAFRegistration`
        - :func:`~snowflake.snowpark.functions.udaf`
    """

    def __init__(
        self,
        handler: Union[Callable, Tuple[str, str]],
        name: str,
        return_type: DataType,
        input_types: List[DataType],
        is_return_nullable: bool = False,
    ) -> None:
        #: The Python class or a tuple containing the Python file path and the function name.
        self.handler: Union[Callable, Tuple[str, str]] = handler
        #: The UDAF name.
        self.name: str = name

        self._return_type = return_type
        self._input_types = input_types
        self._is_return_nullable = is_return_nullable

    def __call__(
        self,
        *cols: Union[ColumnOrName, Iterable[ColumnOrName]],
    ) -> Column:
        exprs = []
        for c in parse_positional_args_to_list(*cols):
            if isinstance(c, Column):
                exprs.append(c._expression)
            elif isinstance(c, str):
                exprs.append(Column(c)._expression)
            else:
                raise TypeError(
                    f"The inputs of UDAF {self.name} must be Column or column name"
                )

        return Column(self._create_udf_expression(exprs))

    def _create_udf_expression(self, exprs: List[Expression]) -> SnowflakeUDF:
        if len(exprs) != len(self._input_types):
            raise ValueError(
                f"Incorrect number of arguments passed to the UDAF:"
                f" Expected: {len(self._input_types)}, Found: {len(exprs)}"
            )
        return SnowflakeUDF(
            self.name,
            exprs,
            self._return_type,
            nullable=self._is_return_nullable,
            api_call_source="UserDefinedAggregateFunction.__call__",
        )


class UDAFRegistration:
    """
    TODO: change link
    Provides methods to register lambdas and functions as UDAFs in the Snowflake database.
    For more information about Snowflake Python UDAFs, see `Python UDAFs <https://docs.snowflake.com/en/developer-guide/udf/python/udf-python.html>`__.

    :attr:`session.udaf <snowflake.snowpark.Session.udaf>` returns an object of this class.
    You can use this object to register UDAFs that you plan to use in the current session or
    permanently. The methods that register a UDAF return a :class:`UserDefinedAggregateFunction` object,
    which you can also use in :class:`~snowflake.snowpark.Column` expressions.

    Registering a UDAF is like registering a scalar UDF, you can use :meth:`register` or :func:`snowflake.snowpark.functions.udaf`
    to explicitly register it. You can also use the decorator `@udaf`. They all use ``cloudpickle`` to transfer the code from the client to the server.
    Another way is to use :meth:`register_from_file`. Refer to module :class:`snowflake.snowpark.udaf.UDAFRegistration` for when to use them.

    To query a registered UDAF is the same as to query other aggregate functions. Refer to :meth:`~snowflake.snowpark.DataFrame.agg`.
    If you want to query a UDAF right after it's created, you can call the created :class:`UserDefinedAggregateFunction` instance like in Example 1 below.

    TODO: Update examples
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

    def __init__(self, session: "snowflake.snowpark.session.Session") -> None:
        self._session = session

    def describe(
        self, udaf_obj: UserDefinedAggregateFunction
    ) -> "snowflake.snowpark.dataframe.DataFrame":
        """
        Returns a :class:`~snowflake.snowpark.DataFrame` that describes the properties of a UDAF.

        Args:
            udaf_obj: A :class:`UserDefinedAggregateFunction` returned by
                :func:`~snowflake.snowpark.functions.udaf` or :meth:`register`.
        """
        func_args = [convert_sp_to_sf_type(t) for t in udaf_obj._input_types]
        return self._session.sql(
            f"describe function {udaf_obj.name}({','.join(func_args)})"
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
        if_not_exists: bool = False,
        parallel: int = 4,
        strict: bool = False,
        secure: bool = False,
        *,
        statement_params: Optional[Dict[str, str]] = None,
        source_code_display: bool = True,
        **kwargs,
    ) -> UserDefinedAggregateFunction:
        """
        TODO: update doc
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
            if_not_exists: Whether to skip creation of a UDF when one with the same signature already exists.
                The default is ``False``. ``if_not_exists`` and ``replace`` are mutually exclusive
                and a ``ValueError`` is raised when both are set. If it is ``True`` and a UDF with
                the same signature exists, the UDF creation is skipped.
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
            strict: Whether the created UDF is strict. A strict UDF will not invoke the UDF if any input is
                null. Instead, a null value will always be returned for that row. Note that the UDF might
                still return null for non-null inputs.
            secure: Whether the created UDF is secure. For more information about secure functions,
                see `Secure UDFs <https://docs.snowflake.com/en/sql-reference/udf-secure.html>`_.
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
            TempObjectType.AGGREGATE_FUNCTION,
            name,
            is_permanent,
            stage_location,
            parallel,
        )

        # register udf
        return self._do_register_udaf(
            func,
            return_type,
            input_types,
            name,
            stage_location,
            imports,
            packages,
            replace,
            if_not_exists,
            parallel,
            strict,
            secure,
            statement_params=statement_params,
            source_code_display=source_code_display,
            api_call_source="UDAFRegistration.register",
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
        if_not_exists: bool = False,
        parallel: int = 4,
        strict: bool = False,
        secure: bool = False,
        *,
        statement_params: Optional[Dict[str, str]] = None,
        source_code_display: bool = True,
        skip_upload_on_content_match: bool = False,
    ) -> UserDefinedAggregateFunction:
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
            if_not_exists: Whether to skip creation of a UDF when one with the same signature already exists.
                The default is ``False``. ``if_not_exists`` and ``replace`` are mutually exclusive
                and a ``ValueError`` is raised when both are set. If it is ``True`` and a UDF with
                the same signature exists, the UDF creation is skipped.
            parallel: The number of threads to use for uploading UDF files with the
                `PUT <https://docs.snowflake.com/en/sql-reference/sql/put.html#put>`_
                command. The default value is 4 and supported values are from 1 to 99.
                Increasing the number of threads can improve performance when uploading
                large UDF files.
            strict: Whether the created UDF is strict. A strict UDF will not invoke the UDF if any input is
                null. Instead, a null value will always be returned for that row. Note that the UDF might
                still return null for non-null inputs.
            secure: Whether the created UDF is secure. For more information about secure functions,
                see `Secure UDFs <https://docs.snowflake.com/en/sql-reference/udf-secure.html>`_.
            statement_params: Dictionary of statement level parameters to be set while executing this action.
            source_code_display: Display the source code of the UDF `func` as comments in the generated script.
                The source code is dynamically generated therefore it may not be identical to how the
                `func` is originally defined. The default is ``True``.
                If it is ``False``, source code will not be generated or displayed.
            skip_upload_on_content_match: When set to ``True`` and a version of source file already exists on stage, the given source
                file will be uploaded to stage only if the contents of the current file differ from the remote file on stage. Defaults
                to ``False``.

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
            TempObjectType.AGGREGATE_FUNCTION,
            name,
            is_permanent,
            stage_location,
            parallel,
        )

        # register udf
        return self._do_register_udaf(
            (file_path, func_name),
            return_type,
            input_types,
            name,
            stage_location,
            imports,
            packages,
            replace,
            if_not_exists,
            parallel,
            strict,
            secure,
            statement_params=statement_params,
            source_code_display=source_code_display,
            api_call_source="UDAFRegistration.register_from_file",
            skip_upload_on_content_match=skip_upload_on_content_match,
        )

    def _do_register_udaf(
        self,
        func: Union[Callable, Tuple[str, str]],
        return_type: Optional[DataType],
        input_types: Optional[List[DataType]],
        name: Optional[str],
        stage_location: Optional[str] = None,
        imports: Optional[List[Union[str, Tuple[str, str]]]] = None,
        packages: Optional[List[Union[str, ModuleType]]] = None,
        replace: bool = False,
        if_not_exists: bool = False,
        parallel: int = 4,
        strict: bool = False,
        secure: bool = False,
        *,
        statement_params: Optional[Dict[str, str]] = None,
        source_code_display: bool = True,
        api_call_source: str,
        skip_upload_on_content_match: bool = False,
    ) -> UserDefinedAggregateFunction:
        # get the udaf name, return and input types
        (udaf_name, _, _, return_type, input_types,) = process_registration_inputs(
            self._session,
            TempObjectType.AGGREGATE_FUNCTION,
            func,
            return_type,
            input_types,
            name,
        )

        arg_names = [f"arg{i + 1}" for i in range(len(input_types))]
        input_args = [
            UDFColumn(dt, arg_name) for dt, arg_name in zip(input_types, arg_names)
        ]

        (
            handler,
            code,
            all_imports,
            all_packages,
            upload_file_stage_location,
        ) = resolve_imports_and_packages(
            self._session,
            TempObjectType.AGGREGATE_FUNCTION,
            func,
            arg_names,
            udaf_name,
            stage_location,
            imports,
            packages,
            parallel,
            statement_params=statement_params,
            source_code_display=source_code_display,
            skip_upload_on_content_match=skip_upload_on_content_match,
        )

        raised = False
        try:
            create_python_udf_or_sp(
                session=self._session,
                return_type=return_type,
                input_args=input_args,
                handler=handler,
                object_type=TempObjectType.AGGREGATE_FUNCTION,
                object_name=udaf_name,
                all_imports=all_imports,
                all_packages=all_packages,
                is_temporary=stage_location is None,
                replace=replace,
                if_not_exists=if_not_exists,
                inline_python_code=code,
                api_call_source=api_call_source,
                strict=strict,
                secure=secure,
            )
        # an exception might happen during registering a udaf
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

        return UserDefinedAggregateFunction(func, udaf_name, return_type, input_types)
