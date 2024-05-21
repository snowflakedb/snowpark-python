#!/usr/bin/env python3
#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

"""Stored procedures in Snowpark. Refer to :class:`~snowflake.snowpark.stored_procedure.StoredProcedure` for details and sample code."""
import sys
import typing
from types import ModuleType
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

import snowflake.snowpark
from snowflake.connector import ProgrammingError
from snowflake.snowpark._internal.analyzer.analyzer_utils import result_scan_statement
from snowflake.snowpark._internal.error_message import SnowparkClientExceptionMessages
from snowflake.snowpark._internal.telemetry import TelemetryField
from snowflake.snowpark._internal.type_utils import convert_sp_to_sf_type
from snowflake.snowpark._internal.udf_utils import (
    UDFColumn,
    add_snowpark_package_to_sproc_packages,
    check_execute_as_arg,
    check_python_runtime_version,
    check_register_args,
    cleanup_failed_permanent_registration,
    create_python_udf_or_sp,
    generate_anonymous_python_sp_sql,
    generate_call_python_sp_sql,
    process_file_path,
    process_registration_inputs,
    resolve_imports_and_packages,
)
from snowflake.snowpark._internal.utils import TempObjectType
from snowflake.snowpark.types import DataType, StructType

# Python 3.8 needs to use typing.Iterable because collections.abc.Iterable is not subscriptable
# Python 3.9 can use both
# Python 3.10 needs to use collections.abc.Iterable because typing.Iterable is removed
if sys.version_info <= (3, 9):
    from typing import Iterable
else:
    from collections.abc import Iterable


class StoredProcedure:
    """
    Encapsulates a user defined lambda or function that is returned by
    :func:`~snowflake.snowpark.functions.sproc`, :meth:`StoredProcedureRegistration.register`
    or :meth:`StoredProcedureRegistration.register_from_file`. The constructor
    of this class is not supposed to be called directly.

    Call an instance of :class:`StoredProcedure` to invoke a stored procedure.
    The input should be Python literal values.

    See Also:
        - :class:`StoredProcedureRegistration`
        - :func:`~snowflake.snowpark.functions.sproc`
    """

    def __init__(
        self,
        func: Callable,
        return_type: DataType,
        input_types: List[DataType],
        name: str,
        execute_as: typing.Literal["caller", "owner"] = "owner",
        anonymous_sp_sql: Optional[str] = None,
        packages: Optional[List[Union[str, ModuleType]]] = None,
    ) -> None:
        #: The Python function.
        self.func: Callable = func
        #: The stored procedure name.
        self.name: str = name

        self._return_type = return_type
        self._input_types = input_types
        self._execute_as = execute_as
        self._anonymous_sp_sql = anonymous_sp_sql
        self._is_return_table = isinstance(return_type, StructType)

        self._packages = packages

    def _validate_call(
        self,
        args: List[Any],
        session: Optional["snowflake.snowpark.session.Session"] = None,
    ):
        if args and isinstance(args[0], snowflake.snowpark.session.Session):
            if session:
                raise ValueError(
                    "Two sessions specified in arguments. Session should either be the first argument or as "
                    "a named argument at the end, but not both"
                )
            session = args[0]
            args = args[1:]
        else:
            session = session or snowflake.snowpark.session._get_active_session()

        if len(self._input_types) != len(args):
            raise ValueError(
                f"Incorrect number of arguments passed to the stored procedure. Expected: {len(self._input_types)}, Found: {len(args)}"
            )

        return args, session

    def __call__(
        self,
        *args: Any,
        session: Optional["snowflake.snowpark.session.Session"] = None,
        statement_params: Optional[Dict[str, str]] = None,
    ) -> Any:
        args, session = self._validate_call(args, session)

        session._conn._telemetry_client.send_function_usage_telemetry(
            "StoredProcedure.__call__", TelemetryField.FUNC_CAT_USAGE.value
        )

        if self._anonymous_sp_sql:
            call_sql = generate_call_python_sp_sql(session, self.name, *args)
            query = f"{self._anonymous_sp_sql}{call_sql}"
            if self._is_return_table:
                qid = session._conn.execute_and_get_sfqid(
                    query, statement_params=statement_params
                )
                df = session.sql(result_scan_statement(qid))
                return df
            df = session.sql(query)
            return df._internal_collect_with_tag(statement_params=statement_params)[0][
                0
            ]
        else:
            return session._call(
                self.name,
                *args,
                is_return_table=self._is_return_table,
                statement_params=statement_params,
            )


class StoredProcedureRegistration:
    """
    Provides methods to register lambdas and functions as stored procedures in the Snowflake database.
    For more information about Snowflake Python stored procedures, see `Python stored procedures <https://docs.snowflake.com/en/sql-reference/stored-procedures-python.html>`__.

    :attr:`session.sproc <snowflake.snowpark.Session.sproc>` returns an object of this class.
    You can use this object to register stored procedures that you plan to use in the current session or
    permanently. The methods that register a stored procedure return a :class:`StoredProcedure` object.

    Note that the first parameter of your function should be a snowpark Session. Also, you need to add
    `snowflake-snowpark-python` package (version >= 0.4.0) to your session before trying to create a
    stored procedure.

    There are two ways to register a stored procedure with Snowpark:

        - Use :func:`~snowflake.snowpark.functions.sproc` or :meth:`register`. By pointing to a
          `runtime Python function`, Snowpark uses `cloudpickle <https://github.com/cloudpipe/cloudpickle>`_
          to serialize this function to bytecode, and deserialize the bytecode to a Python
          function on the Snowflake server during stored procedure creation. During the serialization, the
          global variables used in the Python function will be serialized into the bytecode,
          but only the name of the module object or any objects from a module that are used in the
          Python function will be serialized. If the size of the serialized bytecode is over 8K bytes, it will be uploaded to a stage location as a Python file.
          If it's under 8K, it will be added to the `Stored Procedure in-line code <https://docs.snowflake.com/en/sql-reference/stored-procedures-python.html#choosing-to-create-a-stored-procedure-with-in-line-code-or-with-code-uploaded-from-a-stage>`__.

          During the deserialization, Python will look up the
          corresponding modules and objects by names.

          Details could be found in :class:`snowflake.snowpark.udf.UDFRegistration`.

        - Use :meth:`register_from_file`. By pointing to a `Python file` or a `zip file containing
          Python source code` and the target function name, Snowpark uploads this file to a stage
          (which can also be customized), and load the corresponding function from this file to
          the Python runtime on the Snowflake server during stored procedure creation. Then this
          function will be invoked when calling this stored procedure. This approach can address
          the deficiency of the previous approach that uses cloudpickle, because the source code
          in this file other than the target function will be loaded during stored procedure creation.
          Therefore, this approach is useful and efficient when all your Python code is already in
          source files.

    Snowflake supports the following data types for the parameters for a stored procedure:

    =============================================  ================================================  =========
    Python Type                                    Snowpark Type                                     SQL Type
    =============================================  ================================================  =========
    ``int``                                        :class:`~snowflake.snowpark.types.LongType`       NUMBER
    ``decimal.Decimal``                            :class:`~snowflake.snowpark.types.DecimalType`    NUMBER
    ``float``                                      :class:`~snowflake.snowpark.types.FloatType`      FLOAT
    ``str``                                        :class:`~snowflake.snowpark.types.StringType`     STRING
    ``bool``                                       :class:`~snowflake.snowpark.types.BooleanType`    BOOL
    ``datetime.time``                              :class:`~snowflake.snowpark.types.TimeType`       TIME
    ``datetime.date``                              :class:`~snowflake.snowpark.types.DateType`       DATE
    ``datetime.datetime``                          :class:`~snowflake.snowpark.types.TimestampType`  TIMESTAMP
    ``bytes`` or ``bytearray``                     :class:`~snowflake.snowpark.types.BinaryType`     BINARY
    ``list``                                       :class:`~snowflake.snowpark.types.ArrayType`      ARRAY
    ``dict``                                       :class:`~snowflake.snowpark.types.MapType`        OBJECT
    Dynamically mapped to the native Python type   :class:`~snowflake.snowpark.types.VariantType`    VARIANT
    ``dict``                                       :class:`~snowflake.snowpark.types.GeographyType`  GEOGRAPHY
    =============================================  ================================================  =========

    Note:
        1. Data with the VARIANT SQL type will be converted to a Python type
        dynamically inside a stored procedure. The following SQL types are converted to :class:`str`
        in stored procedures rather than native Python types: TIME, DATE, TIMESTAMP and BINARY.

        2. Data returned as :class:`~snowflake.snowpark.types.ArrayType` (``list``),
        :class:`~snowflake.snowpark.types.MapType` (``dict``) or
        :class:`~snowflake.snowpark.types.VariantType` (:attr:`~snowflake.snowpark.types.Variant`)
        by a stored procedure will be represented as a json string. You can call ``eval()`` or ``json.loads()``
        to convert the result to a native Python object. Data returned as
        :class:`~snowflake.snowpark.types.GeographyType` (:attr:`~snowflake.snowpark.types.Geography`)
        by a stored procedure will be represented as a `GeoJSON <https://datatracker.ietf.org/doc/html/rfc7946>`_
        string.

        3. Currently calling stored procedure that requires VARIANT and GEOGRAPHY input types is not supported
        in snowpark API.

        4. Dataframe returned from :meth:`~snowflake.snowpark.Session.call` does not support stacking dataframe
        operations when sql simplifier is disabled, and output columns in return type for the table stored
        procedure are not defined.

    Example 1
        Use stored procedure to copy data from one table to another::

            >>> import snowflake.snowpark
            >>> from snowflake.snowpark.functions import sproc
            >>>
            >>> session.add_packages('snowflake-snowpark-python')
            >>>
            >>> def my_copy(session: snowflake.snowpark.Session, from_table: str, to_table: str, count: int) -> str:
            ...     session.table(from_table).limit(count).write.save_as_table(to_table)
            ...     return "SUCCESS"
            >>>
            >>> my_copy_sp = session.sproc.register(my_copy, name="my_copy_sp", replace=True)
            >>> _ = session.sql("create or replace temp table test_from(test_str varchar) as select randstr(20, random()) from table(generator(rowCount => 100))").collect()
            >>>
            >>> # call using sql
            >>> _ = session.sql("drop table if exists test_to").collect()
            >>> session.sql("call my_copy_sp('test_from', 'test_to', 10)").collect()
            [Row(MY_COPY_SP='SUCCESS')]
            >>> session.table("test_to").count()
            10
            >>> # call using session#call API
            >>> _ = session.sql("drop table if exists test_to").collect()
            >>> session.call("my_copy_sp", "test_from", "test_to", 10)
            'SUCCESS'
            >>> session.table("test_to").count()
            10

    Example 2
        Create a temporary stored procedure from a lambda and call it::

            >>> from snowflake.snowpark.functions import sproc
            >>> from snowflake.snowpark.types import IntegerType
            >>>
            >>> session.add_packages('snowflake-snowpark-python')
            >>> add_one_sp = sproc(
            ...     lambda session_, x: session_.sql(f"select {x} + 1").collect()[0][0],
            ...     return_type=IntegerType(),
            ...     input_types=[IntegerType()]
            ... )
            >>> add_one_sp(1)
            2

    Example 3
        Create a stored procedure with type hints and ``@sproc`` decorator and call it::

            >>> import snowflake.snowpark
            >>> from snowflake.snowpark.functions import sproc
            >>>
            >>> session.add_packages('snowflake-snowpark-python')
            >>> @sproc
            ... def add_sp(session_: snowflake.snowpark.Session, x: int, y: int) -> int:
            ...    return session_.sql(f"select {x} + {y}").collect()[0][0]
            >>> add_sp(1, 2)
            3

    Example 4
        Create a permanent stored procedure with a name and call it in SQL::

            >>> from snowflake.snowpark.types import IntegerType
            >>>
            >>> session.add_packages('snowflake-snowpark-python')
            >>> _ = session.sql("create or replace temp stage mystage").collect()
            >>> _ = session.sproc.register(
            ...     lambda session_, x, y: session_.sql(f"SELECT {x} * {y}").collect()[0][0],
            ...     return_type=IntegerType(),
            ...     input_types=[IntegerType(), IntegerType()],
            ...     is_permanent=True,
            ...     name="mul_sp",
            ...     replace=True,
            ...     stage_location="@mystage",
            ... )
            >>> session.sql("call mul_sp(5, 6)").collect()
            [Row(MUL_SP=30)]
            >>> # skip stored proc creation if it already exists
            >>> _ = session.sproc.register(
            ...     lambda session_, x, y: session_.sql(f"SELECT {x} * {y} + 1").collect()[0][0],
            ...     return_type=IntegerType(),
            ...     input_types=[IntegerType(), IntegerType()],
            ...     is_permanent=True,
            ...     name="mul_sp",
            ...     if_not_exists=True,
            ...     stage_location="@mystage",
            ... )
            >>> session.sql("call mul_sp(5, 6)").collect()
            [Row(MUL_SP=30)]
            >>> # overwrite stored procedure
            >>> _ = session.sproc.register(
            ...     lambda session_, x, y: session_.sql(f"SELECT {x} * {y} + 1").collect()[0][0],
            ...     return_type=IntegerType(),
            ...     input_types=[IntegerType(), IntegerType()],
            ...     is_permanent=True,
            ...     name="mul_sp",
            ...     replace=True,
            ...     stage_location="@mystage",
            ... )
            >>> session.sql("call mul_sp(5, 6)").collect()
            [Row(MUL_SP=31)]

    Example 5
        Create a stored procedure with stored-procedure-level imports and call it::

            >>> import snowflake.snowpark
            >>> from resources.test_sp_dir.test_sp_file import mod5
            >>> from snowflake.snowpark.functions import sproc
            >>>
            >>> session.add_packages('snowflake-snowpark-python')
            >>> @sproc(imports=[("tests/resources/test_sp_dir/test_sp_file.py", "resources.test_sp_dir.test_sp_file")])
            ... def mod5_and_plus1_sp(session_: snowflake.snowpark.Session, x: int) -> int:
            ...     return mod5(session_, x) + 1
            >>> mod5_and_plus1_sp(2)
            3

    Example 6
        Create a stored procedure with stored-procedure-level packages and call it::

            >>> import snowflake.snowpark
            >>> from snowflake.snowpark.functions import sproc
            >>> import numpy as np
            >>> import math
            >>>
            >>> @sproc(packages=["snowflake-snowpark-python", "numpy"])
            ... def sin_sp(_: snowflake.snowpark.Session, x: float) -> float:
            ...     return np.sin(x)
            >>> sin_sp(0.5 * math.pi)
            1.0

    Example 7
        Creating a stored procedure from a local Python file::

            >>> session.add_packages('snowflake-snowpark-python')
            >>> # mod5() in that file has type hints
            >>> mod5_sp = session.sproc.register_from_file(
            ...     file_path="tests/resources/test_sp_dir/test_sp_file.py",
            ...     func_name="mod5",
            ... )
            >>> mod5_sp(2)
            2

    Example 8
        Creating a stored procedure from a Python file on an internal stage::

            >>> from snowflake.snowpark.types import IntegerType
            >>>
            >>> session.add_packages('snowflake-snowpark-python')
            >>> _ = session.sql("create or replace temp stage mystage").collect()
            >>> _ = session.file.put("tests/resources/test_sp_dir/test_sp_file.py", "@mystage", auto_compress=False)
            >>> mod5_sp = session.sproc.register_from_file(
            ...     file_path="@mystage/test_sp_file.py",
            ...     func_name="mod5",
            ...     return_type=IntegerType(),
            ...     input_types=[IntegerType()],
            ... )
            >>> mod5_sp(2)
            2

    Example 9
        Creating a table stored procedure with return type while defining return columns and datatypes::

            >>> from snowflake.snowpark.types import IntegerType, StructField, StructType
            >>> @sproc(return_type=StructType([StructField("A", IntegerType()), StructField("B", IntegerType())]), input_types=[IntegerType(), IntegerType()])
            ... def select_sp(session_, x, y):
            ...     return session_.sql(f"SELECT {x} as A, {y} as B")
            ...
            >>> select_sp(1, 2).show()
            -------------
            |"A"  |"B"  |
            -------------
            |1    |2    |
            -------------
            <BLANKLINE>

    Example 10
        Creating a table stored procedure with return type with free return columns::

            >>> from snowflake.snowpark.types import IntegerType, StructType
            >>> @sproc(return_type=StructType(), input_types=[IntegerType(), IntegerType()])
            ... def select_sp(session_, x, y):
            ...     return session_.sql(f"SELECT {x} as A, {y} as B")
            ...
            >>> select_sp(1, 2).show()
            -------------
            |"A"  |"B"  |
            -------------
            |1    |2    |
            -------------
            <BLANKLINE>

    Example 9
        Creating a table stored procedure using implicit type hints::

            >>> from snowflake.snowpark.dataframe import DataFrame
            >>> @sproc
            ... def select_sp(session_: snowflake.snowpark.Session, x: int, y: int) -> DataFrame:
            ...     return session_.sql(f"SELECT {x} as A, {y} as B")
            ...
            >>> select_sp(1, 2).show()
            -------------
            |"A"  |"B"  |
            -------------
            |1    |2    |
            -------------
            <BLANKLINE>

    See Also:
        - :class:`snowflake.snowpark.udf.UDFRegistration`
        - :func:`~snowflake.snowpark.functions.sproc`
        - :meth:`register`
        - :meth:`register_from_file`
        - :meth:`~snowflake.snowpark.Session.add_import`
        - :meth:`~snowflake.snowpark.Session.add_packages`
    """

    def __init__(self, session: Optional["snowflake.snowpark.session.Session"]) -> None:
        self._session = session

    def describe(
        self, sproc_obj: StoredProcedure
    ) -> "snowflake.snowpark.dataframe.DataFrame":
        """
        Returns a :class:`~snowflake.snowpark.DataFrame` that describes the properties of a stored procedure.

        Args:
            sproc_obj: A :class:`StoredProcedure` returned by
                :func:`~snowflake.snowpark.functions.sproc` or :meth:`register`.
        """
        func_args = [convert_sp_to_sf_type(t) for t in sproc_obj._input_types]
        return self._session.sql(
            f"describe procedure {sproc_obj.name}({','.join(func_args)})"
        )

    def register(
        self,
        func: Union[Callable, Tuple[str, str]],
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
        execute_as: typing.Literal["caller", "owner"] = "owner",
        strict: bool = False,
        external_access_integrations: Optional[List[str]] = None,
        secrets: Optional[Dict[str, str]] = None,
        comment: Optional[str] = None,
        *,
        statement_params: Optional[Dict[str, str]] = None,
        source_code_display: bool = True,
        **kwargs,
    ) -> StoredProcedure:
        """
        Registers a Python function as a Snowflake Python stored procedure and returns the stored procedure.
        The usage, input arguments, and return value of this method are the same as
        they are for :func:`~snowflake.snowpark.functions.sproc`, but :meth:`register`
        cannot be used as a decorator. See examples in
        :class:`~snowflake.snowpark.stored_procedure.StoredProcedureRegistration`.

        Args:
            func: A Python function used for creating the stored procedure. Note that the first parameter
                of your function should be a snowpark Session.
            return_type: A :class:`~snowflake.snowpark.types.DataType` representing the return data
                type of the stored procedure. Optional if type hints are provided.
            input_types: A list of :class:`~snowflake.snowpark.types.DataType`
                representing the input data types of the stored procedure. Optional if
                type hints are provided.
            name: A string or list of strings that specify the name or fully-qualified
                object identifier (database name, schema name, and function name) for
                the stored procedure in Snowflake, which allows you to call this stored procedure in a SQL
                command or via :meth:`~snowflake.snowpark.Session.call`.
                If it is not provided, a name will be automatically generated for the stored procedure.
                A name must be specified when ``is_permanent`` is ``True``.
            is_permanent: Whether to create a permanent stored procedure. The default is ``False``.
                If it is ``True``, a valid ``stage_location`` must be provided.
            stage_location: The stage location where the Python file for the stored procedure
                and its dependencies should be uploaded. The stage location must be specified
                when ``is_permanent`` is ``True``, and it will be ignored when
                ``is_permanent`` is ``False``. It can be any stage other than temporary
                stages and external stages.
            imports: A list of imports that only apply to this stored procedure. You can use a string to
                represent a file path (similar to the ``path`` argument in
                :meth:`~snowflake.snowpark.Session.add_import`) in this list, or a tuple of two
                strings to represent a file path and an import path (similar to the ``import_path``
                argument in :meth:`~snowflake.snowpark.Session.add_import`). These stored procedure-level imports
                will override the session-level imports added by
                :meth:`~snowflake.snowpark.Session.add_import`.
            packages: A list of packages that only apply to this stored procedure.
                These stored procedure-level packages will override the session-level packages added by
                :meth:`~snowflake.snowpark.Session.add_packages` and
                :meth:`~snowflake.snowpark.Session.add_requirements`.
            replace: Whether to replace a stored procedure that already was registered. The default is ``False``.
                If it is ``False``, attempting to register a stored procedure with a name that already exists
                results in a ``SnowparkSQLException`` exception being thrown. If it is ``True``,
                an existing stored procedure with the same name is overwritten.
            if_not_exists: Whether to skip creation of a stored procedure the same procedure is already registered.
                The default is ``False``. ``if_not_exists`` and ``replace`` are mutually exclusive and a ``ValueError``
                is raised when both are set. If it is ``True`` and a stored procedure is already registered, the registration is skipped.
            parallel: The number of threads to use for uploading stored procedure files with the
                `PUT <https://docs.snowflake.com/en/sql-reference/sql/put.html#put>`_
                command. The default value is 4 and supported values are from 1 to 99.
                Increasing the number of threads can improve performance when uploading
                large stored procedure files.
            execute_as: What permissions should the procedure have while executing. This
                supports caller, or owner for now.
            strict: Whether the created stored procedure is strict. A strict stored procedure will not invoke
                the stored procedure if any input is null. Instead, a null value will always be returned. Note
                that the stored procedure might still return null for non-null inputs.
            statement_params: Dictionary of statement level parameters to be set while executing this action.
            source_code_display: Display the source code of the stored procedure `func` as comments in the generated script.
                The source code is dynamically generated therefore it may not be identical to how the
                `func` is originally defined. The default is ``True``.
                If it is ``False``, source code will not be generated or displayed.
            external_access_integrations: The names of one or more external access integrations. Each
                integration you specify allows access to the external network locations and secrets
                the integration specifies.
            secrets: The key-value pairs of string types of secrets used to authenticate the external network location.
                The secrets can be accessed from handler code. The secrets specified as values must
                also be specified in the external access integration and the keys are strings used to
                retrieve the secrets using secret API.
            comment: Adds a comment for the created object. See
                `COMMENT <https://docs.snowflake.com/en/sql-reference/sql/comment>`_

        See Also:
            - :func:`~snowflake.snowpark.functions.sproc`
            - :meth:`register_from_file`
        """
        if not callable(func):
            raise TypeError(
                "Invalid function: not a function or callable "
                f"(__call__ is not defined): {type(func)}"
            )

        check_execute_as_arg(execute_as)
        check_register_args(
            TempObjectType.PROCEDURE, name, is_permanent, stage_location, parallel
        )
        native_app_params = kwargs.get("native_app_params", None)

        # register stored procedure
        return self._do_register_sp(
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
            external_access_integrations=external_access_integrations,
            secrets=secrets,
            comment=comment,
            statement_params=statement_params,
            execute_as=execute_as,
            api_call_source="StoredProcedureRegistration.register",
            source_code_display=source_code_display,
            anonymous=kwargs.get("anonymous", False),
            is_permanent=is_permanent,
            # force_inline_code avoids uploading python file
            # when we know the code is not too large. This is useful
            # in pandas API to create stored procedures not registered by users.
            force_inline_code=kwargs.get("force_inline_code", False),
            native_app_params=native_app_params,
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
        execute_as: typing.Literal["caller", "owner"] = "owner",
        strict: bool = False,
        external_access_integrations: Optional[List[str]] = None,
        secrets: Optional[Dict[str, str]] = None,
        comment: Optional[str] = None,
        *,
        statement_params: Optional[Dict[str, str]] = None,
        source_code_display: bool = True,
        skip_upload_on_content_match: bool = False,
    ) -> StoredProcedure:
        """
        Registers a Python function as a Snowflake Python stored procedure from a Python or zip file,
        and returns the stored procedure. Apart from ``file_path`` and ``func_name``, the input arguments
        of this method are the same as :meth:`register`. See examples in
        :class:`~snowflake.snowpark.stored_procedure.StoredProcedureRegistration`.

        Args:
            file_path: The path of a local file or a remote file in the stage. See
                more details on ``path`` argument of
                :meth:`session.add_import() <snowflake.snowpark.Session.add_import>`.
                Note that unlike ``path`` argument of
                :meth:`session.add_import() <snowflake.snowpark.Session.add_import>`,
                here the file can only be a Python file or a compressed file
                (e.g., .zip file) containing Python modules.
            func_name: The Python function name in the file that will be created
                as a stored procedure.
            return_type: A :class:`~snowflake.snowpark.types.DataType` representing the return data
                type of the stored procedure. Optional if type hints are provided.
            input_types: A list of :class:`~snowflake.snowpark.types.DataType`
                representing the input data types of the stored procedure. Optional if
                type hints are provided.
            name: A string or list of strings that specify the name or fully-qualified
                object identifier (database name, schema name, and function name) for
                the stored procedure in Snowflake, which allows you to call this stored procedure in a SQL
                command or via :meth:`~snowflake.snowpark.Session.call`.
                If it is not provided, a name will be automatically generated for the stored procedure.
                A name must be specified when ``is_permanent`` is ``True``.
            is_permanent: Whether to create a permanent stored procedure. The default is ``False``.
                If it is ``True``, a valid ``stage_location`` must be provided.
            stage_location: The stage location where the Python file for the stored procedure
                and its dependencies should be uploaded. The stage location must be specified
                when ``is_permanent`` is ``True``, and it will be ignored when
                ``is_permanent`` is ``False``. It can be any stage other than temporary
                stages and external stages.
            imports: A list of imports that only apply to this stored procedure. You can use a string to
                represent a file path (similar to the ``path`` argument in
                :meth:`~snowflake.snowpark.Session.add_import`) in this list, or a tuple of two
                strings to represent a file path and an import path (similar to the ``import_path``
                argument in :meth:`~snowflake.snowpark.Session.add_import`). These stored procedure-level imports
                will override the session-level imports added by
                :meth:`~snowflake.snowpark.Session.add_import`.
            packages: A list of packages that only apply to this stored procedure.
                These stored procedure-level packages will override the session-level packages added by
                :meth:`~snowflake.snowpark.Session.add_packages` and
                :meth:`~snowflake.snowpark.Session.add_requirements`.
            replace: Whether to replace a stored procedure that already was registered. The default is ``False``.
                If it is ``False``, attempting to register a stored procedure with a name that already exists
                results in a ``SnowparkSQLException`` exception being thrown. If it is ``True``,
                an existing stored procedure with the same name is overwritten.
            if_not_exists: Whether to skip creation of a stored procedure the same procedure is already registered.
                The default is ``False``. ``if_not_exists`` and ``replace`` are mutually exclusive and a ``ValueError``
                is raised when both are set. If it is ``True`` and a stored procedure is already registered, the registration is skipped.
            parallel: The number of threads to use for uploading stored procedure files with the
                `PUT <https://docs.snowflake.com/en/sql-reference/sql/put.html#put>`_
                command. The default value is 4 and supported values are from 1 to 99.
                Increasing the number of threads can improve performance when uploading
                large stored procedure files.
            execute_as: What permissions should the procedure have while executing. This
                supports caller, or owner for now.
            strict: Whether the created stored procedure is strict. A strict stored procedure will not invoke
                the stored procedure if any input is null. Instead, a null value will always be returned. Note
                that the stored procedure might still return null for non-null inputs.
            statement_params: Dictionary of statement level parameters to be set while executing this action.
            source_code_display: Display the source code of the stored procedure `func` as comments in the generated script.
                The source code is dynamically generated therefore it may not be identical to how the
                `func` is originally defined. The default is ``True``.
                If it is ``False``, source code will not be generated or displayed.
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
            comment: Adds a comment for the created object. See
                `COMMENT <https://docs.snowflake.com/en/sql-reference/sql/comment>`_

        Note::
            The type hints can still be extracted from the source Python file if they
            are provided, but currently are not working for a zip file. Therefore,
            you have to provide ``return_type`` and ``input_types`` when ``path``
            points to a zip file.

        See Also:
            - :func:`~snowflake.snowpark.functions.sproc`
            - :meth:`register`
        """
        file_path = process_file_path(file_path)
        check_register_args(
            TempObjectType.PROCEDURE, name, is_permanent, stage_location, parallel
        )
        check_execute_as_arg(execute_as)

        # register stored procedure
        return self._do_register_sp(
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
            external_access_integrations=external_access_integrations,
            secrets=secrets,
            comment=comment,
            statement_params=statement_params,
            execute_as=execute_as,
            api_call_source="StoredProcedureRegistration.register_from_file",
            source_code_display=source_code_display,
            skip_upload_on_content_match=skip_upload_on_content_match,
            is_permanent=is_permanent,
        )

    def _do_register_sp(
        self,
        func: Union[Callable, Tuple[str, str]],
        return_type: DataType,
        input_types: List[DataType],
        sp_name: str,
        stage_location: Optional[str],
        imports: Optional[List[Union[str, Tuple[str, str]]]],
        packages: Optional[List[Union[str, ModuleType]]],
        replace: bool,
        if_not_exists: bool,
        parallel: int,
        strict: bool,
        *,
        source_code_display: bool = False,
        statement_params: Optional[Dict[str, str]] = None,
        execute_as: typing.Literal["caller", "owner"] = "owner",
        anonymous: bool = False,
        api_call_source: str,
        skip_upload_on_content_match: bool = False,
        is_permanent: bool = False,
        external_access_integrations: Optional[List[str]] = None,
        secrets: Optional[Dict[str, str]] = None,
        force_inline_code: bool = False,
        comment: Optional[str] = None,
        native_app_params: Optional[Dict[str, Any]] = None,
    ) -> StoredProcedure:
        (
            udf_name,
            is_pandas_udf,
            is_dataframe_input,
            return_type,
            input_types,
        ) = process_registration_inputs(
            self._session,
            TempObjectType.PROCEDURE,
            func,
            return_type,
            input_types,
            sp_name,
            anonymous,
        )

        if is_pandas_udf:
            raise TypeError("pandas stored procedure is not supported")

        arg_names = ["session"] + [f"arg{i+1}" for i in range(len(input_types))]
        input_args = [
            UDFColumn(dt, arg_name) for dt, arg_name in zip(input_types, arg_names[1:])
        ]

        # Add in snowflake-snowpark-python if it is not already in the package list.
        packages = add_snowpark_package_to_sproc_packages(
            session=self._session,
            packages=packages,
        )

        (
            handler,
            code,
            all_imports,
            all_packages,
            upload_file_stage_location,
            custom_python_runtime_version_allowed,
        ) = resolve_imports_and_packages(
            self._session,
            TempObjectType.PROCEDURE,
            func,
            arg_names,
            udf_name,
            stage_location,
            imports,
            packages,
            parallel,
            statement_params=statement_params,
            source_code_display=source_code_display,
            skip_upload_on_content_match=skip_upload_on_content_match,
            is_permanent=is_permanent,
            force_inline_code=force_inline_code,
        )

        if (not custom_python_runtime_version_allowed) and (self._session is not None):
            check_python_runtime_version(
                self._session._runtime_version_from_requirement
            )

        anonymous_sp_sql = None
        if anonymous:
            anonymous_sp_sql = generate_anonymous_python_sp_sql(
                func=func,
                return_type=return_type,
                input_args=input_args,
                handler=handler,
                object_name=udf_name,
                all_imports=all_imports,
                all_packages=all_packages,
                raw_imports=imports,
                inline_python_code=code,
                strict=strict,
                runtime_version=self._session._runtime_version_from_requirement,
                external_access_integrations=external_access_integrations,
                secrets=secrets,
                native_app_params=native_app_params,
            )
        else:
            raised = False
            try:
                create_python_udf_or_sp(
                    session=self._session,
                    func=func,
                    return_type=return_type,
                    input_args=input_args,
                    handler=handler,
                    object_type=TempObjectType.PROCEDURE,
                    object_name=udf_name,
                    all_imports=all_imports,
                    all_packages=all_packages,
                    raw_imports=imports,
                    is_permanent=is_permanent,
                    replace=replace,
                    if_not_exists=if_not_exists,
                    inline_python_code=code,
                    execute_as=execute_as,
                    api_call_source=api_call_source,
                    strict=strict,
                    external_access_integrations=external_access_integrations,
                    secrets=secrets,
                    statement_params=statement_params,
                    comment=comment,
                    native_app_params=native_app_params,
                )
            # an exception might happen during registering a stored procedure
            # (e.g., a dependency might not be found on the stage),
            # then for a permanent stored procedure, we should delete the uploaded
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

        return StoredProcedure(
            func,
            return_type,
            input_types,
            udf_name,
            execute_as=execute_as,
            anonymous_sp_sql=anonymous_sp_sql,
            packages=packages,
        )
