#!/usr/bin/env python3
#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

"""User-defined aggregate functions (UDAFs) in Snowpark. Refer to :class:`~snowflake.snowpark.udaf.UDAFRegistration` for details and sample code."""

import sys
from types import ModuleType
from typing import Any, Callable, Dict, List, Optional, Tuple, Type, Union

import snowflake.snowpark
from snowflake.connector import ProgrammingError
from snowflake.snowpark._internal.analyzer.expression import Expression, SnowflakeUDF
from snowflake.snowpark._internal.error_message import SnowparkClientExceptionMessages
from snowflake.snowpark._internal.open_telemetry import (
    open_telemetry_udf_context_manager,
)
from snowflake.snowpark._internal.type_utils import ColumnOrName, convert_sp_to_sf_type
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
    from typing import Iterable
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
        packages: Optional[List[Union[str, ModuleType]]] = None,
    ) -> None:
        #: The Python class or a tuple containing the Python file path and the function name.
        self.handler: Union[Callable, Tuple[str, str]] = handler
        #: The UDAF name.
        self.name: str = name

        self._return_type = return_type
        self._input_types = input_types

        self._packages = packages

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

        return Column(self._create_udaf_expression(exprs))

    def _create_udaf_expression(self, exprs: List[Expression]) -> SnowflakeUDF:
        if len(exprs) != len(self._input_types):
            raise ValueError(
                f"Incorrect number of arguments passed to the UDAF:"
                f" Expected: {len(self._input_types)}, Found: {len(exprs)}"
            )
        return SnowflakeUDF(
            self.name,
            exprs,
            self._return_type,
            api_call_source="UserDefinedAggregateFunction.__call__",
        )


class UDAFRegistration:
    """
    Provides methods to register lambdas and functions as UDAFs in the Snowflake database.
    For more information about Snowflake Python UDAFs, see `Python UDAFs <https://docs.snowflake.com/developer-guide/udf/python/udf-python-aggregate-functions>`__.

    :attr:`session.udaf <snowflake.snowpark.Session.udaf>` returns an object of this class.
    You can use this object to register UDAFs that you plan to use in the current session or
    permanently. The methods that register a UDAF return a :class:`UserDefinedAggregateFunction` object,
    which you can also use in :class:`~snowflake.snowpark.Column` expressions.

    Registering a UDAF is like registering a scalar UDF, you can use :meth:`register` or :func:`snowflake.snowpark.functions.udaf`
    to explicitly register it. You can also use the decorator `@udaf`. They all use ``cloudpickle`` to transfer the code from the client to the server.
    Another way is to use :meth:`register_from_file`. Refer to module :class:`snowflake.snowpark.udaf.UDAFRegistration` for when to use them.

    To query a registered UDAF is the same as to query other aggregate functions. Refer to :meth:`~snowflake.snowpark.DataFrame.agg`.
    If you want to query a UDAF right after it's created, you can call the created :class:`UserDefinedAggregateFunction` instance like in Example 1 below.

    Example 1
        Create a temporary UDAF and call it:

            >>> from snowflake.snowpark.types import IntegerType
            >>> from snowflake.snowpark.functions import call_function, col, udaf
            >>> class PythonSumUDAF:
            ...     def __init__(self) -> None:
            ...         self._sum = 0
            ...
            ...     @property
            ...     def aggregate_state(self):
            ...         return self._sum
            ...
            ...     def accumulate(self, input_value):
            ...         self._sum += input_value
            ...
            ...     def merge(self, other_sum):
            ...         self._sum += other_sum
            ...
            ...     def finish(self):
            ...         return self._sum

            >>> sum_udaf = udaf(PythonSumUDAF, return_type=IntegerType(), input_types=[IntegerType()])
            >>> df = session.create_dataframe([[1, 3], [1, 4], [2, 5], [2, 6]]).to_df("a", "b")
            >>> df.agg(sum_udaf("a").alias("sum_a")).collect()  # Query it by calling it
            [Row(SUM_A=6)]
            >>> df.select(call_function(sum_udaf.name, col("a")).alias("sum_a")).collect()  # Query it by using the name
            [Row(SUM_A=6)]

    Example 2
        Create a UDAF with type hints and ``@udaf`` decorator and query it:

            >>> from snowflake.snowpark.functions import udaf
            >>> @udaf
            ... class PythonSumUDAF:
            ...     def __init__(self) -> None:
            ...         self._sum = 0
            ...
            ...     @property
            ...     def aggregate_state(self) -> int:
            ...         return self._sum
            ...
            ...     def accumulate(self, input_value: int) -> None:
            ...         self._sum += input_value
            ...
            ...     def merge(self, other_sum: int) -> None:
            ...         self._sum += other_sum
            ...
            ...     def finish(self) -> int:
            ...         return self._sum
            >>> df = session.create_dataframe([[1, 3], [1, 4], [2, 5], [2, 6]]).to_df("a", "b")
            >>> df.agg(PythonSumUDAF("a").alias("sum_a")).collect()  # Query it by calling it
            [Row(SUM_A=6)]
            >>> df.select(call_function(PythonSumUDAF.name, col("a")).alias("sum_a")).collect()  # Query it by using the name
            [Row(SUM_A=6)]

    Example 3
        Create a permanent UDAF with a name and call it in SQL:

            >>> from snowflake.snowpark.functions import udaf
            >>> _ = session.sql("create or replace temp stage mystage").collect()
            >>> @udaf(is_permanent=True, name="sum_udaf", replace=True, stage_location="@mystage")
            ... class PythonSumUDAF:
            ...     def __init__(self) -> None:
            ...         self._sum = 0
            ...
            ...     @property
            ...     def aggregate_state(self) -> int:
            ...         return self._sum
            ...
            ...     def accumulate(self, input_value: int) -> None:
            ...         self._sum += input_value
            ...
            ...     def merge(self, other_sum: int) -> None:
            ...         self._sum += other_sum
            ...
            ...     def finish(self) -> int:
            ...         return self._sum
            >>> session.sql("select sum_udaf(column1) as sum1 from values (1, 2), (2, 3)").collect()
            [Row(SUM1=3)]

    Example 4
        Create a UDAF with UDF-level imports and type hints:

            >>> from resources.test_udf_dir.test_udf_file import mod5
            >>> from snowflake.snowpark.functions import udaf
            >>> @udaf(imports=[("tests/resources/test_udf_dir/test_udf_file.py", "resources.test_udf_dir.test_udf_file")])
            ... class SumMod5UDAF:
            ...     def __init__(self) -> None:
            ...         self._sum = 0
            ...
            ...     @property
            ...     def aggregate_state(self) -> int:
            ...         return self._sum
            ...
            ...     def accumulate(self, input_value: int) -> None:
            ...         self._sum = mod5(self._sum + input_value)
            ...
            ...     def merge(self, other_sum: int) -> None:
            ...         self._sum = mod5(self._sum + other_sum)
            ...
            ...     def finish(self) -> int:
            ...         return self._sum
            >>> df = session.create_dataframe([[1, 3], [1, 4], [2, 5], [2, 6]]).to_df("a", "b")
            >>> df.agg(SumMod5UDAF("a").alias("sum_mod5_a")).collect()
            [Row(SUM_MOD5_A=1)]

    Example 5
        Create a UDAF with UDF-level packages and type hints:

            >>> import math
            >>> from snowflake.snowpark.functions import udaf
            >>> import numpy as np
            >>> @udaf(packages=["numpy"])
            ... class SumSinUDAF:
            ...     def __init__(self) -> None:
            ...         self._sum = 0
            ...
            ...     @property
            ...     def aggregate_state(self) -> float:
            ...         return self._sum
            ...
            ...     def accumulate(self, input_value: float) -> None:
            ...         self._sum += input_value
            ...
            ...     def merge(self, other_sum: float) -> None:
            ...         self._sum += other_sum
            ...
            ...     def finish(self) -> float:
            ...         return np.sin(self._sum)
            >>> df = session.create_dataframe([[0.0], [0.5 * math.pi]]).to_df("a")
            >>> df.agg(SumSinUDAF("a").alias("sum_sin_a")).collect()
            [Row(SUM_SIN_A=1.0)]

    Example 6
        Creating a UDAF from a local Python file:

            >>> sum_udaf = session.udaf.register_from_file(
            ...     file_path="tests/resources/test_udaf_dir/test_udaf_file.py",
            ...     handler_name="MyUDAFWithTypeHints",
            ... )
            >>> df = session.create_dataframe([[1, 3], [1, 4], [2, 5], [2, 6]]).to_df("a", "b")
            >>> df.agg(sum_udaf("a").alias("sum_a")).collect()
            [Row(SUM_A=6)]

    Example 7
        Creating a UDAF from a Python file on an internal stage:

            >>> from snowflake.snowpark.functions import udaf
            >>> from snowflake.snowpark.types import IntegerType
            >>> _ = session.sql("create or replace temp stage mystage").collect()
            >>> _ = session.file.put("tests/resources/test_udaf_dir/test_udaf_file.py", "@mystage", auto_compress=False)
            >>> sum_udaf = session.udaf.register_from_file(
            ...     file_path="@mystage/test_udaf_file.py",
            ...     handler_name="MyUDAFWithoutTypeHints",
            ...     input_types=[IntegerType()],
            ...     return_type=IntegerType(),
            ... )
            >>> df = session.create_dataframe([[1, 3], [1, 4], [2, 5], [2, 6]]).to_df("a", "b")
            >>> df.agg(sum_udaf("a").alias("sum_a")).collect()
            [Row(SUM_A=6)]

    See Also:
        - :func:`~snowflake.snowpark.functions.udaf`
        - :meth:`register`
        - :meth:`register_from_file`
        - :meth:`~snowflake.snowpark.Session.add_import`
        - :meth:`~snowflake.snowpark.Session.add_packages`
        - :meth:`~snowflake.snowpark.Session.table_function`
        - :meth:`~snowflake.snowpark.DataFrame.join_table_function`
    """

    def __init__(self, session: Optional["snowflake.snowpark.session.Session"]) -> None:
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

    # TODO: Support strict/secure once the server side supports these keywords in Python UDAF
    def register(
        self,
        handler: Type,
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
        external_access_integrations: Optional[List[str]] = None,
        secrets: Optional[Dict[str, str]] = None,
        comment: Optional[str] = None,
        *,
        statement_params: Optional[Dict[str, str]] = None,
        source_code_display: bool = True,
        immutable: bool = False,
        **kwargs,
    ) -> UserDefinedAggregateFunction:
        """
        Registers a Python function as a Snowflake Python UDAF and returns the UDAF.
        The usage, input arguments, and return value of this method are the same as
        they are for :func:`~snowflake.snowpark.functions.udaf`, but :meth:`register`
        cannot be used as a decorator. See examples in
        :class:`~snowflake.snowpark.udaf.UDAFRegistration` and notes in
        :func:`~snowflake.snowpark.functions.udaf`.

        Args:
            handler: A Python class used for creating the UDAF.
            return_type: A :class:`~snowflake.snowpark.types.DataType` representing the return data
                type of the UDAF. Optional if type hints are provided.
            input_types: A list of :class:`~snowflake.snowpark.types.DataType`
                representing the input data types of the UDAF. Optional if
                type hints are provided.
            name: A string or list of strings that specify the name or fully-qualified
                object identifier (database name, schema name, and function name) for
                the UDAF in Snowflake, which allows you to call this UDAF in a SQL
                command or via :func:`~snowflake.snowpark.DataFrame.agg` or
                :func:`~snowflake.snowpark.DataFrame.group_by`.
                If it is not provided, a name will be automatically generated for the UDAF.
                A name must be specified when ``is_permanent`` is ``True``.
            is_permanent: Whether to create a permanent UDAF. The default is ``False``.
                If it is ``True``, a valid ``stage_location`` must be provided.
            stage_location: The stage location where the Python file for the UDAF
                and its dependencies should be uploaded. The stage location must be specified
                when ``is_permanent`` is ``True``, and it will be ignored when
                ``is_permanent`` is ``False``. It can be any stage other than temporary
                stages and external stages.
            imports: A list of imports that only apply to this UDAF. You can use a string to
                represent a file path (similar to the ``path`` argument in
                :meth:`~snowflake.snowpark.Session.add_import`) in this list, or a tuple of two
                strings to represent a file path and an import path (similar to the ``import_path``
                argument in :meth:`~snowflake.snowpark.Session.add_import`). These UDAF-level imports
                will override the session-level imports added by
                :meth:`~snowflake.snowpark.Session.add_import`. Note that an empty list means
                no import for this UDAF, and ``None`` or not specifying this parameter means using
                session-level imports.
            packages: A list of packages that only apply to this UDAF. These UDAF-level packages
                will override the session-level packages added by
                :meth:`~snowflake.snowpark.Session.add_packages` and
                :meth:`~snowflake.snowpark.Session.add_requirements`. Note that an empty list means
                no package for this UDAF, and ``None`` or not specifying this parameter means using
                session-level packages. To use Python packages that are not available in Snowflake,
                refer to :meth:`~snowflake.snowpark.Session.custom_package_usage_config`.
            replace: Whether to replace a UDAF that already was registered. The default is ``False``.
                If it is ``False``, attempting to register a UDAF with a name that already exists
                results in a ``SnowparkSQLException`` exception being thrown. If it is ``True``,
                an existing UDAF with the same name is overwritten.
            if_not_exists: Whether to skip creation of a UDAF when one with the same signature already exists.
                The default is ``False``. ``if_not_exists`` and ``replace`` are mutually exclusive
                and a ``ValueError`` is raised when both are set. If it is ``True`` and a UDAF with
                the same signature exists, the UDAF creation is skipped.
            parallel: The number of threads to use for uploading UDAF files with the
                `PUT <https://docs.snowflake.com/en/sql-reference/sql/put.html#put>`_
                command. The default value is 4 and supported values are from 1 to 99.
                Increasing the number of threads can improve performance when uploading
                large UDAF files.
            statement_params: Dictionary of statement level parameters to be set while executing this action.
            source_code_display: Display the source code of the UDAF `func` as comments in the generated script.
                The source code is dynamically generated therefore it may not be identical to how the
                `func` is originally defined. The default is ``True``.
                If it is ``False``, source code will not be generated or displayed.
            immutable: Whether the UDAF result is deterministic or not for the same input.
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
            - :func:`~snowflake.snowpark.functions.udaf`
            - :meth:`register_from_file`
        """
        with open_telemetry_udf_context_manager(
            self.register, handler=handler, name=name
        ):
            if not isinstance(handler, type):
                raise TypeError(
                    f"Invalid handler: expecting a class type, but get {type(handler)}"
                )

            check_register_args(
                TempObjectType.AGGREGATE_FUNCTION,
                name,
                is_permanent,
                stage_location,
                parallel,
            )

            native_app_params = kwargs.get("native_app_params", None)

            # register udaf
            return self._do_register_udaf(
                handler,
                return_type,
                input_types,
                name,
                stage_location,
                imports,
                packages,
                replace,
                if_not_exists,
                parallel,
                statement_params=statement_params,
                source_code_display=source_code_display,
                api_call_source="UDAFRegistration.register",
                is_permanent=is_permanent,
                immutable=immutable,
                external_access_integrations=external_access_integrations,
                secrets=secrets,
                comment=comment,
                native_app_params=native_app_params,
            )

    def register_from_file(
        self,
        file_path: str,
        handler_name: str,
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
        external_access_integrations: Optional[List[str]] = None,
        secrets: Optional[Dict[str, str]] = None,
        comment: Optional[str] = None,
        *,
        statement_params: Optional[Dict[str, str]] = None,
        source_code_display: bool = True,
        skip_upload_on_content_match: bool = False,
        immutable: bool = False,
    ) -> UserDefinedAggregateFunction:
        """
        Registers a Python class as a Snowflake Python UDAF from a Python or zip file,
        and returns the UDAF. Apart from ``file_path`` and ``handler_name``, the input arguments
        of this method are the same as :meth:`register`. See examples in
        :class:`~snowflake.snowpark.udaf.UDAFRegistration`.

        Args:
            file_path: The path of a local file or a remote file in the stage. See
                more details on ``path`` argument of
                :meth:`session.add_import() <snowflake.snowpark.Session.add_import>`.
                Note that unlike ``path`` argument of
                :meth:`session.add_import() <snowflake.snowpark.Session.add_import>`,
                here the file can only be a Python file or a compressed file
                (e.g., .zip file) containing Python modules.
            handler_name: The Python class name in the file that will be created
                as a UDAF.
            return_type: A :class:`~snowflake.snowpark.types.DataType` representing the return data
                type of the UDAF. Optional if type hints are provided.
            input_types: A list of :class:`~snowflake.snowpark.types.DataType`
                representing the input data types of the UDAF. Optional if
                type hints are provided.
            name: A string or list of strings that specify the name or fully-qualified
                object identifier (database name, schema name, and function name) for
                the UDAF in Snowflake, which allows you to call this UDAF in a SQL
                command or via :func:`~snowflake.snowpark.DataFrame.agg` or
                :func:`~snowflake.snowpark.DataFrame.group_by`.
                If it is not provided, a name will be automatically generated for the UDAF.
                A name must be specified when ``is_permanent`` is ``True``.
            is_permanent: Whether to create a permanent UDAF. The default is ``False``.
                If it is ``True``, a valid ``stage_location`` must be provided.
            stage_location: The stage location where the Python file for the UDAF
                and its dependencies should be uploaded. The stage location must be specified
                when ``is_permanent`` is ``True``, and it will be ignored when
                ``is_permanent`` is ``False``. It can be any stage other than temporary
                stages and external stages.
            imports: A list of imports that only apply to this UDAF. You can use a string to
                represent a file path (similar to the ``path`` argument in
                :meth:`~snowflake.snowpark.Session.add_import`) in this list, or a tuple of two
                strings to represent a file path and an import path (similar to the ``import_path``
                argument in :meth:`~snowflake.snowpark.Session.add_import`). These UDAF-level imports
                will override the session-level imports added by
                :meth:`~snowflake.snowpark.Session.add_import`. Note that an empty list means
                no import for this UDAF, and ``None`` or not specifying this parameter means using
                session-level imports.
            packages: A list of packages that only apply to this UDAF. These UDAF-level packages
                will override the session-level packages added by
                :meth:`~snowflake.snowpark.Session.add_packages` and
                :meth:`~snowflake.snowpark.Session.add_requirements`. Note that an empty list means
                no package for this UDAF, and ``None`` or not specifying this parameter means using
                session-level packages. To use Python packages that are not available in Snowflake,
                refer to :meth:`~snowflake.snowpark.Session.custom_package_usage_config`.
            replace: Whether to replace a UDAF that already was registered. The default is ``False``.
                If it is ``False``, attempting to register a UDAF with a name that already exists
                results in a ``SnowparkSQLException`` exception being thrown. If it is ``True``,
                an existing UDAF with the same name is overwritten.
            if_not_exists: Whether to skip creation of a UDAF when one with the same signature already exists.
                The default is ``False``. ``if_not_exists`` and ``replace`` are mutually exclusive
                and a ``ValueError`` is raised when both are set. If it is ``True`` and a UDAF with
                the same signature exists, the UDAF creation is skipped.
            parallel: The number of threads to use for uploading UDAF files with the
                `PUT <https://docs.snowflake.com/en/sql-reference/sql/put.html#put>`_
                command. The default value is 4 and supported values are from 1 to 99.
                Increasing the number of threads can improve performance when uploading
                large UDAF files.
            statement_params: Dictionary of statement level parameters to be set while executing this action.
            source_code_display: Display the source code of the UDAF `func` as comments in the generated script.
                The source code is dynamically generated therefore it may not be identical to how the
                `func` is originally defined. The default is ``True``.
                If it is ``False``, source code will not be generated or displayed.
            skip_upload_on_content_match: When set to ``True`` and a version of source file already exists on stage, the given source
                file will be uploaded to stage only if the contents of the current file differ from the remote file on stage. Defaults
                to ``False``.
            immutable: Whether the UDAF result is deterministic or not for the same input.
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
            The type hints can still be extracted from the local source Python file if they
            are provided, but currently are not working for a zip file or a remote file. Therefore,
            you have to provide ``return_type`` and ``input_types`` when ``path``
            points to a zip file or a remote file.

        See Also:
            - :func:`~snowflake.snowpark.functions.udaf`
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
                TempObjectType.AGGREGATE_FUNCTION,
                name,
                is_permanent,
                stage_location,
                parallel,
            )

            # register udaf
            return self._do_register_udaf(
                (file_path, handler_name),
                return_type,
                input_types,
                name,
                stage_location,
                imports,
                packages,
                replace,
                if_not_exists,
                parallel,
                external_access_integrations=external_access_integrations,
                secrets=secrets,
                statement_params=statement_params,
                source_code_display=source_code_display,
                api_call_source="UDAFRegistration.register_from_file",
                skip_upload_on_content_match=skip_upload_on_content_match,
                is_permanent=is_permanent,
                immutable=immutable,
                comment=comment,
            )

    def _do_register_udaf(
        self,
        handler: Union[Callable, Tuple[str, str]],
        return_type: Optional[DataType],
        input_types: Optional[List[DataType]],
        name: Optional[str],
        stage_location: Optional[str] = None,
        imports: Optional[List[Union[str, Tuple[str, str]]]] = None,
        packages: Optional[List[Union[str, ModuleType]]] = None,
        replace: bool = False,
        if_not_exists: bool = False,
        parallel: int = 4,
        external_access_integrations: Optional[List[str]] = None,
        secrets: Optional[Dict[str, str]] = None,
        comment: Optional[str] = None,
        *,
        native_app_params: Optional[Dict[str, Any]] = None,
        statement_params: Optional[Dict[str, str]] = None,
        source_code_display: bool = True,
        api_call_source: str,
        skip_upload_on_content_match: bool = False,
        is_permanent: bool = False,
        immutable: bool = False,
    ) -> UserDefinedAggregateFunction:
        # get the udaf name, return and input types
        (udaf_name, _, _, return_type, input_types,) = process_registration_inputs(
            self._session,
            TempObjectType.AGGREGATE_FUNCTION,
            handler,
            return_type,
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
            custom_python_runtime_version_allowed,
        ) = resolve_imports_and_packages(
            self._session,
            TempObjectType.AGGREGATE_FUNCTION,
            handler,
            arg_names,
            udaf_name,
            stage_location,
            imports,
            packages,
            parallel,
            statement_params=statement_params,
            source_code_display=source_code_display,
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
                return_type=return_type,
                input_args=input_args,
                handler=handler_name,
                object_type=TempObjectType.AGGREGATE_FUNCTION,
                object_name=udaf_name,
                all_imports=all_imports,
                all_packages=all_packages,
                raw_imports=imports,
                is_permanent=is_permanent,
                replace=replace,
                if_not_exists=if_not_exists,
                inline_python_code=code,
                api_call_source=api_call_source,
                immutable=immutable,
                external_access_integrations=external_access_integrations,
                secrets=secrets,
                statement_params=statement_params,
                comment=comment,
                native_app_params=native_app_params,
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

        return UserDefinedAggregateFunction(
            handler, udaf_name, return_type, input_types, packages=packages
        )
