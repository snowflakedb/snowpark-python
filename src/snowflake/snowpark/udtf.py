#!/usr/bin/env python3
#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#
"""User-defined table functions (UDTFs) in Snowpark."""
import collections.abc
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
from snowflake.snowpark._internal import type_utils
from snowflake.snowpark._internal.type_utils import (
    ColumnOrName,
    convert_sp_to_sf_type,
    python_type_str_to_object,
    retrieve_func_type_hints_from_source,
)
from snowflake.snowpark._internal.udf_utils import (
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
    ):
        #: The Python class or a tuple containing the Python file path and the function name.
        self.handler = handler
        #: The UDTF name.
        self.name: str = name

        self._output_schema = output_schema
        self._input_types = input_types

    def __call__(
        self,
        *arguments: Union[ColumnOrName, Iterable[ColumnOrName]],
        **named_arguments,
    ) -> TableFunctionCall:
        return TableFunctionCall(self.name, *arguments, **named_arguments)


class UDTFRegistration:
    """
    Provides methods to register classes as UDTFs in the Snowflake database.
    For more information about Snowflake Python UDTFs, see `Python UDTFs <https://docs.snowflake.com/en/LIMITEDACCESS/udf-python.html>`__.

    :attr:`session.udtf <snowflake.snowpark.Session.udtf>` returns an object of this class.
    You can use this object to register UDTFs that you plan to use in the current session or
    permanently. The methods that register a UDTF returns a :class:`UserDefinedTableFunction` object,
    which you can also use to call the UDTF.

    There are two ways to register a UDTF with Snowpark:

        - Use :func:`~snowflake.snowpark.functions.udtf` or :meth:`register`. By pointing to a
          `runtime Python class`, Snowpark uses `cloudpickle <https://github.com/cloudpipe/cloudpickle>`_
          to serialize this class to bytecode, and deserialize the bytecode to a Python
          class on the Snowflake server during UDTF creation. During the serialization, the
          global variables used in the Python function will be serialized into the bytecode,
          but only the name of the module object or any objects from a module that are used in the
          Python class will be serialized. During the deserialization, Python will look up the
          corresponding modules and objects by names.

    See Also:
        - :func:`~snowflake.snowpark.functions.udtf`
        - :meth:`register`
        - :meth:`register_from_file`
        - :meth:`~snowflake.snowpark.Session.add_import`
        - :meth:`~snowflake.snowpark.Session.add_packages`
    """

    def __init__(self, session: "snowflake.snowpark.Session"):
        self._session = session

    def register(
        self,
        handler: Type,
        output_schema: [Union[StructType], Iterable[str]],
        input_types: Optional[List[DataType]] = None,
        name: Optional[Union[str, Iterable[str]]] = None,
        is_permanent: bool = False,
        stage_location: Optional[str] = None,
        imports: Optional[List[Union[str, Tuple[str, str]]]] = None,
        packages: Optional[List[Union[str, ModuleType]]] = None,
        replace: bool = False,
        parallel: int = 4,
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
             If a list of column names are provided, the ``process`` method of the handler class must have return type hints to indicate the output schema data types.
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
                results in a ``ProgrammingError`` exception being thrown. If it is ``True``,
                an existing UDTF with the same name is overwritten.
            session: Use this session to register the UDTF. If it's not specified, the session that you created before calling this function will be used.
                You need to specify this parameter if you have created multiple sessions before calling this method.
            parallel: The number of threads to use for uploading UDTF files with the
                `PUT <https://docs.snowflake.com/en/sql-reference/sql/put.html#put>`_
                command. The default value is 4 and supported values are from 1 to 99.
                Increasing the number of threads can improve performance when uploading
                large UDTF files.

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
        )

    def register_from_file(
        self,
        file_path: str,
        handler_name: str,
        output_schema: [Union[StructType], Iterable[str]],
        input_types: Optional[List[DataType]] = None,
        name: Optional[Union[str, Iterable[str]]] = None,
        is_permanent: bool = False,
        stage_location: Optional[str] = None,
        imports: Optional[List[Union[str, Tuple[str, str]]]] = None,
        packages: Optional[List[Union[str, ModuleType]]] = None,
        replace: bool = False,
        parallel: int = 4,
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
                results in a ``ProgrammingError`` exception being thrown. If it is ``True``,
                an existing UDTF with the same name is overwritten.
            session: Use this session to register the UDTF. If it's not specified, the session that you created before calling this function will be used.
                You need to specify this parameter if you have created multiple sessions before calling this method.
            parallel: The number of threads to use for uploading UDTF files with the
                `PUT <https://docs.snowflake.com/en/sql-reference/sql/put.html#put>`_
                command. The default value is 4 and supported values are from 1 to 99.
                Increasing the number of threads can improve performance when uploading
                large UDTF files.

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
        )

    def _do_register_udtf(
        self,
        handler: Union[Callable, Tuple[str, str]],
        output_schema: [Union[StructType], Iterable[str]],
        input_types: Optional[List[DataType]],
        name: Optional[str],
        stage_location: Optional[str] = None,
        imports: Optional[List[Union[str, Tuple[str, str]]]] = None,
        packages: Optional[List[Union[str, ModuleType]]] = None,
        replace: bool = False,
        parallel: int = 4,
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
                type_hints = get_type_hints(getattr(handler, "process"))
                return_type_hint = type_hints.get("return")
            else:
                type_hints = retrieve_func_type_hints_from_source(
                    handler[0], func_name="process", class_name=handler[1]
                )
                return_type_hint = python_type_str_to_object(type_hints.get("return"))
            if not return_type_hint:
                raise ValueError(
                    "Result type hints are not set but 'output_schema' has only column names. You can either use a StructType instance for 'output_schema', or use"
                    "a combination of return type hints for method 'process' and column names for 'output_schema'."
                )
            if get_origin(return_type_hint) not in (
                list,
                tuple,
                collections.abc.Iterable,
                collections.abc.Iterator,
            ):
                raise ValueError(
                    f"The type hint for a UDTF handler must but a collection type. {return_type_hint} is passed."
                )
            row_type_hint = get_args(return_type_hint)[0]  # The inner Tuple
            if get_origin(row_type_hint) != tuple:
                raise ValueError(
                    f"The return type hints of method '{handler.__name__}.process' must be a collection of Tuple or tuple, for instance, Iterable[Tuple[str, int]], if you specify return type hints."
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
        )

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

        return UserDefinedTableFunction(handler, output_schema, input_types, udtf_name)


def _validate_output_schema_names(names: Iterable[str]):
    for name in names:
        validate_object_name(name)
