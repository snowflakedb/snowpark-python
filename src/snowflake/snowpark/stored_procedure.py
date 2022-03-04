#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#

import functools
from types import ModuleType
from typing import Any, Callable, Iterable, List, Optional, Tuple, Union

import snowflake.snowpark
from snowflake.snowpark._internal.sp_expressions import Literal
from snowflake.snowpark._internal.type_utils import convert_to_sf_type
from snowflake.snowpark._internal.udf_utils import (
    FunctionType,
    UDFColumn,
    check_register_args,
    cleanup_failed_permanent_registration,
    create_python_udf_sp,
    process_file_path,
    process_registration_inputs,
    resolve_imports_and_packages,
)
from snowflake.snowpark._internal.utils import Utils
from snowflake.snowpark.types import DataType


class StoredProcedure:
    """
    Encapsulates a user defined lambda or function that is returned by
    :func:`~snowflake.snowpark.stored_procedure.stored_proc` or by :func:`StoredProcedureRegistration.register`.
    The constructor of this class is not supposed to be called directly.

    Call an instance of :class:`StoredProcedure` to invoke a stored procedure. The input type should be python types.

    Examples::

        import snowflake
        from snowflake.snowpark.stored_procedure import stored_proc

        # Create an instance of StoredProcedure using the @stored_proc decorator
        @stored_proc
        def copy(session: snowflake.snowpark.Session, from_table: str, to_table: str, limit: int) -> str:
            session.table(from_table).limit(count).write.save_as_table(to_table)
            return "SUCCESS"

        def double(session: snowflake.snowpark.Session, x: int) -> int:
            return session.sql(f"select 2 * {x}").collect()[0][0]

        # Create an instance of StoredProcedure using the stored_proc() function
        double_sp = stored_proc(double)

        # call stored proc
        copy('test_from', 'test_to', 10)
    """

    def __init__(
        self,
        func: Callable,
        return_type: DataType,
        input_types: List[DataType],
        name: str,
    ):
        #: The Python function.
        self.func: Callable = func
        #: The stored procedure name.
        self.name: str = name

        self._return_type = return_type
        self._input_types = input_types

    def __call__(
        self,
        *args: Union[Any, List[Any], Tuple[Any, ...]],
        session: Optional["snowflake.snowpark.Session"] = None,
    ) -> any:
        session = session or snowflake.snowpark.session._get_active_session()
        arg_list = Utils.parse_positional_args_to_list(*args)
        if len(self._input_types) != len(arg_list):
            raise ValueError(
                f"Incorrect number of arguments passed to the stored procedure. Expected: {len(self._input_types)}, Found: {len(arg_list)}"
            )
        return session.call(self.name, arg_list)


class StoredProcedureRegistration:
    """
    Provides methods to register lambdas and functions as stored procedures in the Snowflake database.
    For more information about Snowflake Python stored procedures, see `Python UDFs <https://docs.snowflake.com/en/LIMITEDACCESS/stored-procedures-python.html>`__.

    :attr:`session.stored_proc <snowflake.snowpark.Session.stored_proc>` returns an object of this class.
    You can use this object to register stored procedures that you plan to use in the current session or permanently.
    The methods that register a stored procedure return a :class:`StoredProcedure` object.

    Note that the first parameter of your function should be a snowpark Session.

    Examples::

        def copy(session: snowflake.snowpark.Session, from_table: str, to_table: str, limit: int) -> str:
            session.table(from_table).limit(count).write.save_as_table(to_table)
            return "SUCCESS"

        copy_sp = session.stored_proc.register(copy, name="my_copy_sp")
        session.sql("call my_copy_sp('test_from', 'test_to', 10)").collect()
        session.call("my_copy_sp", "test_from", "test_to", 10)

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
        in stored procedures rather than native Python types:  TIME, DATE, TIMESTAMP and BINARY.

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

    See Also:
        :func:`~snowflake.snowpark.stored_procedure.stored_proc`
    """

    def __init__(self, session: "snowflake.snowpark.Session"):
        self._session = session

    def describe(
        self, stored_proc_obj: StoredProcedure
    ) -> "snowflake.snowpark.DataFrame":
        """
        Returns a :class:`~snowflake.snowpark.DataFrame` that describes the properties of a stored procedure.

        Args:
            stored_proc_obj: A :class:`StoredProcedure` returned by
                :func:`~snowflake.snowpark.stored_procedure.stored_proc` or :meth:`register`.
        """
        func_args = [convert_to_sf_type(t) for t in stored_proc_obj._input_types]
        return self._session.sql(
            f"describe procedure {stored_proc_obj.name}({','.join(func_args)})"
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
        parallel: int = 4,
    ) -> StoredProcedure:
        """
        Registers a Python function as a Snowflake Python stored procedure and returns the stored procedure.
        The usage, input arguments, and return value of this method are the same as
        they are for :func:`~snowflake.snowpark.stored_procedure.stored_proc`, but :meth:`register`
        cannot be used as a decorator.

        See Also:
            :func:`~snowflake.snowpark.stored_procedure.stored_proc`
        """
        if not callable(func):
            raise TypeError(
                "Invalid function: not a function or callable "
                f"(__call__ is not defined): {type(func)}"
            )

        check_register_args(
            FunctionType.PROCEDURE, name, is_permanent, stage_location, parallel
        )

        # generate a random name for udf py file
        # and we compress it first then upload it
        sp_file_name = f"sp_py_{Utils.random_number()}.zip"

        # register stored procedure
        return self.__do_register_sp(
            func,
            return_type,
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
    ) -> StoredProcedure:
        """
        Registers a Python function as a Snowflake Python stored procedure from a Python or zip file,
        and returns the stored procedure. Apart from ``file_path`` and ``func_name``, the input arguments
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
                as a stored procedure.

        Example::

            # "my_copy.py" contains a function "copy":
            # def copy(session: snowflake.snowpark.Session, from_table: str, to_table: str, limit: int) -> str:
            #     session.table(from_table).limit(count).write.save_as_table(to_table)
            #     return "SUCCESS"
            copy_sp = session.stored_proc.register_from_file("my_copy.py", "copy", name="my_copy_sp")

        Note::
            The type hints can still be extracted from the source Python file if they
            are provided, but currently are not working for a zip file. Therefore,
            you have to provide ``return_type`` and ``input_types`` when ``path``
            points to a zip file.

        See Also:
            - :func:`~snowflake.snowpark.stored_procedure.stored_proc`
            - :meth:`register`
        """
        file_path = process_file_path(self._session, file_path)
        check_register_args(
            FunctionType.UDF, name, is_permanent, stage_location, parallel
        )

        # register udf
        return self.__do_register_sp(
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

    def __do_register_sp(
        self,
        func: Union[Callable, Tuple[str, str]],
        return_type: DataType,
        input_types: List[DataType],
        sp_name: str,
        stage_location: Optional[str],
        imports: Optional[List[Union[str, Tuple[str, str]]]],
        packages: Optional[List[Union[str, ModuleType]]],
        replace: bool,
        parallel: int,
    ) -> StoredProcedure:
        udf_name, return_type, input_types = process_registration_inputs(
            self._session,
            FunctionType.PROCEDURE,
            func,
            return_type,
            input_types,
            sp_name,
        )

        arg_names = ["session"] + [f"arg{i+1}" for i in range(len(input_types))]
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
            FunctionType.PROCEDURE,
            func,
            arg_names,
            udf_name,
            stage_location,
            imports,
            packages,
            parallel,
        )

        try:
            create_python_udf_sp(
                session=self._session,
                return_type=return_type,
                input_args=input_args,
                handler=handler,
                function_type=FunctionType.PROCEDURE,
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

        return StoredProcedure(func, return_type, input_types, udf_name)


def stored_proc(
    func: Optional[Callable] = None,
    *,
    return_type: Optional[DataType] = None,
    input_types: Optional[List[DataType]] = None,
    name: Optional[Union[str, Iterable[str]]] = None,
    is_permanent: bool = False,
    stage_location: Optional[str] = None,
    imports: Optional[List[Union[str, Tuple[str, str]]]] = None,
    packages: Optional[List[Union[str, ModuleType]]] = None,
    replace: bool = False,
    session: Optional["snowflake.snowpark.Session"] = None,
    parallel: int = 4,
) -> Union[StoredProcedure, functools.partial]:
    """Registers a Python function as a Snowflake Python stored procedure and returns the stored procedure.

    It can be used as either a function call or a decorator. In most cases you work with a single session.
    This function uses that session to register the stored procedure. If you have multiple sessions, you need to
    explicitly specify the ``session`` parameter of this function. If you have a function and would
    like to register it to multiple databases, use ``session.stored_proc.register`` instead.

    Args:
        func: A Python function used for creating the stored procedure.
        return_type: A :class:`types.DataType` representing the return data
            type of the stored procedure. Optional if type hints are provided.
        input_types: A list of :class:`~snowflake.snowpark.types.DataType`
            representing the input data types of the stored procedure. Optional if
            type hints are provided.
        name: A string or list of strings that specify the name or fully-qualified
            object identifier (database name, schema name, and function name) for
            the stored procedure in Snowflake, which allows you to call this stored procedure in a SQL
            command or via :func:`session.call()`. If it is not provided, a name will
            be automatically generated for the stored procedure. A name must be specified when
            ``is_permanent`` is ``True``.
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
            argument in :meth:`~snowflake.snowpark.Session.add_import`). These stored-proc-level imports
            will override the session-level imports added by
            :meth:`~snowflake.snowpark.Session.add_import`.
        packages: A list of packages that only apply to this stored procedure. These stored-proc-level packages
            will override the session-level packages added by
            :meth:`~snowflake.snowpark.Session.add_packages` and
            :meth:`~snowflake.snowpark.Session.add_requirements`.
        replace: Whether to replace a UDF that already was registered. The default is ``False``.
            If it is ``False``, attempting to register a stored procedure with a name that already exists
            results in a ``ProgrammingError`` exception being thrown. If it is ``True``,
            an existing stored procedure with the same name is overwritten.
        session: Use this session to register the stored procedure. If it's not specified, the session that you created before calling this function will be used.
            You need to specify this parameter if you have created multiple sessions before calling this method.
        parallel: The number of threads to use for uploading stored procedure files with the
            `PUT <https://docs.snowflake.com/en/sql-reference/sql/put.html#put>`_
            command. The default value is 4 and supported values are from 1 to 99.
            Increasing the number of threads can improve performance when uploading
            large stored procedure files.

    Returns:
        A stored procedure function that can be called with python value.

    Examples::

        from snowflake.snowpark.types import IntegerType
        # register a temporary stored procedure
        add_one = stored_proc(
            lambda session_, x: session_.sql(f"SELECT {x} + 1").collect()[0][0],
            return_type=IntegerType(),
            input_types=[IntegerType()],
        )

        # register a permanent udf by setting is_permanent to True
        @udf(name="minus_one", is_permanent=True, stage_location="@mystage")
        def minus_one(session_: snowflake.snowpark.Session, x: int) -> int:
            return session_.sql(f"SELECT {x} - 1").collect()[0][0]

        # use stored-proc-level imports
        from my_math import sqrt
        @udf(name="my_sqrt", is_permanent=True, stage_location="@mystage", imports=["my_math.py"])
        def my_sqrt(session_: snowflake.snowpark.Session, x: float) -> float:
            return session_.sql(f"SELECT {sqrt(x)}").collect()[0][0]

        session.call("minus_one", 5)

    Note:
        1. When type hints are provided and are complete for a function,
        ``return_type`` and ``input_types`` are optional and will be ignored.
        See details of supported data types for stored procedure in
        :class:`~snowflake.snowpark.stored_procedure.UDFRegistration`.

            - You can use :attr:`~snowflake.snowpark.types.Variant` to
              annotate a variant, and use :attr:`~snowflake.snowpark.types.Geography`
              to annotate a geography when defining a stored procedure.

            - :class:`typing.Union` is not a valid type annotation for stored procedures,
              but :class:`typing.Optional` can be used to indicate the optional type.

        2. A temporary stored procedure (when ``is_permanent`` is ``False``) is scoped to this ``session``
        and all stored procedure related files will be uploaded to a temporary session stage
        (:func:`session.get_session_stage() <snowflake.snowpark.Session.get_session_stage>`).
        For a permanent stored procedure, these files will be uploaded to the stage that you provide.

        3. By default, stored procedure registration fails if a function with the same name is already
        registered. Invoking :func:`udf` with ``replace`` set to ``True`` will overwrite the
        previously registered function.

    See Also:
        :class:`~snowflake.snowpark.stored_procedure.StoredProcedureRegistration`
    """
    session = session or snowflake.snowpark.session._get_active_session()
    if func is None:
        return functools.partial(
            session.stored_proc.register,
            return_type=return_type,
            input_types=input_types,
            name=name,
            is_permanent=is_permanent,
            stage_location=stage_location,
            imports=imports,
            packages=packages,
            replace=replace,
            parallel=parallel,
        )
    else:
        return session.stored_proc.register(
            func,
            return_type=return_type,
            input_types=input_types,
            name=name,
            is_permanent=is_permanent,
            stage_location=stage_location,
            imports=imports,
            packages=packages,
            replace=replace,
            parallel=parallel,
        )
