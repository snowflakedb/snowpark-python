#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#
"""Stored procedures in Snowpark."""
import functools
from types import ModuleType
from typing import Any, Callable, Iterable, List, Optional, Tuple, Union

import snowflake.snowpark
from snowflake.snowpark._internal.type_utils import convert_to_sf_type
from snowflake.snowpark._internal.udf_utils import (
    FunctionType,
    UDFColumn,
    check_register_args,
    cleanup_failed_permanent_registration,
    create_python_udf_or_sp,
    process_file_path,
    process_registration_inputs,
    resolve_imports_and_packages,
)
from snowflake.snowpark._internal.utils import Utils
from snowflake.snowpark.types import DataType


class StoredProcedure:
    """
    Encapsulates a user defined lambda or function that is returned by
    :func:`~snowflake.snowpark.functions.sproc` or by :func:`StoredProcedureRegistration.register`.
    The constructor of this class is not supposed to be called directly.

    Call an instance of :class:`StoredProcedure` to invoke a stored procedure.
    The input should be Python literal values.


    Examples::

        >>> import snowflake.snowpark
        >>> from snowflake.snowpark.functions import sproc
        >>>
        >>> session.add_packages('snowflake-snowpark-python')
        >>>
        >>> @sproc(name="my_copy_sp", replace=True)
        ... def copy(session: snowflake.snowpark.Session, from_table: str, to_table: str, count: int) -> str:
        ...     session.table(from_table).limit(count).write.save_as_table(to_table, create_temp_table=True)
        ...     return "SUCCESS"
        >>>
        >>> def double(session: snowflake.snowpark.Session, x: float) -> float:
        ...     return session.sql(f"select 2 * {x}").collect()[0][0]
        >>>
        >>> # Create an instance of StoredProcedure using the sproc() function
        >>> double_sp = sproc(double, replace=True)
        >>>
        >>> # call stored proc
        >>> double_sp(2.2)
        >>> 4.4
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
        *args: Any,
        session: Optional["snowflake.snowpark.Session"] = None,
    ) -> any:
        session = session or snowflake.snowpark.session._get_active_session()
        if len(self._input_types) != len(args):
            raise ValueError(
                f"Incorrect number of arguments passed to the stored procedure. Expected: {len(self._input_types)}, Found: {len(args)}"
            )
        return session.call(self.name, *args)


class StoredProcedureRegistration:
    """
    Provides methods to register lambdas and functions as stored procedures in the Snowflake database.
    For more information about Snowflake Python stored procedures, see `Python stored procedures <https://docs.snowflake.com/en/LIMITEDACCESS/stored-procedures-python.html>`__.

    :attr:`session.sproc <snowflake.snowpark.Session.sproc>` returns an object of this class.
    You can use this object to register stored procedures that you plan to use in the current session or permanently.
    The methods that register a stored procedure return a :class:`StoredProcedure` object.

    Note that the first parameter of your function should be a snowpark Session. Also, you need to add
    `snowflake-snowpark-python` package (version >= 0.4.0) to your session before trying to create a
    stored procedure.

    Examples::

        >>> import snowflake.snowpark
        >>> from snowflake.snowpark.functions import sproc
        >>>
        >>> session.add_packages('snowflake-snowpark-python')
        >>>
        >>> def copy(session: snowflake.snowpark.Session, from_table: str, to_table: str, count: int) -> str:
        ...     session.table(from_table).limit(count).write.save_as_table(to_table)
        ...     return "SUCCESS"
        >>>
        >>> my_copy_sp = session.sproc.register(copy, name="my_copy_sp", replace=True)
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
        :func:`~snowflake.snowpark.functions.sproc`
    """

    def __init__(self, session: "snowflake.snowpark.Session"):
        self._session = session

    def describe(self, sproc_obj: StoredProcedure) -> "snowflake.snowpark.DataFrame":
        """
        Returns a :class:`~snowflake.snowpark.DataFrame` that describes the properties of a stored procedure.

        Args:
            sproc_obj: A :class:`StoredProcedure` returned by
                :func:`~snowflake.snowpark.stored_procedure.sproc` or :meth:`register`.
        """
        func_args = [convert_to_sf_type(t) for t in sproc_obj._input_types]
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
        parallel: int = 4,
    ) -> StoredProcedure:
        """
        Registers a Python function as a Snowflake Python stored procedure and returns the stored procedure.
        The usage, input arguments, and return value of this method are the same as
        they are for :func:`~snowflake.snowpark.stored_procedure.sproc`, but :meth:`register`
        cannot be used as a decorator.

        See Also:
            :func:`~snowflake.snowpark.functions.sproc`
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

            >>> import snowflake.snowpark
            >>> from snowflake.snowpark.functions import sproc
            >>> from snowflake.snowpark.types import IntegerType
            >>>
            >>> session.add_packages('snowflake-snowpark-python')
            >>>
            >>> # Contests in test_sp_file.py:
            >>> # def mod5(session, x):
            >>> #   return session.sql(f"SELECT {x} % 5").collect()[0][0]
            >>> mod5_sp = session.sproc.register_from_file(
            ...     "tests/resources/test_sp_dir/test_sp_file.py",
            ...     "mod5",
            ...     name="my_mod5_sp",
            ...     return_type=IntegerType(),
            ...     input_types=[IntegerType()],
            ...     )
            >>> mod5_sp(7)
            2

        Note::
            The type hints can still be extracted from the source Python file if they
            are provided, but currently are not working for a zip file. Therefore,
            you have to provide ``return_type`` and ``input_types`` when ``path``
            points to a zip file.

        See Also:
            - :func:`~snowflake.snowpark.functions.sproc`
            - :meth:`register`
        """
        file_path = process_file_path(self._session, file_path)
        check_register_args(
            FunctionType.PROCEDURE, name, is_permanent, stage_location, parallel
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
            UDFColumn(dt, arg_name) for dt, arg_name in zip(input_types, arg_names[1:])
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
            create_python_udf_or_sp(
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
