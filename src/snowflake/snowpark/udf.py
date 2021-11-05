#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#
"""User-defined functions (UDFs) in Snowpark."""
import io
import os
import pickle
import zipfile
from logging import getLogger
from typing import (
    Callable,
    Iterable,
    List,
    NamedTuple,
    Optional,
    Tuple,
    Union,
    get_type_hints,
)

import cloudpickle

import snowflake.snowpark
from snowflake.snowpark._internal.sp_expressions import (
    Expression as SPExpression,
    SnowflakeUDF,
)
from snowflake.snowpark._internal.sp_types.types_package import (
    ColumnOrName,
    _python_type_to_snow_type,
    convert_to_sf_type,
)
from snowflake.snowpark._internal.utils import TempObjectType, Utils
from snowflake.snowpark.column import Column
from snowflake.snowpark.types import DataType, StringType

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
        func: Callable,
        return_type: DataType,
        input_types: List[DataType],
        name: str,
        is_return_nullable: bool = False,
    ):
        #: The Python function.
        self.func: Callable = func
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


class _UDFColumn(NamedTuple):
    datatype: DataType
    name: str


class UDFRegistration:
    """
    Provides methods to register lambdas and functions as UDFs in the Snowflake database.
    For more information about Snowflake Python UDFs, see `Python UDFs <https://docs.snowflake.com/en/LIMITEDACCESS/udf-python.html>`__.

    :attr:`session.udf <snowflake.snowpark.Session.udf>` returns an object of this class.
    You can use this object to register UDFs that you plan to use in the current session.
    The methods that register a UDF return a :class:`UserDefinedFunction` object,
    which you can also use in :class:`~snowflake.snowpark.Column` expressions.

    Examples::

        def double(x: int) -> int:
            return 2 * x

        double_udf = session.udf.register(double, name="mydoubleudf")
        session.sql(s"SELECT mydoubleudf(c) FROM table")
        df.select(double_udf("c"))

    Snowflake supports the following data types for the parameters for a UDF:

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
    ``str``                                        :class:`~snowflake.snowpark.types.GeographyType`  GEOGRAPHY
    =============================================  ================================================  =========

    Note:
        1. A temporary UDF (when ``is_permanent`` is ``False`` in
        :func:`~snowflake.snowpark.functions.udf` or :func:`UDFRegistration.register`)
        is scoped to this session and all UDF related files will be uploaded to
        a temporary session stage (:func:`session.get_session_stage() <snowflake.snowpark.Session.get_session_stage>`).
        For a permanent UDF, these files will be uploaded to the stage that you provide.

        2. You can also use :class:`typing.List` to annotate a :class:`list`,
        use :class:`typing.Dict` to annotate a :class:`dict`, and use
        :class:`typing.Any` to annotate a variant when defining a UDF.

        3. :class:`typing.Union` is not a valid type annotation for UDFs,
        but :class:`typing.Optional` can be used to indicate the optional type.

        4. Data with the VARIANT SQL type will be converted to a Python type
        dynamically inside a UDF. The following SQL types are converted to :class:`str`
        in UDFs rather than native Python types:  TIME, DATE, TIMESTAMP and BINARY.

        5. Data returned as :class:`~snowflake.snowpark.types.ArrayType` (``list``),
        :class:`~snowflake.snowpark.types.MapType` (``dict``) or
        :class:`~snowflake.snowpark.types.VariantType` by a UDF will be represented
        as a json string. You can call ``eval()`` or ``json.loads()`` to convert
        the result to a native Python object.
    """

    def __init__(self, session: "snowflake.snowpark.Session"):
        self.session = session

    def register(
        self,
        func: Callable,
        return_type: Optional[DataType] = None,
        input_types: Optional[List[DataType]] = None,
        name: Optional[Union[str, Iterable[str]]] = None,
        is_permanent: bool = False,
        stage_location: Optional[str] = None,
        imports: Optional[List[Union[str, Tuple[str, str]]]] = None,
        replace: bool = False,
        parallel: int = 4,
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

        if is_permanent:
            if not name:
                raise ValueError("name must be specified for permanent udf")
            if not stage_location:
                raise ValueError("stage_location must be specified for permanent udf")

        if parallel < 1 or parallel > 99:
            raise ValueError(
                "Supported values of parallel are from 1 to 99, " f"but got {parallel}"
            )

        # get the udf name
        if name:
            udf_name = name if isinstance(name, str) else ".".join(name)
        else:
            udf_name = f"{self.session.get_fully_qualified_current_schema()}.{Utils.random_name_for_temp_object(TempObjectType.FUNCTION)}"
        Utils.validate_object_name(udf_name)

        # get return and input types
        if return_type or input_types:
            new_return_type = return_type if return_type else StringType()
            is_return_nullable = False
            new_input_types = input_types if input_types else []
        else:
            (
                new_return_type,
                is_return_nullable,
                new_input_types,
            ) = self.__get_types_from_type_hints(func)

        # generate a random name for udf py file
        # and we compress it first then upload it
        udf_file_name = f"udf_py_{Utils.random_number()}.zip"

        # register udf
        try:
            self.__do_register_udf(
                func,
                new_return_type,
                new_input_types,
                udf_name,
                udf_file_name,
                stage_location,
                imports,
                replace,
                parallel,
            )
        # an exception might happen during registering a UDF
        # (e.g., a dependency might not be found on the stage),
        # then for a permanent udf, we should delete the uploaded
        # python file and raise the exception
        except BaseException as ex:
            if is_permanent:
                upload_stage = Utils.normalize_stage_location(stage_location)
                dest_prefix = Utils.get_udf_upload_prefix(udf_name)
                udf_file_path = f"{upload_stage}/{dest_prefix}/{udf_file_name}"
                try:
                    logger.info("Removing Snowpark uploaded file: %s", udf_file_path)
                    self.session._run_query(f"REMOVE {udf_file_path}")
                    logger.info(
                        "Finished removing Snowpark uploaded file: %s", udf_file_path
                    )
                except BaseException as clean_ex:
                    logger.warning("Failed to clean uploaded file: %s", clean_ex)
            raise ex

        return UserDefinedFunction(
            func, return_type, new_input_types, udf_name, is_return_nullable
        )

    def __get_types_from_type_hints(
        self, func: Callable
    ) -> Tuple[DataType, bool, List[DataType]]:
        # For Python 3.10+, the result values of get_type_hints()
        # will become strings, which we have to change the implementation
        # here at that time. https://www.python.org/dev/peps/pep-0563/
        num_args = func.__code__.co_argcount
        python_types_dict = get_type_hints(func)
        assert "return" in python_types_dict, f"The return type must be specified"
        assert len(python_types_dict) - 1 == num_args, (
            f"The number of arguments ({num_args}) is different from "
            f"the number of argument type hints ({len(python_types_dict) - 1})"
        )

        return_type, is_nullable = _python_type_to_snow_type(
            python_types_dict["return"]
        )
        input_types = []
        # types are in order
        for key, python_type in python_types_dict.items():
            if key != "return":
                input_types.append(_python_type_to_snow_type(python_type)[0])

        return return_type, is_nullable, input_types

    def __do_register_udf(
        self,
        func: Callable,
        return_type: DataType,
        input_types: List[DataType],
        udf_name: str,
        udf_file_name: str,
        stage_location: Optional[str] = None,
        imports: Optional[List[Union[str, Tuple[str, str]]]] = None,
        replace: bool = False,
        parallel: int = 4,
    ) -> None:
        arg_names = [f"arg{i+1}" for i in range(len(input_types))]
        input_args = [
            _UDFColumn(dt, arg_name) for dt, arg_name in zip(input_types, arg_names)
        ]
        code = self.__generate_python_code(func, arg_names)
        upload_stage = (
            Utils.normalize_stage_location(stage_location)
            if stage_location
            else self.session.get_session_stage()
        )

        # resolve imports
        if imports:
            udf_level_imports = {}
            for udf_import in imports:
                if isinstance(udf_import, str):
                    resolved_import_tuple = self.session._resolve_import_path(
                        udf_import
                    )
                elif isinstance(udf_import, tuple) and len(udf_import) == 2:
                    resolved_import_tuple = self.session._resolve_import_path(
                        udf_import[0], udf_import[1]
                    )
                else:
                    raise TypeError(
                        "UDF-level import can only be a file path (str) "
                        "or a tuple of the file path (str) and the import path (str)."
                    )
                udf_level_imports[resolved_import_tuple[0]] = resolved_import_tuple[1:]
            all_urls = self.session._resolve_imports(upload_stage, udf_level_imports)
        else:
            all_urls = self.session._resolve_imports(upload_stage)

        # Upload closure to stage if it is beyond inline closure size limit
        if len(code) > _MAX_INLINE_CLOSURE_SIZE_BYTES:
            dest_prefix = Utils.get_udf_upload_prefix(udf_name)
            upload_file_stage_location = f"{upload_stage}/{dest_prefix}/{udf_file_name}"
            udf_file_name_base = os.path.splitext(udf_file_name)[0]
            with io.BytesIO() as input_stream:
                with zipfile.ZipFile(
                    input_stream, mode="w", compression=zipfile.ZIP_DEFLATED
                ) as zf:
                    zf.writestr(f"{udf_file_name_base}.py", code)
                self.session._conn.upload_stream(
                    input_stream=input_stream,
                    stage_location=upload_stage,
                    dest_filename=udf_file_name,
                    dest_prefix=dest_prefix,
                    parallel=parallel,
                    source_compression="DEFLATE",
                    compress_data=False,
                    overwrite=True,
                )
            all_urls.append(upload_file_stage_location)
            code = None
            handler = f"{udf_file_name_base}.{_DEFAULT_HANDLER_NAME}"
        else:
            handler = _DEFAULT_HANDLER_NAME

        # build imports string
        all_imports = ",".join([f"'{url}'" for url in all_urls])
        self.__create_python_udf(
            return_type=return_type,
            input_args=input_args,
            handler=handler,
            udf_name=udf_name,
            all_imports=all_imports,
            is_temporary=stage_location is None,
            replace=replace,
            inline_python_code=code,
        )

    def __generate_python_code(self, func: Callable, arg_names: List[str]) -> str:
        pickled_func = cloudpickle.dumps(func, protocol=pickle.HIGHEST_PROTOCOL)
        args = ",".join(arg_names)
        code = f"""
import pickle

func = pickle.loads(bytes.fromhex('{pickled_func.hex()}'))

def {_DEFAULT_HANDLER_NAME}({args}):
    return func({args})
"""
        return code

    def __create_python_udf(
        self,
        return_type: DataType,
        input_args: List[_UDFColumn],
        handler: str,
        udf_name: str,
        all_imports: str,
        is_temporary: bool,
        replace: bool,
        inline_python_code: Optional[str] = None,
    ) -> None:
        return_sql_type = convert_to_sf_type(return_type)
        input_sql_types = [convert_to_sf_type(arg.datatype) for arg in input_args]
        sql_func_args = ",".join(
            [f"{a.name} {t}" for a, t in zip(input_args, input_sql_types)]
        )
        imports_in_sql = f"IMPORTS=({all_imports})" if all_imports else ""
        inline_python_code_in_sql = (
            f"""
AS $$
{inline_python_code}
$$
"""
            if inline_python_code
            else ""
        )

        create_udf_query = f"""
CREATE {"OR REPLACE " if replace else ""}
{"TEMPORARY" if is_temporary else ""} FUNCTION {udf_name}({sql_func_args})
RETURNS {return_sql_type}
LANGUAGE PYTHON
RUNTIME_VERSION=3.8
{imports_in_sql}
HANDLER='{handler}'
{inline_python_code_in_sql}
"""
        self.session._run_query(create_udf_query)
