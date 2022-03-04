#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#

import io
import os
import pickle
import zipfile
from enum import Enum
from logging import getLogger
from types import ModuleType
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
from snowflake.snowpark._internal.type_utils import (
    _python_type_to_snow_type,
    _retrieve_func_type_hints_from_source,
    convert_to_sf_type,
)
from snowflake.snowpark._internal.utils import TempObjectType, Utils
from snowflake.snowpark.types import DataType, StringType

logger = getLogger(__name__)

# the default handler name for generated udf python file
_DEFAULT_HANDLER_NAME = "compute"

# Max code size to inline generated closure. Beyond this threshold, the closure will be uploaded to a stage for imports.
# Current number is the same as scala. We might have the potential to make it larger but that requires further benchmark
# because zip compression ratio is quite high.
_MAX_INLINE_CLOSURE_SIZE_BYTES = 8192


class UDFColumn(NamedTuple):
    datatype: DataType
    name: str


class FunctionContext(NamedTuple):
    temp_object_type: TempObjectType
    abbr: str


class FunctionType(Enum):
    UDF = FunctionContext(TempObjectType.FUNCTION, "udf")
    PROCEDURE = FunctionContext(TempObjectType.PROCEDURE, "stored proc")


def __get_types_from_type_hints(
    func: Union[Callable, Tuple[str, str]],
    function_type: FunctionType,
) -> Tuple[DataType, List[DataType]]:
    if isinstance(func, Callable):
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
    else:
        python_types_dict = _retrieve_func_type_hints_from_source(func[0], func[1])

    return_type, _ = _python_type_to_snow_type(python_types_dict["return"])
    input_types = []
    # types are in order
    index = 0
    for key, python_type in python_types_dict.items():
        # The first parameter of sp function should be Session
        if function_type == FunctionType.PROCEDURE and index == 0:
            if python_type != snowflake.snowpark.Session and python_type not in [
                "Session",
                "snowflake.snowpark.Session",
            ]:
                raise TypeError(
                    "First parameter of stored proc function should be Session"
                )
        elif key != "return":
            input_types.append(_python_type_to_snow_type(python_type)[0])
        index += 1

    return return_type, input_types


def check_register_args(
    function_type: FunctionType,
    name: Optional[Union[str, Iterable[str]]] = None,
    is_permanent: bool = False,
    stage_location: Optional[str] = None,
    parallel: int = 4,
):
    if is_permanent:
        if not name:
            raise ValueError(
                f"name must be specified for permanent {function_type.value.abbr}"
            )
        if not stage_location:
            raise ValueError(
                f"stage_location must be specified for permanent {function_type.value.abbr}"
            )

    if parallel < 1 or parallel > 99:
        raise ValueError(
            "Supported values of parallel are from 1 to 99, " f"but got {parallel}"
        )


def process_file_path(session: "snowflake.snowpark.Session", file_path: str) -> str:
    file_path = file_path.strip()
    if not file_path.startswith(session._STAGE_PREFIX) and not os.path.exists(
        file_path
    ):
        raise ValueError(f"file_path {file_path} does not exist")
    return file_path


def process_registration_inputs(
    session: "snowflake.snowpark.Session",
    function_type: FunctionType,
    func: Union[Callable, Tuple[str, str]],
    return_type: Optional[DataType],
    input_types: Optional[List[DataType]],
    name: Optional[Union[str, Iterable[str]]],
) -> Tuple[str, DataType, List[DataType]]:
    # get the udf name
    if name:
        object_name = name if isinstance(name, str) else ".".join(name)
    else:
        object_name = f"{session.get_fully_qualified_current_schema()}.{Utils.random_name_for_temp_object(function_type.value.temp_object_type)}"
    Utils.validate_object_name(object_name)

    # get return and input types
    if return_type or input_types:
        new_return_type = return_type if return_type else StringType()
        new_input_types = input_types if input_types else []
    else:
        (
            new_return_type,
            new_input_types,
        ) = __get_types_from_type_hints(func, function_type)

    return object_name, new_return_type, new_input_types


def cleanup_failed_permanent_registration(
    session: "snowflake.snowpark.Session",
    upload_file_stage_location: str,
    stage_location: str,
) -> None:
    if stage_location and upload_file_stage_location:
        try:
            logger.info(
                "Removing Snowpark uploaded file: %s",
                upload_file_stage_location,
            )
            session._run_query(f"REMOVE {upload_file_stage_location}")
            logger.info(
                "Finished removing Snowpark uploaded file: %s",
                upload_file_stage_location,
            )
        except BaseException as clean_ex:
            logger.warning("Failed to clean uploaded file: %s", clean_ex)
    raise


def __generate_python_code(func: Callable, arg_names: List[str]) -> str:
    # clear the annotations because when the user annotates Variant and Geography,
    # which are from snowpark modules and will not work on the server side
    # built-in functions don't have __annotations__
    if hasattr(func, "__annotations__"):
        annotations = func.__annotations__
        func.__annotations__ = {}
        pickled_func = cloudpickle.dumps(func, protocol=pickle.HIGHEST_PROTOCOL)
        # restore the annotations so we don't change the original function
        func.__annotations__ = annotations
    else:
        pickled_func = cloudpickle.dumps(func, protocol=pickle.HIGHEST_PROTOCOL)
    args = ",".join(arg_names)
    code = f"""
import pickle

func = pickle.loads(bytes.fromhex('{pickled_func.hex()}'))

def {_DEFAULT_HANDLER_NAME}({args}):
    return func({args})
"""
    return code


def resolve_imports_and_packages(
    session: "snowflake.snowpark.Session",
    function_type: FunctionType,
    func: Callable,
    arg_names: List[str],
    udf_name: str,
    stage_location: Optional[str],
    imports: Optional[List[Union[str, Tuple[str, str]]]],
    packages: Optional[List[Union[str, ModuleType]]],
    parallel: int = 4,
) -> Tuple[str, str, str, str, str]:
    upload_stage = (
        Utils.unwrap_stage_location_single_quote(stage_location)
        if stage_location
        else session.get_session_stage()
    )

    # resolve imports
    if imports:
        udf_level_imports = {}
        for udf_import in imports:
            if isinstance(udf_import, str):
                resolved_import_tuple = session._resolve_import_path(udf_import)
            elif isinstance(udf_import, tuple) and len(udf_import) == 2:
                resolved_import_tuple = session._resolve_import_path(
                    udf_import[0], udf_import[1]
                )
            else:
                raise TypeError(
                    f"{function_type.value.abbr.replace(' ', '-')}-level import can only be a file path (str) "
                    "or a tuple of the file path (str) and the import path (str)."
                )
            udf_level_imports[resolved_import_tuple[0]] = resolved_import_tuple[1:]
        all_urls = session._resolve_imports(upload_stage, udf_level_imports)
    else:
        all_urls = session._resolve_imports(upload_stage)

    # resolve packages
    resolved_packages = (
        session._resolve_packages(packages)
        if packages
        else session._resolve_packages([], session._packages, validate_package=False)
    )

    dest_prefix = Utils.get_udf_upload_prefix(udf_name)

    # Upload closure to stage if it is beyond inline closure size limit
    if isinstance(func, Callable):
        # generate a random name for udf py file
        # and we compress it first then upload it
        udf_file_name_base = f"udf_py_{Utils.random_number()}"
        udf_file_name = f"{udf_file_name_base}.zip"
        code = __generate_python_code(func, arg_names)
        if len(code) > _MAX_INLINE_CLOSURE_SIZE_BYTES:
            dest_prefix = Utils.get_udf_upload_prefix(udf_name)
            upload_file_stage_location = Utils.normalize_remote_file_or_dir(
                f"{upload_stage}/{dest_prefix}/{udf_file_name}"
            )
            udf_file_name_base = os.path.splitext(udf_file_name)[0]
            with io.BytesIO() as input_stream:
                with zipfile.ZipFile(
                    input_stream, mode="w", compression=zipfile.ZIP_DEFLATED
                ) as zf:
                    zf.writestr(f"{udf_file_name_base}.py", code)
                session._conn.upload_stream(
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
            inline_code = None
            handler = f"{udf_file_name_base}.{_DEFAULT_HANDLER_NAME}"
        else:
            inline_code = code
            upload_file_stage_location = None
            handler = _DEFAULT_HANDLER_NAME
    else:
        udf_file_name = os.path.basename(func[0])
        # for a compressed file, it might have multiple extensions
        # and we should remove all extensions
        udf_file_name_base = udf_file_name.split(".")[0]
        inline_code = None
        handler = f"{udf_file_name_base}.{func[1]}"

        if func[0].startswith(session._STAGE_PREFIX):
            upload_file_stage_location = None
            all_urls.append(func[0])
        else:
            upload_file_stage_location = Utils.normalize_remote_file_or_dir(
                f"{upload_stage}/{dest_prefix}/{udf_file_name}"
            )
            session._conn.upload_file(
                path=func[0],
                stage_location=upload_stage,
                dest_prefix=dest_prefix,
                parallel=parallel,
                compress_data=False,
                overwrite=True,
            )
            all_urls.append(upload_file_stage_location)

    # build imports and packages string
    all_imports = ",".join(
        [url if Utils.is_single_quoted(url) else f"'{url}'" for url in all_urls]
    )
    all_packages = ",".join([f"'{package}'" for package in resolved_packages])
    return handler, inline_code, all_imports, all_packages, upload_file_stage_location


def create_python_udf_sp(
    session: "snowflake.snowpark.Session",
    return_type: DataType,
    input_args: List[UDFColumn],
    handler: str,
    function_type: FunctionType,
    object_name: str,
    all_imports: str,
    all_packages: str,
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
    packages_in_sql = f"PACKAGES=({all_packages})" if all_packages else ""
    inline_python_code_in_sql = (
        f"""
AS $$
{inline_python_code}
$$
"""
        if inline_python_code
        else ""
    )

    create_query = f"""
CREATE {"OR REPLACE " if replace else ""}
{"TEMPORARY" if is_temporary else ""} {function_type.value.temp_object_type.value} {object_name}({sql_func_args})
RETURNS {return_sql_type}
LANGUAGE PYTHON
RUNTIME_VERSION=3.8
{imports_in_sql}
{packages_in_sql}
HANDLER='{handler}'
{inline_python_code_in_sql}
"""
    session._run_query(create_query)
