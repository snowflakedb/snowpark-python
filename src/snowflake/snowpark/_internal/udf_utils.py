#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
import collections.abc
import io
import os
import pickle
import sys
import typing
import zipfile
from logging import getLogger
from types import ModuleType
from typing import (
    Any,
    Callable,
    Dict,
    List,
    NamedTuple,
    Optional,
    Tuple,
    Union,
    get_type_hints,
)

import cloudpickle

import snowflake.snowpark
from snowflake.connector.options import installed_pandas, pandas
from snowflake.snowpark._internal import code_generation, type_utils
from snowflake.snowpark._internal.analyzer.datatype_mapper import to_sql
from snowflake.snowpark._internal.telemetry import TelemetryField
from snowflake.snowpark._internal.type_utils import (
    NoneType,
    convert_sp_to_sf_type,
    infer_type,
    python_type_str_to_object,
    python_type_to_snow_type,
    retrieve_func_type_hints_from_source,
)
from snowflake.snowpark._internal.utils import (
    STAGE_PREFIX,
    TempObjectType,
    get_udf_upload_prefix,
    is_single_quoted,
    normalize_remote_file_or_dir,
    random_name_for_temp_object,
    random_number,
    unwrap_stage_location_single_quote,
    validate_object_name,
)
from snowflake.snowpark.types import DataType, StructField, StructType

if installed_pandas:
    from snowflake.snowpark.types import (
        PandasDataFrame,
        PandasDataFrameType,
        PandasSeriesType,
    )

# Python 3.8 needs to use typing.Iterable because collections.abc.Iterable is not subscriptable
# Python 3.9 can use both
# Python 3.10 needs to use collections.abc.Iterable because typing.Iterable is removed
if sys.version_info <= (3, 9):
    from typing import Iterable
else:
    from collections.abc import Iterable

logger = getLogger(__name__)

# the default handler name for generated udf python file
_DEFAULT_HANDLER_NAME = "compute"

# Max code size to inline generated closure. Beyond this threshold, the closure will be uploaded to a stage for imports.
# Current number is the same as scala. We might have the potential to make it larger but that requires further benchmark
# because zip compression ratio is quite high.
_MAX_INLINE_CLOSURE_SIZE_BYTES = 8192

# Every table function handler class must define the process method.
TABLE_FUNCTION_PROCESS_METHOD = "process"
TABLE_FUNCTION_END_PARTITION_METHOD = "end_partition"

# Every aggregate function handler class must define accumulate and finish methods.
AGGREGATE_FUNCTION_ACCULUMATE_METHOD = "accumulate"
AGGREGATE_FUNCTION_FINISH_METHOD = "finish"
AGGREGATE_FUNCTION_MERGE_METHOD = "merge"
AGGREGATE_FUNCTION_STATE_METHOD = "aggregate_state"

EXECUTE_AS_WHITELIST = frozenset(["owner", "caller"])


class UDFColumn(NamedTuple):
    datatype: DataType
    name: str


def is_local_python_file(file_path: str) -> bool:
    return not file_path.startswith(STAGE_PREFIX) and file_path.endswith(".py")


def get_python_types_dict_for_udaf(
    accumulate_hints: Dict[str, Any], finish_hints: Dict[str, Any]
) -> Dict[str, Any]:
    python_types_dict = {k: v for k, v in accumulate_hints.items() if k != "return"}
    if "return" in finish_hints:
        python_types_dict["return"] = finish_hints["return"]
    return python_types_dict


def get_python_types_dict_for_udtf(
    process: Dict[str, Any], end_partition: Dict[str, Any]
) -> Dict[str, Any]:
    # Prefer input types from process and return types from end_partition
    python_types_dict = {**end_partition, **process}
    if "return" in end_partition:
        python_types_dict["return"] = end_partition["return"]
    return python_types_dict


def extract_return_type_from_udtf_type_hints(
    return_type_hint, output_schema, func_name
) -> Union[StructType, "PandasDataFrameType", None]:
    if return_type_hint is None and output_schema is not None:
        raise ValueError(
            "The return type hint is not set but 'output_schema' has only column names. You can either use a StructType instance for 'output_schema', or use"
            "a combination of a return type hint for method 'process' and column names for 'output_schema'."
        )
    if typing.get_origin(return_type_hint) in [
        list,
        tuple,
        collections.abc.Iterable,
        collections.abc.Iterator,
    ]:
        row_type_hint = typing.get_args(return_type_hint)[0]  # The inner Tuple
        if typing.get_origin(row_type_hint) != tuple:
            raise ValueError(
                f"The return type hint of method '{func_name}.process' must be a collection of tuples, for instance, Iterable[Tuple[str, int]], if you specify return type hint."
            )
        column_type_hints = typing.get_args(row_type_hint)
        if len(column_type_hints) > 1 and column_type_hints[1] == Ellipsis:
            return StructType(
                [
                    StructField(
                        name,
                        type_utils.python_type_to_snow_type(column_type_hints[0])[0],
                    )
                    for name in output_schema
                ]
            )
        elif output_schema:
            if len(column_type_hints) != len(output_schema):
                raise ValueError(
                    f"'output_schema' has {len(output_schema)} names while type hints Tuple has only {len(column_type_hints)}."
                )
            return StructType(
                [
                    StructField(
                        name,
                        type_utils.python_type_to_snow_type(column_type)[0],
                    )
                    for name, column_type in zip(output_schema, column_type_hints)
                ]
            )
        else:  # both type hints and return type are specified
            return None
    elif return_type_hint is None:
        return None
    else:
        if installed_pandas:  # Vectorized UDTF
            if typing.get_origin(return_type_hint) == PandasDataFrame:
                return PandasDataFrameType(
                    col_types=[
                        python_type_to_snow_type(x)[0]
                        for x in typing.get_args(return_type_hint)
                    ],
                    col_names=output_schema,
                )
            elif return_type_hint is pandas.DataFrame:
                return PandasDataFrameType(
                    []
                )  # placeholder, indicating the return type is pandas DataFrame
        if return_type_hint is NoneType:
            return None
        else:
            raise ValueError(
                f"The return type hint for a UDTF handler must be a collection type or None or a PandasDataFrame. {return_type_hint} is used."
            )


def get_types_from_type_hints(
    func: Union[Callable, Tuple[str, str]],
    object_type: TempObjectType,
    output_schema: Optional[List[str]] = None,
) -> Tuple[DataType, List[DataType]]:
    if isinstance(func, Callable):
        # For Python 3.10+, the result values of get_type_hints()
        # will become strings, which we have to change the implementation
        # here at that time. https://www.python.org/dev/peps/pep-0563/
        func_name = func.__name__
        try:
            if object_type == TempObjectType.AGGREGATE_FUNCTION:
                accumulate_hints = get_type_hints(
                    getattr(func, AGGREGATE_FUNCTION_ACCULUMATE_METHOD, func)
                )
                finish_hints = get_type_hints(
                    getattr(func, AGGREGATE_FUNCTION_FINISH_METHOD, func)
                )
                python_types_dict = get_python_types_dict_for_udaf(
                    accumulate_hints, finish_hints
                )

            elif object_type == TempObjectType.TABLE_FUNCTION:
                if not (
                    hasattr(func, TABLE_FUNCTION_END_PARTITION_METHOD)
                    or hasattr(func, TABLE_FUNCTION_PROCESS_METHOD)
                ):
                    raise AttributeError(
                        f"Neither `{TABLE_FUNCTION_PROCESS_METHOD}` nor `{TABLE_FUNCTION_END_PARTITION_METHOD}` is defined for class {func}"
                    )
                process_types_dict = {}
                end_partition_types_dict = {}
                # PROCESS and END_PARTITION have the same return type but input types might be different, favor PROCESS's types if both methods are present
                if hasattr(func, TABLE_FUNCTION_PROCESS_METHOD):
                    process_types_dict = get_type_hints(
                        getattr(func, TABLE_FUNCTION_PROCESS_METHOD)
                    )
                if hasattr(func, TABLE_FUNCTION_END_PARTITION_METHOD):
                    end_partition_types_dict = get_type_hints(
                        getattr(func, TABLE_FUNCTION_END_PARTITION_METHOD)
                    )
                python_types_dict = get_python_types_dict_for_udtf(
                    process_types_dict, end_partition_types_dict
                )
            else:
                python_types_dict = get_type_hints(func)
        except TypeError:
            # if we fail to run get_type_hints on a function (a TypeError will be raised),
            # return empty type dict. This will fail for functions like numpy.ufunc
            # (e.g., get_type_hints(np.exp))
            python_types_dict = {}
        return_type_hint = python_types_dict.get("return")
    else:
        # Register from file
        filename, func_name = func[0], func[1]
        if not is_local_python_file(filename):
            python_types_dict = {}
        elif object_type == TempObjectType.AGGREGATE_FUNCTION:
            accumulate_hints = retrieve_func_type_hints_from_source(
                filename, AGGREGATE_FUNCTION_ACCULUMATE_METHOD, class_name=func_name
            )
            finish_hints = retrieve_func_type_hints_from_source(
                filename, AGGREGATE_FUNCTION_FINISH_METHOD, class_name=func_name
            )
            python_types_dict = get_python_types_dict_for_udaf(
                accumulate_hints, finish_hints
            )
        elif object_type == TempObjectType.TABLE_FUNCTION:
            process_types_dict = retrieve_func_type_hints_from_source(
                filename, TABLE_FUNCTION_PROCESS_METHOD, class_name=func[1]
            )
            end_partition_types_dict = retrieve_func_type_hints_from_source(
                func[0], TABLE_FUNCTION_END_PARTITION_METHOD, class_name=func[1]
            )
            if process_types_dict is None and end_partition_types_dict is None:
                raise ValueError(
                    f"Neither {func_name}.{TABLE_FUNCTION_PROCESS_METHOD} or {func_name}.{TABLE_FUNCTION_END_PARTITION_METHOD} could be found from {filename}"
                )
            python_types_dict = get_python_types_dict_for_udtf(
                process_types_dict or {}, end_partition_types_dict or {}
            )
        elif object_type in (TempObjectType.FUNCTION, TempObjectType.PROCEDURE):
            python_types_dict = retrieve_func_type_hints_from_source(
                filename, func_name
            )
        else:
            raise ValueError(
                f"Expecting FUNCTION, PROCEDURE, TABLE_FUNCTION, or AGGREGATE_FUNCTION as object_type, got {object_type}"
            )

        if "return" in python_types_dict:
            return_type_hint = python_type_str_to_object(
                python_types_dict["return"], object_type == TempObjectType.PROCEDURE
            )
        else:
            return_type_hint = None

    if object_type == TempObjectType.TABLE_FUNCTION:
        return_type = extract_return_type_from_udtf_type_hints(
            return_type_hint, output_schema, func_name
        )
    else:
        return_type = (
            python_type_to_snow_type(
                python_types_dict["return"], object_type == TempObjectType.PROCEDURE
            )[0]
            if "return" in python_types_dict
            else None
        )

    input_types = []

    # types are in order
    index = 0
    for key, python_type in python_types_dict.items():
        # The first parameter of sp function should be Session
        if object_type == TempObjectType.PROCEDURE and index == 0:
            if python_type != snowflake.snowpark.Session and python_type not in [
                "Session",
                "snowflake.snowpark.Session",
            ]:
                raise TypeError(
                    "The first argument of stored proc function should be Session"
                )
        elif key != "return":
            input_types.append(
                python_type_to_snow_type(
                    python_type, object_type == TempObjectType.PROCEDURE
                )[0]
            )
        index += 1

    return return_type, input_types


def get_error_message_abbr(object_type: TempObjectType) -> str:
    if object_type == TempObjectType.FUNCTION:
        return "udf"
    if object_type == TempObjectType.PROCEDURE:
        return "stored proc"
    if object_type == TempObjectType.TABLE_FUNCTION:
        return "table function"
    if object_type == TempObjectType.AGGREGATE_FUNCTION:
        return "aggregate function"
    raise ValueError(f"Expect FUNCTION of PROCEDURE, but get {object_type}")


def check_register_args(
    object_type: TempObjectType,
    name: Optional[Union[str, Iterable[str]]] = None,
    is_permanent: bool = False,
    stage_location: Optional[str] = None,
    parallel: int = 4,
):
    if is_permanent:
        if not name:
            raise ValueError(
                f"name must be specified for permanent {get_error_message_abbr(object_type)}"
            )
        if not stage_location:
            raise ValueError(
                f"stage_location must be specified for permanent {get_error_message_abbr(object_type)}"
            )

    if parallel < 1 or parallel > 99:
        raise ValueError(
            "Supported values of parallel are from 1 to 99, " f"but got {parallel}"
        )


def check_execute_as_arg(execute_as: typing.Literal["caller", "owner"]):
    if (
        not isinstance(execute_as, str)
        or execute_as.lower() not in EXECUTE_AS_WHITELIST
    ):
        raise TypeError(
            f"'execute_as' value '{execute_as}' is invalid, choose from "
            f"{', '.join(EXECUTE_AS_WHITELIST, )}"
        )


def check_python_runtime_version(runtime_version_from_requirement: Optional[str]):
    system_version = f"{sys.version_info[0]}.{sys.version_info[1]}"
    if (
        runtime_version_from_requirement is not None
        and runtime_version_from_requirement != system_version
    ):
        raise ValueError(
            f"Cloudpickle can only be used to send objects between the exact same version of Python. "
            f"Your system version is {system_version} while your requirements have specified version "
            f"{runtime_version_from_requirement}!"
        )


def process_file_path(file_path: str) -> str:
    file_path = file_path.strip()
    if not file_path.startswith(STAGE_PREFIX) and not os.path.exists(file_path):
        raise ValueError(f"file_path {file_path} does not exist")
    return file_path


def extract_return_input_types(
    func: Union[Callable, Tuple[str, str]],
    return_type: Optional[DataType],
    input_types: Optional[List[DataType]],
    object_type: TempObjectType,
    output_schema: Optional[List[str]] = None,
) -> Tuple[bool, bool, Union[DataType, List[DataType]], List[DataType]]:
    """
    Returns:
        is_pandas_udf
        is_dataframe_input
        return_types
        input_types

    Notes:
        There are 3 cases:
           1. return_type and input_types are provided:
              a. type hints are provided and they are all pandas.Series or pandas.DataFrame,
                 then combine them to pandas-related types.
              b. otherwise, just use return_type and input_types.
           2. return_type and input_types are not provided, but type hints are provided,
              then just use the types inferred from type hints.
    """

    (
        return_type_from_type_hints,
        input_types_from_type_hints,
    ) = get_types_from_type_hints(func, object_type, output_schema)
    if installed_pandas and return_type and return_type_from_type_hints:
        if isinstance(return_type_from_type_hints, PandasSeriesType):
            res_return_type = (
                return_type.element_type
                if isinstance(return_type, PandasSeriesType)
                else return_type
            )
            res_input_types = (
                input_types[0].col_types
                if len(input_types) == 1
                and isinstance(input_types[0], PandasDataFrameType)
                else input_types
            )
            res_input_types = [
                tp.element_type if isinstance(tp, PandasSeriesType) else tp
                for tp in res_input_types
            ]
            if len(input_types_from_type_hints) == 0:
                return True, False, res_return_type, []
            elif len(input_types_from_type_hints) == 1 and isinstance(
                input_types_from_type_hints[0], PandasDataFrameType
            ):
                return True, True, res_return_type, res_input_types
            elif all(
                isinstance(tp, PandasSeriesType) for tp in input_types_from_type_hints
            ):
                return True, False, res_return_type, res_input_types
        elif isinstance(
            return_type_from_type_hints, PandasDataFrameType
        ):  # vectorized UDTF
            return_type = PandasDataFrameType(
                [x.datatype for x in return_type], [x.name for x in return_type]
            )

    res_return_type = return_type or return_type_from_type_hints
    res_input_types = input_types or input_types_from_type_hints

    if not res_return_type or (
        installed_pandas
        and isinstance(res_return_type, PandasSeriesType)
        and not res_return_type.element_type
    ):
        raise TypeError("The return type must be specified")

    # We only want to have this check when only type hints are provided
    if (
        not return_type
        and not input_types
        and isinstance(func, Callable)
        and hasattr(func, "__code__")
    ):
        # don't count Session if it's a SP
        num_args = (
            func.__code__.co_argcount
            if object_type == TempObjectType.FUNCTION
            else func.__code__.co_argcount - 1
        )
        if num_args != len(input_types_from_type_hints):
            raise TypeError(
                f'{"" if object_type == TempObjectType.FUNCTION else f"Excluding session argument in stored procedure, "}'
                f"the number of arguments ({num_args}) is different from "
                f"the number of argument type hints ({len(input_types_from_type_hints)})"
            )

    if not installed_pandas:
        return False, False, res_return_type, res_input_types

    if isinstance(res_return_type, PandasSeriesType):
        if len(res_input_types) == 0:
            return True, False, res_return_type.element_type, []
        elif len(res_input_types) == 1 and isinstance(
            res_input_types[0], PandasDataFrameType
        ):
            return (
                True,
                True,
                res_return_type.element_type,
                res_input_types[0].get_snowflake_col_datatypes(),
            )
        elif all(isinstance(tp, PandasSeriesType) for tp in res_input_types):
            return (
                True,
                False,
                res_return_type.element_type,
                [tp.element_type for tp in res_input_types],
            )
    elif isinstance(res_return_type, PandasDataFrameType):
        if len(res_input_types) == 0:
            return True, True, res_return_type, []
        elif len(res_input_types) == 1 and isinstance(
            res_input_types[0], PandasDataFrameType
        ):
            return (
                True,
                True,
                res_return_type,
                res_input_types[0].get_snowflake_col_datatypes(),
            )
        else:
            return True, True, res_return_type, res_input_types

    # not pandas UDF
    if not isinstance(res_return_type, (PandasSeriesType, PandasDataFrameType)) and all(
        not isinstance(tp, (PandasSeriesType, PandasDataFrameType))
        for tp in res_input_types
    ):
        return False, False, res_return_type, res_input_types

    raise TypeError(
        f"Invalid return type or input types: return type {res_return_type}, input types {res_input_types}"
    )


def process_registration_inputs(
    session: "snowflake.snowpark.Session",
    object_type: TempObjectType,
    func: Union[Callable, Tuple[str, str]],
    return_type: Optional[DataType],
    input_types: Optional[List[DataType]],
    name: Optional[Union[str, Iterable[str]]],
    anonymous: bool = False,
    output_schema: Optional[List[str]] = None,
) -> Tuple[str, bool, bool, DataType, List[DataType]]:
    """

    Args:
        output_schema: List of column names of in the output, only applicable to UDTF
    """
    if name:
        object_name = name if isinstance(name, str) else ".".join(name)
    else:
        object_name = random_name_for_temp_object(object_type)
        if not anonymous:
            object_name = (
                f"{session.get_fully_qualified_current_schema()}.{object_name}"
            )
    validate_object_name(object_name)

    # get return and input types
    (
        is_pandas_udf,
        is_dataframe_input,
        return_type,
        input_types,
    ) = extract_return_input_types(
        func, return_type, input_types or [], object_type, output_schema
    )

    return object_name, is_pandas_udf, is_dataframe_input, return_type, input_types


def cleanup_failed_permanent_registration(
    session: "snowflake.snowpark.Session",
    upload_file_stage_location: str,
    stage_location: str,
) -> None:
    if stage_location and upload_file_stage_location:
        try:
            logger.debug(
                "Removing Snowpark uploaded file: %s",
                upload_file_stage_location,
            )
            session._run_query(f"REMOVE {upload_file_stage_location}")
            logger.info(
                "Finished removing Snowpark uploaded file: %s",
                upload_file_stage_location,
            )
        except Exception as clean_ex:
            logger.warning("Failed to clean uploaded file: %s", clean_ex)


def pickle_function(func: Callable) -> bytes:
    failure_hint = (
        "you might have to save the unpicklable object in the local environment first, "
        "add it to the UDF with session.add_import(), and read it from the UDF."
    )
    try:
        return cloudpickle.dumps(func, protocol=pickle.HIGHEST_PROTOCOL)
    # it happens when copying the global object inside the UDF that can't be pickled
    except TypeError as ex:
        error_message = str(ex)
        if "cannot pickle" in error_message:
            raise TypeError(f"{error_message}: {failure_hint}")
        raise ex
    # it shouldn't happen because the function can always be pickled
    # but we still catch this exception here in case cloudpickle changes its implementation
    except pickle.PicklingError as ex:
        raise pickle.PicklingError(f"{str(ex)}: {failure_hint}")


def generate_python_code(
    func: Callable,
    arg_names: List[str],
    object_type: TempObjectType,
    is_pandas_udf: bool,
    is_dataframe_input: bool,
    max_batch_size: Optional[int] = None,
    source_code_display: bool = False,
) -> str:
    # if func is a method object, we need to extract the target function first to check
    # annotations. However, we still serialize the original method because the extracted
    # function will have an extra argument `cls` or `self` from the class.
    if object_type == TempObjectType.TABLE_FUNCTION:
        if is_pandas_udf:
            annotated_funcs = [getattr(func, TABLE_FUNCTION_END_PARTITION_METHOD)]
        else:
            annotated_funcs = [getattr(func, TABLE_FUNCTION_PROCESS_METHOD)]
    elif object_type == TempObjectType.AGGREGATE_FUNCTION:
        annotated_funcs = [
            getattr(func, AGGREGATE_FUNCTION_ACCULUMATE_METHOD),
            getattr(func, AGGREGATE_FUNCTION_FINISH_METHOD),
            getattr(func, AGGREGATE_FUNCTION_MERGE_METHOD),
            getattr(func, AGGREGATE_FUNCTION_STATE_METHOD).fget,
        ]
    elif object_type in (TempObjectType.FUNCTION, TempObjectType.PROCEDURE):
        annotated_funcs = [getattr(func, "__func__", func)]
    else:
        raise ValueError(
            f"Expecting FUNCTION, PROCEDURE, TABLE_FUNCTION or AGGREGATE_FUNCTION for object_type, got {object_type}"
        )

    # clear the annotations because when the user annotates Variant and Geography,
    # which are from snowpark modules and will not work on the server side
    # built-in functions don't have __annotations__
    # TODO(SNOW-861329): This removal could cause race condition if two sessions are trying to register UDF using the
    #   same decorated function/class.
    annotations = [
        getattr(annotated_func, "__annotations__", None)
        for annotated_func in annotated_funcs
    ]
    if any(annotations):
        try:
            for annotated_func in annotated_funcs:
                annotated_func.__annotations__ = {}
            # we still serialize the original function
            pickled_func = pickle_function(func)
        finally:
            # restore the annotations so we don't change the original function
            for annotated_func, annotation in zip(annotated_funcs, annotations):
                if annotation:
                    annotated_func.__annotations__ = annotation
    else:
        pickled_func = pickle_function(func)
    args = ",".join(arg_names)

    try:
        source_code_comment = (
            code_generation.generate_source_code(func) if source_code_display else ""
        )
    except Exception as exc:
        error_msg = (
            f"Source code comment could not be generated for {func} due to error {exc}."
        )
        logger.debug(error_msg)
        # We shall also have telemetry for the code generation
        # check https://snowflakecomputing.atlassian.net/browse/SNOW-651381
        source_code_comment = code_generation.comment_source_code(error_msg)

    deserialization_code = f"""
import pickle

func = pickle.loads(bytes.fromhex('{pickled_func.hex()}'))
{source_code_comment}
""".rstrip()

    if object_type == TempObjectType.PROCEDURE:
        func_code = f"""
def {_DEFAULT_HANDLER_NAME}({args}):
    return func({args})
"""
    else:
        # UDF and UDTF use wrapper functions to invoke the pickled functions.
        if is_pandas_udf:
            func_args = "df"
            wrapper_params = func_args
            if not is_dataframe_input:
                func_args = "*[df[idx] for idx in range(df.shape[1])]"
        else:
            func_args = args
            wrapper_params = args

        # Function/Class definition
        func_code = """

from threading import RLock

lock = RLock()

class InvokedFlag:
    def __init__(self):
        self.invoked = False

def lock_function_once(f, flag):
    def wrapper(*args, **kwargs):
        if not flag.invoked:
            with lock:
                if not flag.invoked:
                    result = f(*args, **kwargs)
                    flag.invoked = True
                    return result
                return f(*args, **kwargs)
        return f(*args, **kwargs)
    return wrapper

"""
        if object_type == TempObjectType.TABLE_FUNCTION:
            func_code = f"""{func_code}
init_invoked = InvokedFlag()
process_invoked = InvokedFlag()
end_partition_invoked = InvokedFlag()

class {_DEFAULT_HANDLER_NAME}(func):
    def __init__(self):
        lock_function_once(super().__init__, init_invoked)()
"""
            if hasattr(func, TABLE_FUNCTION_PROCESS_METHOD):
                func_code = f"""{func_code}
    def process(self, {wrapper_params}):
        return lock_function_once(super().process, process_invoked)({func_args})
"""
            if hasattr(func, TABLE_FUNCTION_END_PARTITION_METHOD):
                func_code = f"""{func_code}
    def end_partition(self, {wrapper_params if is_pandas_udf else ""}):
        return lock_function_once(super().end_partition, end_partition_invoked)({func_args if is_pandas_udf else ""})
"""
        elif object_type == TempObjectType.AGGREGATE_FUNCTION:
            func_code = f"""{func_code}
init_invoked = InvokedFlag()
accumulate_invoked = InvokedFlag()
merge_invoked = InvokedFlag()
finish_invoked = InvokedFlag()

class {_DEFAULT_HANDLER_NAME}(func):
    def __init__(self):
        lock_function_once(super().__init__, init_invoked)()

    def accumulate(self, {args}):
        return lock_function_once(super().accumulate, accumulate_invoked)({args})

    def merge(self, other_agg_state):
        return lock_function_once(super().merge, merge_invoked)(other_agg_state)

    def finish(self):
        return lock_function_once(super().finish, finish_invoked)()
            """
        elif object_type == TempObjectType.FUNCTION:
            func_code = f"""{func_code}
invoked = InvokedFlag()

def {_DEFAULT_HANDLER_NAME}({wrapper_params}):
    return lock_function_once(func, invoked)({func_args})
""".rstrip()

        # Vectorized UDxF attributes
        if is_pandas_udf:
            pandas_code = f"""
import pandas

{_DEFAULT_HANDLER_NAME}{("."+TABLE_FUNCTION_END_PARTITION_METHOD) if object_type == TempObjectType.TABLE_FUNCTION else ""}._sf_vectorized_input = pandas.DataFrame
""".rstrip()
            if max_batch_size:
                pandas_code = f"""
{pandas_code}
{_DEFAULT_HANDLER_NAME}._sf_max_batch_size = {int(max_batch_size)}
""".rstrip()
            func_code = f"""
{func_code}
{pandas_code}
""".rstrip()

    return f"""
{deserialization_code}
{func_code}
""".strip()


def resolve_imports_and_packages(
    session: "snowflake.snowpark.Session",
    object_type: TempObjectType,
    func: Union[Callable, Tuple[str, str]],
    arg_names: List[str],
    udf_name: str,
    stage_location: Optional[str],
    imports: Optional[List[Union[str, Tuple[str, str]]]],
    packages: Optional[List[Union[str, ModuleType]]],
    parallel: int = 4,
    is_pandas_udf: bool = False,
    is_dataframe_input: bool = False,
    max_batch_size: Optional[int] = None,
    *,
    statement_params: Optional[Dict[str, str]] = None,
    source_code_display: bool = False,
    skip_upload_on_content_match: bool = False,
    is_permanent: bool = False,
) -> Tuple[str, str, str, str, str, bool]:
    import_only_stage = (
        unwrap_stage_location_single_quote(stage_location)
        if stage_location
        else session.get_session_stage()
    )

    upload_and_import_stage = (
        import_only_stage if is_permanent else session.get_session_stage()
    )

    # resolve packages
    resolved_packages = (
        session._resolve_packages(packages, include_pandas=is_pandas_udf)
        if packages is not None
        else session._resolve_packages(
            [],
            session._packages,
            validate_package=False,
            include_pandas=is_pandas_udf,
        )
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
                    f"{get_error_message_abbr(object_type).replace(' ', '-')}-level import can only be a file path (str) "
                    "or a tuple of the file path (str) and the import path (str)."
                )
            udf_level_imports[resolved_import_tuple[0]] = resolved_import_tuple[1:]
        all_urls = session._resolve_imports(
            import_only_stage,
            upload_and_import_stage,
            udf_level_imports,
            statement_params=statement_params,
        )
    elif imports is None:
        all_urls = session._resolve_imports(
            import_only_stage,
            upload_and_import_stage,
            statement_params=statement_params,
        )
    else:
        all_urls = []

    dest_prefix = get_udf_upload_prefix(udf_name)

    # Upload closure to stage if it is beyond inline closure size limit
    if isinstance(func, Callable):
        custom_python_runtime_version_allowed = (
            False  # As cloudpickle is being used, we cannot allow a custom runtime
        )

        # generate a random name for udf py file
        # and we compress it first then upload it
        udf_file_name_base = f"udf_py_{random_number()}"
        udf_file_name = f"{udf_file_name_base}.zip"
        code = generate_python_code(
            func,
            arg_names,
            object_type,
            is_pandas_udf,
            is_dataframe_input,
            max_batch_size,
            source_code_display=source_code_display,
        )
        if len(code) > _MAX_INLINE_CLOSURE_SIZE_BYTES:
            dest_prefix = get_udf_upload_prefix(udf_name)
            upload_file_stage_location = normalize_remote_file_or_dir(
                f"{upload_and_import_stage}/{dest_prefix}/{udf_file_name}"
            )
            udf_file_name_base = os.path.splitext(udf_file_name)[0]
            with io.BytesIO() as input_stream:
                with zipfile.ZipFile(
                    input_stream, mode="w", compression=zipfile.ZIP_DEFLATED
                ) as zf:
                    zf.writestr(f"{udf_file_name_base}.py", code)
                session._conn.upload_stream(
                    input_stream=input_stream,
                    stage_location=upload_and_import_stage,
                    dest_filename=udf_file_name,
                    dest_prefix=dest_prefix,
                    parallel=parallel,
                    source_compression="DEFLATE",
                    compress_data=False,
                    overwrite=True,
                    is_in_udf=True,
                    skip_upload_on_content_match=skip_upload_on_content_match,
                )
            all_urls.append(upload_file_stage_location)
            inline_code = None
            handler = f"{udf_file_name_base}.{_DEFAULT_HANDLER_NAME}"
        else:
            inline_code = code
            upload_file_stage_location = None
            handler = _DEFAULT_HANDLER_NAME
    else:
        custom_python_runtime_version_allowed = True
        udf_file_name = os.path.basename(func[0])
        # for a compressed file, it might have multiple extensions
        # and we should remove all extensions
        udf_file_name_base = udf_file_name.split(".")[0]
        inline_code = None
        handler = f"{udf_file_name_base}.{func[1]}"

        if func[0].startswith(STAGE_PREFIX):
            upload_file_stage_location = None
            all_urls.append(func[0])
        else:
            upload_file_stage_location = normalize_remote_file_or_dir(
                f"{upload_and_import_stage}/{dest_prefix}/{udf_file_name}"
            )
            session._conn.upload_file(
                path=func[0],
                stage_location=upload_and_import_stage,
                dest_prefix=dest_prefix,
                parallel=parallel,
                compress_data=False,
                overwrite=True,
                skip_upload_on_content_match=skip_upload_on_content_match,
            )
            all_urls.append(upload_file_stage_location)

    # build imports and packages string
    all_imports = ",".join(
        [url if is_single_quoted(url) else f"'{url}'" for url in all_urls]
    )
    all_packages = ",".join([f"'{package}'" for package in resolved_packages])
    return (
        handler,
        inline_code,
        all_imports,
        all_packages,
        upload_file_stage_location,
        custom_python_runtime_version_allowed,
    )


def create_python_udf_or_sp(
    session: "snowflake.snowpark.Session",
    return_type: DataType,
    input_args: List[UDFColumn],
    handler: str,
    object_type: TempObjectType,
    object_name: str,
    all_imports: str,
    all_packages: str,
    is_permanent: bool,
    replace: bool,
    if_not_exists: bool,
    inline_python_code: Optional[str] = None,
    execute_as: Optional[typing.Literal["caller", "owner"]] = None,
    api_call_source: Optional[str] = None,
    strict: bool = False,
    secure: bool = False,
    external_access_integrations: Optional[List[str]] = None,
    secrets: Optional[Dict[str, str]] = None,
    immutable: bool = False,
) -> None:
    runtime_version = (
        f"{sys.version_info[0]}.{sys.version_info[1]}"
        if not session._runtime_version_from_requirement
        else session._runtime_version_from_requirement
    )
    if replace and if_not_exists:
        raise ValueError("options replace and if_not_exists are incompatible")
    if isinstance(return_type, StructType):
        return_sql = f'RETURNS TABLE ({",".join(f"{field.name} {convert_sp_to_sf_type(field.datatype)}" for field in return_type.fields)})'
    elif installed_pandas and isinstance(return_type, PandasDataFrameType):
        return_sql = f'RETURNS TABLE ({",".join(f"{name} {convert_sp_to_sf_type(datatype)}" for name, datatype in zip(return_type.col_names, return_type.col_types))})'
    else:
        return_sql = f"RETURNS {convert_sp_to_sf_type(return_type)}"
    input_sql_types = [convert_sp_to_sf_type(arg.datatype) for arg in input_args]
    sql_func_args = ",".join(
        [f"{a.name} {t}" for a, t in zip(input_args, input_sql_types)]
    )
    imports_in_sql = f"IMPORTS=({all_imports})" if all_imports else ""
    packages_in_sql = f"PACKAGES=({all_packages})" if all_packages else ""
    # Since this function is called for UDFs and Stored Procedures we need to
    #  make execute_as_sql a multi-line string for cases when we need it.
    #  This makes sure that when we don't need it we don't end up inserting
    #  totally empty lines.
    if execute_as is None:
        execute_as_sql = ""
    else:
        execute_as_sql = f"""
EXECUTE AS {execute_as.upper()}
"""
    inline_python_code_in_sql = (
        f"""
AS $$
{inline_python_code}
$$
"""
        if inline_python_code
        else ""
    )
    mutability = "IMMUTABLE" if immutable else "VOLATILE"
    strict_as_sql = "\nSTRICT" if strict else ""

    external_access_integrations_in_sql = (
        f"\nEXTERNAL_ACCESS_INTEGRATIONS=({','.join(external_access_integrations)})"
        if external_access_integrations
        else ""
    )
    secrets_in_sql = (
        f"""\nSECRETS=({",".join([f"'{k}'={v}" for k, v in secrets.items()])})"""
        if secrets
        else ""
    )

    create_query = f"""
CREATE{" OR REPLACE " if replace else ""}
{"" if is_permanent else "TEMPORARY"} {"SECURE" if secure else ""} {object_type.value.replace("_", " ")} {"IF NOT EXISTS" if if_not_exists else ""} {object_name}({sql_func_args})
{return_sql}
LANGUAGE PYTHON {strict_as_sql}
{mutability}
RUNTIME_VERSION={runtime_version}
{imports_in_sql}
{packages_in_sql}
{external_access_integrations_in_sql}
{secrets_in_sql}
HANDLER='{handler}'{execute_as_sql}
{inline_python_code_in_sql}
"""
    session._run_query(create_query, is_ddl_on_temp_object=not is_permanent)

    # fire telemetry after _run_query is successful
    api_call_source = api_call_source or "_internal.create_python_udf_or_sp"
    telemetry_client = session._conn._telemetry_client
    telemetry_client.send_function_usage_telemetry(
        api_call_source, TelemetryField.FUNC_CAT_CREATE.value
    )


def generate_anonymous_python_sp_sql(
    return_type: DataType,
    input_args: List[UDFColumn],
    handler: str,
    object_name: str,
    all_imports: str,
    all_packages: str,
    inline_python_code: Optional[str] = None,
    strict: bool = False,
    runtime_version: Optional[str] = None,
    external_access_integrations: Optional[List[str]] = None,
    secrets: Optional[Dict[str, str]] = None,
):
    runtime_version = (
        f"{sys.version_info[0]}.{sys.version_info[1]}"
        if not runtime_version
        else runtime_version
    )
    if isinstance(return_type, StructType):
        return_sql = f'RETURNS TABLE ({",".join(f"{field.name} {convert_sp_to_sf_type(field.datatype)}" for field in return_type.fields)})'
    else:
        return_sql = f"RETURNS {convert_sp_to_sf_type(return_type)}"
    input_sql_types = [convert_sp_to_sf_type(arg.datatype) for arg in input_args]
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
    strict_as_sql = "\nSTRICT" if strict else ""
    external_access_integrations_in_sql = (
        f"\nEXTERNAL_ACCESS_INTEGRATIONS=({','.join(external_access_integrations)})"
        if external_access_integrations
        else ""
    )
    secrets_in_sql = (
        f"""\nSECRETS=({",".join([f"'{k}'={v}" for k, v in secrets.items()])})"""
        if secrets
        else ""
    )

    sql = f"""
WITH {object_name} AS PROCEDURE ({sql_func_args})
{return_sql}
LANGUAGE PYTHON {strict_as_sql}
RUNTIME_VERSION={runtime_version}
{imports_in_sql}
{packages_in_sql}
{external_access_integrations_in_sql}
{secrets_in_sql}
HANDLER='{handler}'
{inline_python_code_in_sql}
"""
    return sql


def generate_call_python_sp_sql(
    session: "snowflake.snowpark.Session", sproc_name: str, *args: Any
) -> str:
    sql_args = []
    for arg in args:
        if isinstance(arg, snowflake.snowpark.Column):
            sql_args.append(session._analyzer.analyze(arg._expression, {}))
        else:
            sql_args.append(to_sql(arg, infer_type(arg)))
    return f"CALL {sproc_name}({', '.join(sql_args)})"
