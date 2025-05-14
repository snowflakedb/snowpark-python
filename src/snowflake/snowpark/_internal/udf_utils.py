#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
import collections.abc
import inspect
import io
import os
import pickle
import sys
import typing
import zipfile
from copy import deepcopy
from enum import Enum
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
from snowflake.snowpark._internal.analyzer.datatype_mapper import to_sql, to_sql_no_cast
from snowflake.snowpark._internal.telemetry import TelemetryField
from snowflake.snowpark._internal.type_utils import (
    NoneType,
    convert_sp_to_sf_type,
    infer_type,
    python_type_str_to_object,
    python_type_to_snow_type,
    python_value_str_to_object,
    retrieve_func_defaults_from_source,
    retrieve_func_type_hints_from_source,
)
from snowflake.snowpark._internal.utils import (
    STAGE_PREFIX,
    TempObjectType,
    escape_single_quotes,
    get_udf_upload_prefix,
    is_single_quoted,
    normalize_remote_file_or_dir,
    random_name_for_temp_object,
    random_number,
    unwrap_stage_location_single_quote,
    validate_object_name,
    warning,
)
from snowflake.snowpark.types import DataType, StructField, StructType
from snowflake.snowpark.version import VERSION

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

EXECUTE_AS_WHITELIST = frozenset(["owner", "caller", "restricted caller"])

REGISTER_KWARGS_ALLOWLIST = {
    "native_app_params",
    "anonymous",
    "force_inline_code",
    "_from_pandas_udf_function",
    "input_names",  # for pandas_udtf
    "max_batch_size",  # for pandas_udtf
    "_registered_object_name",  # object name within Snowflake (post registration)
}

ALLOWED_CONSTRAINT_CONFIGURATION = {"architecture": {"x86"}}


class UDFColumn(NamedTuple):
    datatype: DataType
    name: str


class RegistrationType(Enum):
    UDF = "UDF"
    UDAF = "UDAF"
    UDTF = "UDTF"
    SPROC = "SPROC"


class ExtensionFunctionProperties:
    """
    This is a data class to hold all information, resolved or otherwise, about a UDF/UDTF/UDAF/Sproc object
    that we want to create in a user's Snowflake account.
    One of the use cases of this class is to be able to pass on information to a callback that may be installed
    in the execution environment, such as for testing.
    """

    def __init__(
        self,
        object_type: TempObjectType,
        object_name: str,
        input_args: List[UDFColumn],
        input_sql_types: List[str],
        return_sql: str,
        runtime_version: str,
        all_imports: Optional[str],
        all_packages: str,
        handler: Optional[str],
        external_access_integrations: Optional[List[str]],
        secrets: Optional[Dict[str, str]],
        inline_python_code: Optional[str],
        native_app_params: Optional[Dict[str, Any]],
        raw_imports: Optional[List[Union[str, Tuple[str, str]]]],
        func: Union[Callable, Tuple[str, str]],
        replace: bool = False,
        if_not_exists: bool = False,
        execute_as: Optional[
            typing.Literal["caller", "owner", "restricted caller"]
        ] = None,
        anonymous: bool = False,
    ) -> None:
        self.func = func
        self.replace = replace
        self.object_type = object_type
        self.if_not_exists = if_not_exists
        self.object_name = object_name
        self.input_args = deepcopy(input_args)
        self.input_sql_types = input_sql_types
        self.return_sql = return_sql
        self.runtime_version = runtime_version
        self.all_imports = all_imports
        self.all_packages = all_packages
        self.external_access_integrations = deepcopy(external_access_integrations)
        self.secrets = deepcopy(secrets)
        self.handler = handler
        self.execute_as = execute_as
        self.inline_python_code = inline_python_code
        self.native_app_params = deepcopy(native_app_params)
        self.raw_imports = deepcopy(raw_imports)
        self.anonymous = anonymous


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


def get_opt_arg_defaults(
    func: Union[Callable, Tuple[str, str]],
    object_type: TempObjectType,
    input_types: List[DataType],
) -> List[Optional[str]]:
    EMPTY_DEFAULT_VALUES = [None] * len(input_types)

    def build_default_values_result(
        default_values: Any,
        input_types: List[DataType],
        convert_python_str_to_object: bool,
    ) -> List[Optional[str]]:
        if default_values is None:
            return EMPTY_DEFAULT_VALUES
        num_optional_args = len(default_values)
        num_positional_args = len(input_types) - num_optional_args
        input_types_for_default_args = input_types[-num_optional_args:]
        if convert_python_str_to_object:
            default_values = [
                python_value_str_to_object(value, tp)
                for value, tp in zip(default_values, input_types_for_default_args)
            ]

        if num_optional_args != 0:
            default_values_to_sql_str = [
                to_sql(value, datatype)
                for value, datatype in zip(default_values, input_types_for_default_args)
            ]
        else:
            default_values_to_sql_str = []
        return [None] * num_positional_args + default_values_to_sql_str

    def get_opt_arg_defaults_from_callable():
        target_func = None
        if object_type == TempObjectType.TABLE_FUNCTION:
            # extract from process method
            if hasattr(func, TABLE_FUNCTION_PROCESS_METHOD):
                target_func = getattr(func, TABLE_FUNCTION_PROCESS_METHOD)
        if object_type in (TempObjectType.PROCEDURE, TempObjectType.FUNCTION):
            # sproc and udf
            target_func = func

        if target_func is None:
            return EMPTY_DEFAULT_VALUES

        arg_spec = inspect.getfullargspec(target_func)
        return build_default_values_result(arg_spec.defaults, input_types, False)

    def get_opt_arg_defaults_from_file():
        filename, func_name = func[0], func[1]
        default_values_str = None
        if not is_local_python_file(filename):
            return EMPTY_DEFAULT_VALUES

        if object_type == TempObjectType.TABLE_FUNCTION:
            default_values_str = retrieve_func_defaults_from_source(
                filename, TABLE_FUNCTION_PROCESS_METHOD, func_name
            )
        elif object_type in (TempObjectType.FUNCTION, TempObjectType.PROCEDURE):
            default_values_str = retrieve_func_defaults_from_source(filename, func_name)

        return build_default_values_result(default_values_str, input_types, True)

    try:
        if isinstance(func, Callable):
            return get_opt_arg_defaults_from_callable()
        else:
            return get_opt_arg_defaults_from_file()
    except TypeError as e:
        logger.warn(
            f"Got error {e} when trying to read default values from function: {func}. "
            "Proceeding without creating optional arguments"
        )
        return EMPTY_DEFAULT_VALUES


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


def check_decorator_args(**kwargs):
    for key, _ in kwargs.items():
        if key not in REGISTER_KWARGS_ALLOWLIST:
            raise ValueError(
                f"Invalid key-value argument passed to the decorator: {key}"
            )


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


def check_execute_as_arg(
    execute_as: typing.Literal["caller", "owner", "restricted caller"]
):
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


def check_resource_constraint(constraint: Optional[Dict[str, str]]):
    if constraint is None:
        return

    errors = []
    for key, value in constraint.items():
        if key.lower() not in ALLOWED_CONSTRAINT_CONFIGURATION:
            errors.append(ValueError(f"Unknown resource constraint key '{key}'"))
            continue
        if value.lower() not in ALLOWED_CONSTRAINT_CONFIGURATION[key]:
            errors.append(ValueError(f"Unknown value '{value}' for key '{key}'"))

    if errors:
        raise Exception(errors)


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
) -> Tuple[str, bool, bool, DataType, List[DataType], List[Optional[str]]]:
    """

    Args:
        output_schema: List of column names of in the output, only applicable to UDTF
    """
    if name:
        object_name = name if isinstance(name, str) else ".".join(name)
    else:
        object_name = random_name_for_temp_object(object_type)
        if (not anonymous) and (session is not None):
            object_name = session.get_fully_qualified_name_if_possible(object_name)
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
    if is_pandas_udf or is_dataframe_input:
        # vectorized UDF/UDTF does not support default values. They only
        # take a single input which is of type DataFrame
        opt_arg_defaults = [None] * len(input_types)
    else:
        opt_arg_defaults = get_opt_arg_defaults(func, object_type, input_types)

    return (
        object_name,
        is_pandas_udf,
        is_dataframe_input,
        return_type,
        input_types,
        opt_arg_defaults,
    )


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
        annotated_funcs = []
        # clean-up annotations from process and end_partition methods if they are defined
        if hasattr(func, TABLE_FUNCTION_PROCESS_METHOD):
            annotated_funcs.append(getattr(func, TABLE_FUNCTION_PROCESS_METHOD))
        if hasattr(func, TABLE_FUNCTION_END_PARTITION_METHOD):
            annotated_funcs.append(getattr(func, TABLE_FUNCTION_END_PARTITION_METHOD))
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
                end_partition_vectorized = is_pandas_udf and not hasattr(
                    func, TABLE_FUNCTION_PROCESS_METHOD
                )
                func_code = f"""{func_code}
    def end_partition(self, {wrapper_params if end_partition_vectorized else ""}):
        return lock_function_once(super().end_partition, end_partition_invoked)({func_args if end_partition_vectorized else ""})
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
            vectorized_sub_component = ""
            if object_type == TempObjectType.TABLE_FUNCTION:
                if hasattr(func, TABLE_FUNCTION_PROCESS_METHOD):
                    vectorized_sub_component = f".{TABLE_FUNCTION_PROCESS_METHOD}"
                else:
                    vectorized_sub_component = f".{TABLE_FUNCTION_END_PARTITION_METHOD}"
            pandas_code = f"""
import pandas

{_DEFAULT_HANDLER_NAME}{vectorized_sub_component}._sf_vectorized_input = pandas.DataFrame
""".rstrip()

            if max_batch_size:
                max_batch_size_sub_component = ""
                if object_type == TempObjectType.TABLE_FUNCTION and hasattr(
                    func, TABLE_FUNCTION_PROCESS_METHOD
                ):
                    max_batch_size_sub_component = f".{TABLE_FUNCTION_PROCESS_METHOD}"
                pandas_code = f"""
{pandas_code}
{_DEFAULT_HANDLER_NAME}{max_batch_size_sub_component}._sf_max_batch_size = {int(max_batch_size)}
""".rstrip()
            func_code = f"""
{func_code}
{pandas_code}
""".rstrip()

    return f"""
{deserialization_code}
{func_code}
""".strip()


def add_snowpark_package_to_sproc_packages(
    session: Optional["snowflake.snowpark.Session"],
    packages: Optional[List[Union[str, ModuleType]]],
) -> List[Union[str, ModuleType]]:
    major, minor, patch = VERSION
    package_name = "snowflake-snowpark-python"
    # Use == to ensure that the remote version matches the local version
    this_package = f"{package_name}=={major}.{minor}.{patch}"

    # When resolve_imports_and_packages is called below it will use the provided packages or
    # default to the packages in the current session. If snowflake-snowpark-python is not
    # included by either of those two mechanisms then create package list does include it and
    # any other relevant packages.
    if packages is None:
        if session is None:
            packages = [this_package]
        else:
            with session._package_lock:
                if package_name not in session._packages:
                    packages = list(session._packages.values()) + [this_package]
        return packages

    return add_package_to_existing_packages(packages, package_name, this_package)


def add_package_to_existing_packages(
    packages: Optional[List[Union[str, ModuleType]]],
    package: Union[str, ModuleType],
    package_spec: Optional[str] = None,
) -> List[Union[str, ModuleType]]:
    if packages is None:
        return [package]
    package_name = package if isinstance(package, str) else package.__name__
    package_names = [p if isinstance(p, str) else p.__name__ for p in packages]
    if not any(p.startswith(package_name) for p in package_names):
        packages.append(package_spec or package)
    return packages


def resolve_packages_in_client_side_sandbox(
    packages: Optional[List[Union[str, ModuleType]]],
) -> List[str]:
    """
    Special function invoked only when executing Snowpark code in a sandbox environment created for a client,
    which is different from regular XP sandbox.
    """
    resolved_packages: List[str] = []
    if packages is not None:
        parsed_packages = snowflake.snowpark.Session._parse_packages(packages)
        # Similar to local testing we don't resolve the packages, we just return what is added
        errors = []
        resolved_packages_dict: Dict[str, str] = {}

        for pkg_name, _, pkg_req in parsed_packages.values():
            if (
                pkg_name in resolved_packages_dict
                and str(pkg_req) != resolved_packages_dict[pkg_name]
            ):
                errors.append(
                    ValueError(
                        f"Cannot add package '{str(pkg_req)}' because {resolved_packages_dict[pkg_name]} "
                        "is already added."
                    )
                )
            else:
                resolved_packages_dict[pkg_name] = str(pkg_req)
        if len(errors) == 1:
            raise errors[0]
        elif len(errors) > 0:
            raise RuntimeError(errors)
        resolved_packages = list(resolved_packages_dict.values())
    return resolved_packages


# TODO: SNOW-1338175 to add override function definition.
def resolve_imports_and_packages(
    session: Optional["snowflake.snowpark.Session"],
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
    force_inline_code: bool = False,
) -> Tuple[
    Optional[str],
    Optional[str],
    Optional[str],
    Optional[str],
    Optional[str],
    bool,
]:

    # resolve packages
    if session is None:  # In case of sandbox
        resolved_packages = resolve_packages_in_client_side_sandbox(packages=packages)
    else:  # In any other scenario
        resolved_packages = (
            session._resolve_packages(
                packages,
                include_pandas=is_pandas_udf,
                statement_params=statement_params,
            )
            if packages is not None
            else session._resolve_packages(
                [],
                session._packages,
                validate_package=False,
                include_pandas=is_pandas_udf,
                statement_params=statement_params,
            )
        )

    all_urls = []
    if session is not None:
        import_only_stage = (
            unwrap_stage_location_single_quote(stage_location)
            if stage_location
            else session.get_session_stage(statement_params=statement_params)
        )

        upload_and_import_stage = (
            import_only_stage
            if is_permanent
            else session.get_session_stage(statement_params=statement_params)
        )

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

    dest_prefix = get_udf_upload_prefix(udf_name)

    # Upload closure to stage if it is beyond inline closure size limit
    handler = inline_code = upload_file_stage_location = None
    # As cloudpickle is being used, we cannot allow a custom runtime
    custom_python_runtime_version_allowed = not isinstance(func, Callable)
    if session is not None:
        if isinstance(func, Callable):
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
            if not force_inline_code and len(code) > _MAX_INLINE_CLOSURE_SIZE_BYTES:
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
    session: Optional["snowflake.snowpark.Session"],
    func: Union[Callable, Tuple[str, str]],
    return_type: DataType,
    input_args: List[UDFColumn],
    opt_arg_defaults: List[Optional[str]],
    handler: Optional[str],
    object_type: TempObjectType,
    object_name: str,
    all_imports: Optional[str],
    all_packages: str,
    is_permanent: bool,
    replace: bool,
    if_not_exists: bool,
    raw_imports: Optional[List[Union[str, Tuple[str, str]]]],
    registration_type: RegistrationType,
    inline_python_code: Optional[str] = None,
    execute_as: Optional[typing.Literal["caller", "owner", "restricted caller"]] = None,
    api_call_source: Optional[str] = None,
    strict: bool = False,
    secure: bool = False,
    external_access_integrations: Optional[List[str]] = None,
    secrets: Optional[Dict[str, str]] = None,
    immutable: bool = False,
    statement_params: Optional[Dict[str, str]] = None,
    comment: Optional[str] = None,
    native_app_params: Optional[Dict[str, Any]] = None,
    copy_grants: bool = False,
    runtime_version: Optional[str] = None,
    artifact_repository: Optional[str] = None,
    artifact_repository_packages: Optional[List[str]] = None,
    resource_constraint: Optional[Dict[str, str]] = None,
) -> None:
    runtime_version = runtime_version or f"{sys.version_info[0]}.{sys.version_info[1]}"
    check_resource_constraint(resource_constraint)

    if replace and if_not_exists:
        raise ValueError("options replace and if_not_exists are incompatible")

    if (
        isinstance(return_type, StructType)
        and not return_type.structured
        and registration_type in {RegistrationType.UDTF, RegistrationType.SPROC}
    ):
        return_sql = f'RETURNS TABLE ({",".join(f"{field.name} {convert_sp_to_sf_type(field.datatype)}" for field in return_type.fields)})'
    elif installed_pandas and isinstance(return_type, PandasDataFrameType):
        return_sql = f'RETURNS TABLE ({",".join(f"{name} {convert_sp_to_sf_type(datatype)}" for name, datatype in zip(return_type.col_names, return_type.col_types))})'
    else:
        return_sql = f"RETURNS {convert_sp_to_sf_type(return_type)}"
    input_sql_types = [convert_sp_to_sf_type(arg.datatype) for arg in input_args]
    sql_func_args = ",".join(
        [
            f"{a.name} {t}{f' DEFAULT {value}' if value else ''}"
            for a, t, value in zip(input_args, input_sql_types, opt_arg_defaults)
        ]
    )
    imports_in_sql = f"IMPORTS=({all_imports})" if all_imports else ""
    packages_in_sql = f"PACKAGES=({all_packages})" if all_packages else ""

    if artifact_repository_packages and not artifact_repository:
        raise ValueError(
            "artifact_repository must be specified when artifact_repository_packages has been specified"
        )
    if all_packages and artifact_repository_packages:
        package_names = [
            pack.strip("\"'").split("==")[0] for pack in all_packages.split(",")
        ]
        artifact_repository_package_names = [
            pack.strip("\"'").split("==")[0] for pack in artifact_repository_packages
        ]

        for package in package_names:
            if package in artifact_repository_package_names:
                raise ValueError(
                    f"Cannot create a function with duplicates between packages and artifact repository packages. packages: {all_packages}, artifact_repository_packages: {','.join(artifact_repository_packages)}"
                )

    artifact_repository_in_sql = (
        f"ARTIFACT_REPOSITORY={artifact_repository}" if artifact_repository else ""
    )
    if artifact_repository:
        artifact_repository_packages = {
            *list(session._artifact_repository_packages[artifact_repository].values()),
            *(artifact_repository_packages or []),
        }
    artifact_repository_packages_str = (
        "','".join(artifact_repository_packages) if artifact_repository_packages else ""
    )
    artifact_repository_packages_in_sql = (
        f"ARTIFACT_REPOSITORY_PACKAGES=('{artifact_repository_packages_str}')"
        if artifact_repository_packages
        else ""
    )

    resource_constraint_fmt = (
        ""
        if resource_constraint is None
        else ",".join(f"{k}='{v}'" for k, v in resource_constraint.items())
    )
    resource_constraint_sql = (
        f"RESOURCE_CONSTRAINT=({resource_constraint_fmt})"
        if resource_constraint_fmt
        else ""
    )
    if artifact_repository_in_sql or artifact_repository_packages_in_sql:
        warning(
            "artifact_repository_support",
            "Support for artifact_repository udxf options is experimental since v1.29.0. Do not use it in production.",
        )
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

    # As an FYI, _should_continue_registration is a function, and is defined outside the Snowpark context.
    if snowflake.snowpark.context._should_continue_registration is None:
        continue_registration = True
    else:
        extension_function_properties = ExtensionFunctionProperties(
            func=func,
            replace=replace,
            object_type=object_type,
            if_not_exists=if_not_exists,
            object_name=object_name,
            input_args=input_args,
            input_sql_types=input_sql_types,
            return_sql=return_sql,
            runtime_version=runtime_version,
            all_imports=all_imports,
            all_packages=all_packages,
            external_access_integrations=external_access_integrations,
            secrets=secrets,
            handler=handler,
            execute_as=execute_as,
            inline_python_code=inline_python_code,
            native_app_params=native_app_params,
            raw_imports=raw_imports,
        )
        continue_registration = (
            snowflake.snowpark.context._should_continue_registration(
                extension_function_properties
            )
        )

    # This means the execution environment does not want to continue creating the object in Snowflake
    if not bool(continue_registration):
        return

    create_query = f"""
CREATE{" OR REPLACE " if replace else ""}
{"" if is_permanent else "TEMPORARY"} {"SECURE" if secure else ""} {object_type.value.replace("_", " ")} {"IF NOT EXISTS" if if_not_exists else ""} {object_name}({sql_func_args})
{" COPY GRANTS " if copy_grants else ""}
{return_sql}
LANGUAGE PYTHON {strict_as_sql}
{mutability}
RUNTIME_VERSION={runtime_version}
{imports_in_sql}
{packages_in_sql}
{artifact_repository_in_sql}
{artifact_repository_packages_in_sql}
{external_access_integrations_in_sql}
{resource_constraint_sql}
{secrets_in_sql}
HANDLER='{handler}'{execute_as_sql}
{inline_python_code_in_sql}
"""
    session._run_query(
        create_query,
        is_ddl_on_temp_object=not is_permanent,
        statement_params=statement_params,
    )

    if comment is not None:
        object_signature_sql = f"{object_name}({','.join(input_sql_types)})"
        comment = escape_single_quotes(comment)
        comment_query = f"COMMENT ON {object_type.value.split('_')[-1]} {object_signature_sql} IS '{comment}'"
        session._run_query(
            comment_query,
            is_ddl_on_temp_object=not is_permanent,
            statement_params=statement_params,
        )

    # fire telemetry after _run_query is successful
    api_call_source = api_call_source or "_internal.create_python_udf_or_sp"
    telemetry_client = session._conn._telemetry_client
    telemetry_client.send_function_usage_telemetry(
        api_call_source, TelemetryField.FUNC_CAT_CREATE.value
    )


def generate_anonymous_python_sp_sql(
    func: Union[Callable, Tuple[str, str]],
    return_type: DataType,
    input_args: List[UDFColumn],
    handler: str,
    object_name: str,
    all_imports: str,
    all_packages: str,
    raw_imports: Optional[List[Union[str, Tuple[str, str]]]],
    inline_python_code: Optional[str] = None,
    strict: bool = False,
    runtime_version: Optional[str] = None,
    external_access_integrations: Optional[List[str]] = None,
    secrets: Optional[Dict[str, str]] = None,
    native_app_params: Optional[Dict[str, Any]] = None,
) -> str:
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

    # As an FYI, _should_continue_registration is a function, and is defined outside the Snowpark context.
    if snowflake.snowpark.context._should_continue_registration is not None:
        extension_function_properties = ExtensionFunctionProperties(
            anonymous=True,
            object_type=TempObjectType.PROCEDURE,
            object_name=object_name,
            input_args=input_args,
            input_sql_types=input_sql_types,
            return_sql=return_sql,
            runtime_version=runtime_version,
            all_imports=all_imports,
            all_packages=all_packages,
            external_access_integrations=external_access_integrations,
            secrets=secrets,
            handler=handler,
            inline_python_code=inline_python_code,
            native_app_params=native_app_params,
            raw_imports=raw_imports,
            func=func,
        )
        # The result of the function call below does not matter because we are not using session object here
        snowflake.snowpark.context._should_continue_registration(
            extension_function_properties
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
        elif "system$" in sproc_name.lower():
            sql_args.append(to_sql_no_cast(arg, infer_type(arg)))
        else:
            sql_args.append(to_sql(arg, infer_type(arg)))
    return f"CALL {sproc_name}({', '.join(sql_args)})"
