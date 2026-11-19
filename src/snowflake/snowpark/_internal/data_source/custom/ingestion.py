#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
import functools
import logging
import os
import pickle
import re
import site
import sys
import sysconfig
import threading
import time
import types
from collections import defaultdict
from contextlib import contextmanager
from io import BytesIO
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Iterator,
    List,
    Optional,
    Sequence,
    Tuple,
    Type,
    Union,
)

import cloudpickle

import snowflake.snowpark.datasource as datasource_module
from snowflake.snowpark._internal.data_source.custom import conversion
from snowflake.snowpark._internal.data_source.custom import options as options_module
from snowflake.snowpark._internal.data_source.custom.conversion import (
    FieldMatcher,
    arrow_table_to_parquet_bytesio,
    arrow_to_staged_table,
    cast_udtf_output_columns,
    check_not_null,
    convert_row,
    is_arrow_data,
    normalize_items,
    parse_semi_structured_columns,
    rows_to_arrow_table,
    staging_schema,
    to_row_tuple,
    udtf_output_schema,
    udtf_row_converters,
    validate_schema,
    value_converters,
)
from snowflake.snowpark._internal.data_source.custom.options import (
    EngineOptions,
    split_options,
)
from snowflake.snowpark._internal.data_source.custom.pyspark_compat import (
    PYSPARK_PACKAGE,
    is_pyspark_class,
    is_pyspark_struct_type,
    pyspark_struct_type_to_snowpark,
    to_pyspark_struct_type,
    uses_pyspark,
    with_spark_timestamp_semantics,
)
from snowflake.snowpark._internal.data_source.drivers.base_driver import BaseDriver
from snowflake.snowpark._internal.data_source.utils import (
    create_data_source_table_and_stage,
    local_ingestion,
)
from snowflake.snowpark._internal.telemetry import set_api_call_source
from snowflake.snowpark._internal.utils import (
    TempObjectType,
    random_name_for_temp_object,
)
from snowflake.snowpark.datasource import DataSourceLike
from snowflake.snowpark.exceptions import SnowparkDataframeReaderException
from snowflake.snowpark.types import BinaryType, LongType, StructField, StructType

if TYPE_CHECKING:
    import pyarrow

    import snowflake.snowpark

logger = logging.getLogger(__name__)

DATA_SOURCE_CUSTOM_SIGNATURE = "DataFrameReader.custom_data_source"
STATEMENT_PARAMS_DATA_SOURCE_CUSTOM = "SNOWPARK_PYTHON_DATASOURCE_CUSTOM"
DATA_SOURCE_CUSTOM_SQL_COMMENT = (
    f"/* Python:snowflake.snowpark.{DATA_SOURCE_CUSTOM_SIGNATURE} */"
)
_PARTITION_IDX_COLUMN = "SNOWPARK_DATA_SOURCE_PARTITION_IDX"
_PARTITION_COLUMN = "SNOWPARK_DATA_SOURCE_PARTITION"
_SNOWPARK_PACKAGE = "snowflake-snowpark-python"

# Modules pickled by value in server-side ingestion, so that the UDTF does not depend on
# the Snowpark version available in Snowflake having them. options is included because readers
# commonly keep DataSource.options, a CaseInsensitiveDict.
_PICKLE_BY_VALUE_MODULES = (datasource_module, conversion, options_module)
_pickle_by_value_lock = threading.Lock()
# bounds of the search for local modules in the state of the reader and partitions
_MAX_STATE_DEPTH = 20
_MAX_STATE_VALUES = 100_000


def _installed_package_paths() -> List[str]:
    paths = set()
    if hasattr(site, "getsitepackages"):  # not available in some virtualenvs
        paths.update(site.getsitepackages())
    if hasattr(site, "getusersitepackages"):
        paths.add(site.getusersitepackages())
    paths.update(
        sysconfig.get_paths()[key]
        for key in ("stdlib", "platstdlib", "purelib", "platlib")
    )
    return [os.path.realpath(p) for p in paths if p]


def _is_local_module(
    module: Optional[types.ModuleType], installed_paths: List[str]
) -> bool:
    if (
        module is None
        or module.__name__ in ("__main__", "builtins")
        or module.__name__.split(".")[0] == "snowflake"
    ):
        return False
    path = getattr(module, "__file__", None)
    if path is None:
        return False
    path = os.path.realpath(path)
    return not any(path == p or path.startswith(p + os.sep) for p in installed_paths)


def _imported_paths(imports: Optional[Sequence[Any]]) -> List[str]:
    """The real paths of the local files and directories in the ``snowpark.imports`` option."""
    paths = []
    for entry in imports or []:
        if isinstance(entry, (tuple, list)):  # (path, import_path)
            entry = entry[0]
        if isinstance(entry, (str, os.PathLike)) and not str(entry).startswith("@"):
            paths.append(os.path.realpath(entry))
    return paths


def _local_modules_of(
    objects: List[Any], imports: Optional[Sequence[Any]] = None
) -> List[types.ModuleType]:
    """Returns the local (not installed) modules that define the classes of ``objects``, and the
    local modules they use. They are pickled by value, so that the UDTF can unpickle the reader
    and partitions without the user adding these modules to ``snowpark.imports``. The modules
    that the user uploads with ``snowpark.imports`` are pickled by reference."""
    # an installed package is not pickled by value, and neither is an uploaded module
    installed_paths = _installed_package_paths() + _imported_paths(imports)
    modules = {}
    seen = set()
    pending = []

    def add(module: Optional[types.ModuleType]) -> None:
        if module is None or module.__name__ in seen:
            return
        seen.add(module.__name__)
        if module.__name__ == "__main__":
            # cloudpickle already pickles __main__ by value, but the local modules it imports
            # are pickled by value only if they are registered
            pending.append(module)
        elif _is_local_module(module, installed_paths):
            modules[module.__name__] = module
            pending.append(module)

    visited = set()

    def visit(value: Any, depth: int, is_root: bool = False) -> None:
        # a local module can also be used only by a value that the reader holds
        if (
            depth > _MAX_STATE_DEPTH
            or id(value) in visited
            or len(visited) > _MAX_STATE_VALUES
        ):
            return
        visited.add(id(value))
        if isinstance(value, types.ModuleType):
            add(value)
        elif isinstance(value, (types.FunctionType, type)):
            add(sys.modules.get(value.__module__))
        elif isinstance(value, (list, tuple, set, frozenset)):
            for item in list(value):
                visit(item, depth + 1)
        elif isinstance(value, dict):
            for key, item in list(value.items()):
                visit(key, depth + 1)
                visit(item, depth + 1)
        else:
            module_name = type(value).__module__
            add(sys.modules.get(module_name))
            # don't walk into the state of objects from installed packages
            if not (is_root or module_name in modules or module_name == "__main__"):
                return
            state = getattr(value, "__dict__", None)
            if isinstance(state, dict):
                visit(state, depth + 1)
            for cls in type(value).__mro__:
                for name in getattr(cls, "__slots__", ()):
                    if isinstance(name, str) and hasattr(value, name):
                        visit(getattr(value, name), depth + 1)

    for obj in objects:
        for cls in type(obj).__mro__:
            add(sys.modules.get(cls.__module__))
        visit(obj, 0, is_root=True)
    # the functions and classes of a module are pickled by reference to the modules defining them
    while pending:
        for value in list(vars(pending.pop()).values()):
            if isinstance(value, types.ModuleType):
                add(value)
            elif isinstance(value, (types.FunctionType, type)):
                add(sys.modules.get(value.__module__))
    return list(modules.values())


@contextmanager
def _pickle_by_value(extra_modules: Sequence[types.ModuleType] = ()):
    if not all(
        hasattr(cloudpickle, name)
        for name in (
            "list_registry_pickle_by_value",
            "register_pickle_by_value",
            "unregister_pickle_by_value",
        )
    ):
        raise ImportError(
            "Server-side reads of custom data sources require cloudpickle>=2.0, "
            f"but cloudpickle {cloudpickle.__version__} is installed."
        )
    with _pickle_by_value_lock:
        # don't unregister the modules the user registered themselves
        registered = cloudpickle.list_registry_pickle_by_value()
        modules = [
            m
            for m in list(_PICKLE_BY_VALUE_MODULES) + list(extra_modules)
            if m.__name__ not in registered
        ]
        for module in modules:
            cloudpickle.register_pickle_by_value(module)
        try:
            yield
        finally:
            for module in modules:
                cloudpickle.unregister_pickle_by_value(module)


def _dumps(obj: Any, description: str) -> bytes:
    try:
        return cloudpickle.dumps(obj, protocol=pickle.HIGHEST_PROTOCOL)
    except Exception as exc:
        raise TypeError(
            f"The {description} of a custom data source must be picklable, but pickling "
            f"{obj!r} failed: {exc!r}. Avoid holding connections, clients, locks or open files "
            "as attributes or in module-level variables; create them inside "
            "DataSourceReader.read() instead."
        ) from exc


class _UserReadError(Exception):
    """Wraps an error raised by the user's ``DataSourceReader.read()``, to tell it apart from
    errors raised by Snowpark when it validates and converts the data that ``read()`` yielded.
    """


def _call_user_read(reader: Any, partition: Any) -> Iterator[Any]:
    try:
        yield from reader.read(partition)
    except Exception as exc:
        raise _UserReadError() from exc


class CustomPartitionReader:
    """The reader used by ``local_ingestion`` workers. It reads a partition with the user's
    ``DataSourceReader`` and converts the data to Arrow tables with the staging column types."""

    def __init__(
        self, pickled_reader: bytes, schema: StructType, batch_size: int
    ) -> None:
        self.pickled_reader = pickled_reader
        self.schema = schema
        self.batch_size = batch_size
        self._reader = None

    def __getstate__(self) -> Dict[str, Any]:
        state = self.__dict__.copy()
        state["_reader"] = None
        return state

    def read(self, partition: Tuple[int, bytes]) -> Iterator["pyarrow.Table"]:
        partition_idx, pickled_partition = partition
        if self._reader is None:
            # each worker unpickles its own copy of the reader, so readers are not shared across threads
            self._reader = cloudpickle.loads(self.pickled_reader)
        matcher = FieldMatcher(self.schema)
        input_partition = cloudpickle.loads(pickled_partition)
        produced_output = False
        rows = []
        try:
            for item in _call_user_read(self._reader, input_partition):
                if is_arrow_data(item):
                    if rows:
                        produced_output = True
                        yield rows_to_arrow_table(rows, matcher, partition_idx)
                        rows = []
                    produced_output = True
                    yield arrow_to_staged_table(item, matcher, partition_idx)
                else:
                    rows.append(to_row_tuple(item, matcher))
                    if len(rows) >= self.batch_size:
                        produced_output = True
                        yield rows_to_arrow_table(rows, matcher, partition_idx)
                        rows = []
            if rows:
                produced_output = True
                yield rows_to_arrow_table(rows, matcher, partition_idx)
        except _UserReadError as user_error:
            exc = user_error.__cause__
            if produced_output:
                # part of this partition may already be loaded, so a retry could duplicate rows
                raise SnowparkDataframeReaderException(
                    f"Failed to read partition {partition_idx} of the custom data source after part of "
                    f"it was loaded: {exc!r}"
                ) from exc
            raise exc
        except SnowparkDataframeReaderException:
            raise
        except Exception as exc:
            # the data yielded by read() is invalid, retrying won't help
            raise SnowparkDataframeReaderException(
                f"Failed to read partition {partition_idx} of the custom data source: {exc!r}"
            ) from exc

    def data_to_parquet_bytesio(self, table: "pyarrow.Table") -> Optional[BytesIO]:
        return arrow_table_to_parquet_bytesio(table)


def read_custom_data_source(
    session: "snowflake.snowpark.Session",
    data_source_cls: Type[DataSourceLike],
    options: Dict[str, Any],
    user_schema: Optional[Union[StructType, str]],
    _emit_ast: bool = True,
) -> "snowflake.snowpark.DataFrame":
    from snowflake.snowpark._internal.data_source.datasource_partitioner import (
        DataSourcePartitioner,
    )
    from snowflake.snowpark.mock._connection import MockServerConnection

    start_time = time.perf_counter()
    user_options, engine_options = split_options(options)
    is_pyspark_source = is_pyspark_class(data_source_cls)
    data_source = data_source_cls(user_options)

    schema = user_schema if user_schema is not None else data_source.schema()
    # the schema passed to reader(), a pyspark.sql.types.StructType for PySpark data sources
    reader_schema = None
    if is_pyspark_struct_type(schema):
        if is_pyspark_source:
            reader_schema = schema
        schema = pyspark_struct_type_to_snowpark(schema)
    elif isinstance(schema, str):
        schema = DataSourcePartitioner.formatting_custom_schema(schema)
    elif not isinstance(schema, StructType):
        raise TypeError(
            f"{data_source_cls.__name__}.schema() must return a StructType or a DDL string, got {schema!r}."
        )
    if is_pyspark_source:
        schema = with_spark_timestamp_semantics(schema)
    validate_schema(schema)
    if reader_schema is None:
        reader_schema = to_pyspark_struct_type(schema) if is_pyspark_source else schema

    try:
        reader = data_source.reader(reader_schema)
    except NotImplementedError as exc:
        if not is_pyspark_source:
            raise
        raise NotImplementedError(
            f"{data_source_cls.__name__}.reader() is not implemented. Snowpark only supports "
            "batch reads of PySpark data sources, not streaming reads."
        ) from exc
    if not callable(getattr(reader, "read", None)):
        raise TypeError(
            f"{data_source_cls.__name__}.reader() must return a DataSourceReader, got {reader!r}."
        )
    partitions = _get_partitions(reader)

    telemetry_json_string = defaultdict()
    telemetry_json_string["function_name"] = DATA_SOURCE_CUSTOM_SIGNATURE
    telemetry_json_string["ingestion_mode"] = (
        "udtf_ingestion" if engine_options.use_udtf_ingestion else "local_ingestion"
    )
    telemetry_json_string["num_partitions"] = len(partitions)
    statement_params = {STATEMENT_PARAMS_DATA_SOURCE_CUSTOM: "1"}

    if isinstance(session._conn, MockServerConnection):
        if engine_options.use_udtf_ingestion:
            raise NotImplementedError(
                "Server-side ingestion of custom data sources is not supported in local testing mode."
            )
        return _mock_ingestion(session, reader, partitions, schema, _emit_ast)

    if not partitions:
        df = session.create_dataframe([], schema=schema, _emit_ast=_emit_ast)
    elif engine_options.use_udtf_ingestion:
        df = _udtf_ingestion(
            session,
            reader,
            partitions,
            schema,
            engine_options,
            statement_params,
            requires_pyspark=is_pyspark_source,
            _emit_ast=_emit_ast,
        )
    else:
        df = _local_ingestion(
            session,
            reader,
            partitions,
            schema,
            engine_options,
            statement_params,
            telemetry_json_string,
            _emit_ast,
        )

    telemetry_json_string["end_to_end_duration"] = time.perf_counter() - start_time
    session._conn._telemetry_client.send_data_source_perf_telemetry(
        telemetry_json_string
    )
    set_api_call_source(df, DATA_SOURCE_CUSTOM_SIGNATURE)
    return df


def _get_partitions(reader: Any) -> List[Any]:
    partitions = getattr(reader, "partitions", None)
    if partitions is None:
        return [None]
    try:
        return list(partitions())
    except NotImplementedError:
        # older PySpark versions don't implement a default partitions()
        return [None]


def _local_ingestion(
    session: "snowflake.snowpark.Session",
    reader: Any,
    partitions: List[Any],
    schema: StructType,
    engine_options: EngineOptions,
    statement_params: Dict[str, str],
    telemetry_json_string: Dict[str, Any],
    _emit_ast: bool,
) -> "snowflake.snowpark.DataFrame":
    from snowflake.snowpark._internal.options import installed_pyarrow

    if not installed_pyarrow:
        raise ImportError(
            "pyarrow is required to read custom data sources. Install it with "
            "'pip install \"snowflake-snowpark-python[pandas]\"'."
        )

    pickled_reader = _dumps(reader, "DataSourceReader")
    pickled_partitions = [
        (idx, _dumps(partition, "InputPartition"))
        for idx, partition in enumerate(partitions)
    ]

    snowflake_table_name = random_name_for_temp_object(TempObjectType.TABLE)
    snowflake_stage_name = random_name_for_temp_object(TempObjectType.STAGE)
    create_data_source_table_and_stage(
        session=session,
        schema=staging_schema(schema),
        snowflake_table_name=snowflake_table_name,
        snowflake_stage_name=snowflake_stage_name,
        statements_params_for_telemetry=statement_params,
    )
    local_ingestion(
        session=session,
        create_reader=functools.partial(
            CustomPartitionReader,
            pickled_reader,
            schema,
            engine_options.batch_size,
        ),
        partitions=pickled_partitions,
        max_workers=min(engine_options.max_workers, len(partitions)),
        snowflake_stage_name=snowflake_stage_name,
        snowflake_table_name=snowflake_table_name,
        statements_params_for_telemetry=statement_params,
        telemetry_json_string=telemetry_json_string,
        fetch_with_process=engine_options.fetch_with_process,
        _emit_ast=_emit_ast,
    )
    df = session.table(snowflake_table_name, _emit_ast=_emit_ast)
    return parse_semi_structured_columns(df, schema, _emit_ast)


def _build_udtf_handler(pickled_reader: bytes, schema: StructType) -> type:
    # The handler only references cloudpickle and the modules in _PICKLE_BY_VALUE_MODULES,
    # so that it can be unpickled with any Snowpark version in Snowflake.
    matcher = FieldMatcher(schema)
    converters = udtf_row_converters(schema)

    class CustomDataSourceIngestion:
        def __init__(self) -> None:
            try:
                self._reader = cloudpickle.loads(pickled_reader)
            except ModuleNotFoundError as exc:
                # installed packages are pickled by reference, so they must be available in the UDTF
                raise ModuleNotFoundError(
                    f"{exc}. The custom data source reader uses the package '{exc.name}', which is "
                    "not available in the UDTF. If it is an installed package, add it to the "
                    "'snowpark.packages' option (with 'snowpark.artifact_repository' for packages "
                    "from PyPI). If it is a local module, upload it with the 'snowpark.imports' "
                    "option.",
                    name=exc.name,
                ) from exc

        def process(self, partition_idx: int, partition: bytes):
            try:
                for row in normalize_items(
                    self._reader.read(cloudpickle.loads(partition)), matcher
                ):
                    check_not_null(row, matcher)
                    yield convert_row(row, converters)
            except Exception as exc:
                raise RuntimeError(
                    f"Failed to read partition {partition_idx} of the custom data source: {exc!r}"
                ) from exc

    return CustomDataSourceIngestion


def _resolve_udtf_packages(
    packages: Optional[List[str]], requires_pyspark: bool = False
) -> List[str]:
    packages = list(packages or [])
    names = {
        re.split(r"[\s<>=!~\[;]", p.strip(), maxsplit=1)[0].lower() for p in packages
    }
    required = [_SNOWPARK_PACKAGE] + ([PYSPARK_PACKAGE] if requires_pyspark else [])
    packages.extend(p for p in required if p not in names)
    return packages


def _dumps_reader_and_partitions(
    reader: Any, partitions: List[Any]
) -> Tuple[bytes, List[List[Any]]]:
    return (
        _dumps(reader, "DataSourceReader"),
        [
            [idx, _dumps(partition, "InputPartition")]
            for idx, partition in enumerate(partitions)
        ],
    )


def _udtf_ingestion(
    session: "snowflake.snowpark.Session",
    reader: Any,
    partitions: List[Any],
    schema: StructType,
    engine_options: EngineOptions,
    statement_params: Dict[str, str],
    requires_pyspark: bool = False,
    _emit_ast: bool = False,
) -> "snowflake.snowpark.DataFrame":
    # readers and partitions of PySpark data sources are unpickled with PySpark in the UDTF
    requires_pyspark = requires_pyspark or uses_pyspark([reader] + list(partitions))
    local_modules = _local_modules_of(
        [reader] + list(partitions), engine_options.imports
    )
    try:
        with _pickle_by_value(local_modules):
            pickled_reader, partition_rows = _dumps_reader_and_partitions(
                reader, partitions
            )
    except TypeError:
        if not local_modules:
            raise
        # a module-level variable of a local module may not be picklable by value, for
        # example a lock, so pickle the modules by reference, which requires uploading them
        # with 'snowpark.imports'
        logger.warning(
            "Pickling the local modules %s by value failed, so they are pickled by reference. "
            "Upload them with the 'snowpark.imports' option, so that they are available in the UDTF.",
            [m.__name__ for m in local_modules],
        )
        local_modules = []
        with _pickle_by_value(local_modules):
            pickled_reader, partition_rows = _dumps_reader_and_partitions(
                reader, partitions
            )

    partitions_table = random_name_for_temp_object(TempObjectType.TABLE)
    session.create_dataframe(
        partition_rows,
        schema=StructType(
            [
                StructField(_PARTITION_IDX_COLUMN, LongType(), False),
                StructField(_PARTITION_COLUMN, BinaryType(), False),
            ]
        ),
        _emit_ast=False,
    ).write.save_as_table(
        partitions_table,
        table_type="temp",
        statement_params=statement_params,
        _emit_ast=False,
    )

    udtf_name = random_name_for_temp_object(TempObjectType.TABLE_FUNCTION)
    with _pickle_by_value(local_modules):
        session.udtf.register(
            _build_udtf_handler(pickled_reader, schema),
            output_schema=udtf_output_schema(schema),
            input_types=[LongType(), BinaryType()],
            name=udtf_name,
            is_permanent=False,
            packages=_resolve_udtf_packages(engine_options.packages, requires_pyspark),
            imports=engine_options.imports,
            artifact_repository=engine_options.artifact_repository,
            external_access_integrations=[engine_options.external_access_integration],
            secrets=engine_options.secrets,
            statement_params=statement_params,
            _emit_ast=False,
        )

    columns = ", ".join(field.name for field in schema.fields)
    query = (
        f"SELECT {columns} FROM {partitions_table}, "
        f"TABLE({udtf_name}({_PARTITION_IDX_COLUMN}, {_PARTITION_COLUMN}) "
        f"OVER (PARTITION BY {_PARTITION_IDX_COLUMN})) {DATA_SOURCE_CUSTOM_SQL_COMMENT}"
    )
    df = cast_udtf_output_columns(
        session.sql(query, _emit_ast=_emit_ast), schema, _emit_ast
    )
    return BaseDriver.keep_nullable_attributes(df, schema)


def _mock_ingestion(
    session: "snowflake.snowpark.Session",
    reader: Any,
    partitions: List[Any],
    schema: StructType,
    _emit_ast: bool,
) -> "snowflake.snowpark.DataFrame":
    # live reads pickle the reader and partitions, so unpicklable ones fail here too
    _dumps(reader, "DataSourceReader")
    for partition in partitions:
        _dumps(partition, "InputPartition")
    matcher = FieldMatcher(schema)
    converters = value_converters(schema)
    rows = []
    for idx, partition in enumerate(partitions):
        try:
            for row in normalize_items(reader.read(partition), matcher):
                check_not_null(row, matcher)
                rows.append(convert_row(row, converters))
        except Exception as exc:
            raise SnowparkDataframeReaderException(
                f"Failed to read partition {idx} of the custom data source: {exc!r}"
            ) from exc
    return session.create_dataframe(rows, schema=schema, _emit_ast=_emit_ast)
