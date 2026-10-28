#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
import functools
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
from snowflake.snowpark._internal.data_source.custom.conversion import (
    FieldMatcher,
    arrow_table_to_parquet_bytesio,
    arrow_to_staged_table,
    cast_udtf_output_columns,
    is_arrow_data,
    normalize_items,
    parse_semi_structured_columns,
    rows_to_arrow_table,
    semi_structured_indices,
    staging_schema,
    timestamp_indices,
    to_row_tuple,
    to_udtf_row,
    udtf_output_schema,
    validate_schema,
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

DATA_SOURCE_CUSTOM_SIGNATURE = "DataFrameReader.custom_data_source"
STATEMENT_PARAMS_DATA_SOURCE_CUSTOM = "SNOWPARK_PYTHON_DATASOURCE_CUSTOM"
DATA_SOURCE_CUSTOM_SQL_COMMENT = (
    f"/* Python:snowflake.snowpark.{DATA_SOURCE_CUSTOM_SIGNATURE} */"
)
_PARTITION_IDX_COLUMN = "SNOWPARK_DATA_SOURCE_PARTITION_IDX"
_PARTITION_COLUMN = "SNOWPARK_DATA_SOURCE_PARTITION"
_SNOWPARK_PACKAGE = "snowflake-snowpark-python"

# Modules pickled by value in server-side ingestion, so that the UDTF does not depend on
# the Snowpark version available in Snowflake having them.
_PICKLE_BY_VALUE_MODULES = (datasource_module, conversion)
_pickle_by_value_lock = threading.Lock()


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


def _local_modules_of(objects: List[Any]) -> List[types.ModuleType]:
    """Returns the local (not installed) modules that define the classes of ``objects``. They
    are pickled by value, so that the UDTF can unpickle the reader and partitions without the
    user adding these modules to ``snowpark.imports``."""
    installed_paths = _installed_package_paths()
    modules = {}
    for obj in objects:
        for cls in type(obj).__mro__:
            module = sys.modules.get(cls.__module__)
            if (
                module is None
                or module.__name__ in modules
                or module.__name__ in ("__main__", "builtins")
                or module.__name__.split(".")[0] == "snowflake"
            ):
                continue
            path = getattr(module, "__file__", None)
            if path is None:
                continue
            path = os.path.realpath(path)
            if any(path.startswith(p + os.sep) for p in installed_paths):
                continue
            modules[module.__name__] = module
    return list(modules.values())


@contextmanager
def _pickle_by_value(extra_modules: Sequence[types.ModuleType] = ()):
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
            "as attributes; create them inside DataSourceReader.read() instead."
        ) from exc


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
        produced_output = False
        rows = []
        try:
            for item in self._reader.read(cloudpickle.loads(pickled_partition)):
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
        except SnowparkDataframeReaderException:
            raise
        except (TypeError, ValueError) as exc:
            # the data does not match the schema, retrying won't help
            raise SnowparkDataframeReaderException(
                f"Failed to read partition {partition_idx} of the custom data source: {exc!r}"
            ) from exc
        except Exception as exc:
            if produced_output:
                # part of this partition may already be loaded, so a retry could duplicate rows
                raise SnowparkDataframeReaderException(
                    f"Failed to read partition {partition_idx} of the custom data source after part of "
                    f"it was loaded: {exc!r}"
                ) from exc
            raise

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
    return parse_semi_structured_columns(df, schema)


def _build_udtf_handler(pickled_reader: bytes, schema: StructType) -> type:
    # The handler only references cloudpickle and the modules in _PICKLE_BY_VALUE_MODULES,
    # so that it can be unpickled with any Snowpark version in Snowflake.
    matcher = FieldMatcher(schema)
    ts_indices = timestamp_indices(schema)
    json_indices = semi_structured_indices(schema)

    class CustomDataSourceIngestion:
        def __init__(self) -> None:
            self._reader = cloudpickle.loads(pickled_reader)

        def process(self, partition_idx: int, partition: bytes):
            try:
                for row in normalize_items(
                    self._reader.read(cloudpickle.loads(partition)), matcher
                ):
                    yield to_udtf_row(row, ts_indices, json_indices)
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


def _udtf_ingestion(
    session: "snowflake.snowpark.Session",
    reader: Any,
    partitions: List[Any],
    schema: StructType,
    engine_options: EngineOptions,
    statement_params: Dict[str, str],
    requires_pyspark: bool = False,
) -> "snowflake.snowpark.DataFrame":
    # readers and partitions of PySpark data sources are unpickled with PySpark in the UDTF
    requires_pyspark = requires_pyspark or uses_pyspark([reader] + list(partitions))
    local_modules = _local_modules_of([reader] + list(partitions))
    with _pickle_by_value(local_modules):
        pickled_reader = _dumps(reader, "DataSourceReader")
        partition_rows = [
            [idx, _dumps(partition, "InputPartition")]
            for idx, partition in enumerate(partitions)
        ]

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
    df = cast_udtf_output_columns(session.sql(query, _emit_ast=False), schema)
    return BaseDriver.keep_nullable_attributes(df, schema)


def _mock_ingestion(
    session: "snowflake.snowpark.Session",
    reader: Any,
    partitions: List[Any],
    schema: StructType,
    _emit_ast: bool,
) -> "snowflake.snowpark.DataFrame":
    matcher = FieldMatcher(schema)
    rows = []
    for idx, partition in enumerate(partitions):
        try:
            rows.extend(normalize_items(reader.read(partition), matcher))
        except Exception as exc:
            raise SnowparkDataframeReaderException(
                f"Failed to read partition {idx} of the custom data source: {exc!r}"
            ) from exc
    return session.create_dataframe(rows, schema=schema, _emit_ast=_emit_ast)
