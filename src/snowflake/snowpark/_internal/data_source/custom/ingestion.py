#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
import functools
import pickle
import time
from collections import defaultdict
from io import BytesIO
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Iterator,
    List,
    Optional,
    Tuple,
    Type,
    Union,
)

import cloudpickle

from snowflake.snowpark._internal.data_source.custom.conversion import (
    FieldMatcher,
    arrow_table_to_parquet_bytesio,
    arrow_to_staged_table,
    check_not_null,
    convert_row,
    is_arrow_data,
    normalize_items,
    parse_semi_structured_columns,
    rows_to_arrow_table,
    staging_schema,
    to_row_tuple,
    validate_schema,
    value_converters,
)
from snowflake.snowpark._internal.data_source.custom.options import (
    EngineOptions,
    split_options,
)
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
from snowflake.snowpark.types import StructType

if TYPE_CHECKING:
    import pyarrow

    import snowflake.snowpark

DATA_SOURCE_CUSTOM_SIGNATURE = "DataFrameReader.custom_data_source"
STATEMENT_PARAMS_DATA_SOURCE_CUSTOM = "SNOWPARK_PYTHON_DATASOURCE_CUSTOM"


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
    data_source = data_source_cls(user_options)

    schema = user_schema if user_schema is not None else data_source.schema()
    if isinstance(schema, str):
        schema = DataSourcePartitioner.formatting_custom_schema(schema)
    elif not isinstance(schema, StructType):
        raise TypeError(
            f"{data_source_cls.__name__}.schema() must return a StructType or a DDL string, got {schema!r}."
        )
    validate_schema(schema)

    reader = data_source.reader(schema)
    if not callable(getattr(reader, "read", None)):
        raise TypeError(
            f"{data_source_cls.__name__}.reader() must return a DataSourceReader, got {reader!r}."
        )
    partitions = _get_partitions(reader)

    telemetry_json_string = defaultdict()
    telemetry_json_string["function_name"] = DATA_SOURCE_CUSTOM_SIGNATURE
    telemetry_json_string["ingestion_mode"] = "local_ingestion"
    telemetry_json_string["num_partitions"] = len(partitions)
    statement_params = {STATEMENT_PARAMS_DATA_SOURCE_CUSTOM: "1"}

    if isinstance(session._conn, MockServerConnection):
        return _mock_ingestion(session, reader, partitions, schema, _emit_ast)

    if not partitions:
        df = session.create_dataframe([], schema=schema, _emit_ast=_emit_ast)
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
        # a reader may not implement partitions()
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
