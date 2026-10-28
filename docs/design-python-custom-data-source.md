# Design: Python Custom Data Source API (Spark 4–compatible)

Status: **Draft**. Author: Yijun Xie. Last updated: 2026-10-28.

This document proposes a Python custom data source API for Snowpark Python, modeled on
[PySpark 4's Python Data Source API](https://spark.apache.org/docs/latest/api/python/tutorial/sql/python_data_source.html)
(`pyspark.sql.datasource`). Users would write a `DataSource` subclass that reads from (and later writes to) any
external system, register it with a session, and use it through `session.read.format(...)` and
`df.write.format(...)`.

---

## Table of Contents

1. [Motivation](#1-motivation)
2. [Goals and Non-Goals](#2-goals-and-non-goals)
3. [User-Facing API](#3-user-facing-api)
4. [Execution Model: Reads](#4-execution-model-reads)
5. [Execution Model: Writes](#5-execution-model-writes)
6. [Schema and Type Conversion](#6-schema-and-type-conversion)
7. [Options and Engine Configuration](#7-options-and-engine-configuration)
8. [Reusing the Existing `dbapi` Ingestion Machinery](#8-reusing-the-existing-dbapi-ingestion-machinery)
9. [Module Layout](#9-module-layout)
10. [Error Handling, Retries, Security](#10-error-handling-retries-security)
11. [AST, Telemetry, Local Testing](#11-ast-telemetry-local-testing)
12. [PySpark Compatibility](#12-pyspark-compatibility)
13. [Alternatives Considered](#13-alternatives-considered)
14. [Phased Delivery](#14-phased-delivery)
15. [Open Questions](#15-open-questions)

---

## 1. Motivation

Snowpark can already ingest data from external databases with `session.read.dbapi()` (GA) and
`session.read.jdbc()` (PrPr). Both only work with a SQL database that has a DBAPI/JDBC driver, and each
DBMS needs its own dialect and driver class in
[`_internal/data_source/`](../src/snowflake/snowpark/_internal/data_source/).

Users also want to pull data from sources that aren't SQL databases: REST APIs (Salesforce, HubSpot,
GitHub), SaaS exports, message queues, proprietary file formats, vector stores, internal services.
Today they write their own loops that fetch data, build a DataFrame (or Parquet files), and `PUT`/`COPY` it,
or they write a UDTF by hand. They have to handle parallelism, batching, typing, and staging themselves.

Spark 4 solved this with a small, well-known interface: the user provides `schema()`, `partitions()` and
`read(partition)`, and the engine handles everything else. Matching that interface:

- gives users one way to plug in any source, with parallelism and Snowflake loading handled by Snowpark;
- lets existing PySpark data sources move over with few or no changes (see [the spark-migration
  story](#12-pyspark-compatibility)), including for **Snowpark Connect**, where Spark Connect clients
  already call `spark.dataSource.register(...)`;
- lets us eventually rebuild `dbapi` as one built-in data source on top of the same engine (§8).

## 2. Goals and Non-Goals

### Goals

- G1. A public `snowflake.snowpark.datasource` module whose classes and methods use the same names and
  meanings as `pyspark.sql.datasource` (batch read and batch write).
- G2. Registration with `session.data_source.register(cls)`, and usage with
  `session.read.format(name).option(...).load()` and `df.write.format(name).mode(...).save()`.
- G3. Two read execution modes, the same ones `dbapi` already has:
  - **local**: `read()` runs in client threads or processes; data is staged as Parquet and copied into a
    temp table.
  - **server-side (UDTF)**: `read()` runs inside Snowflake in a UDTF, which needs an external access
    integration.
- G4. `read()` may yield tuples, lists, `Row`s, or `pyarrow.RecordBatch`es (Spark 4.0 supports the last).
- G5. Reuse the existing parallel fetch → Parquet → PUT → COPY pipeline instead of building a second one.

### Non-Goals (v1)

- Streaming (`DataSourceStreamReader`, `simpleStreamReader`, `streamWriter`). Snowpark has no streaming
  execution engine. Revisit together with Dynamic Tables or Snowpipe Streaming.
- Filter pushdown and column pruning (`DataSourceReader.pushFilters`, Spark 4.1). This needs lazy
  planning; see §4.4. It's planned for Phase 3.
- Catalog integration (`CREATE TABLE ... USING my_source`).
- Running `pyspark.sql.datasource` subclasses *unchanged* when they use PySpark-only types inside
  `read()`. Phase 2 adds a best-effort adapter.

## 3. User-Facing API

### 3.1 Public classes (`snowflake.snowpark.datasource`)

```python
class DataSource(ABC):
    def __init__(self, options: Dict[str, str]) -> None:
        self.options = options              # case-insensitive dict

    @classmethod
    def name(cls) -> str:                   # format name; defaults to cls.__name__
        return cls.__name__

    def schema(self) -> Union[StructType, str]:  # StructType or DDL string
        raise NotImplementedError

    def reader(self, schema: StructType) -> "DataSourceReader":
        raise NotImplementedError

    def writer(self, schema: StructType, overwrite: bool) -> "DataSourceWriter":  # Phase 2
        raise NotImplementedError


class InputPartition:
    """Any picklable value. Subclass to carry partition metadata."""
    def __init__(self, value: Any) -> None:
        self.value = value


class DataSourceReader(ABC):
    def partitions(self) -> Sequence[InputPartition]:
        return [InputPartition(None)]      # single partition by default

    @abstractmethod
    def read(self, partition: InputPartition) -> Iterator[Union[tuple, Row, "pyarrow.RecordBatch"]]:
        ...


class WriterCommitMessage:                 # Phase 2
    ...


class DataSourceWriter(ABC):               # Phase 2
    @abstractmethod
    def write(self, iterator: Iterator[Row]) -> WriterCommitMessage: ...
    def commit(self, messages: List[Optional[WriterCommitMessage]]) -> None: ...
    def abort(self, messages: List[Optional[WriterCommitMessage]]) -> None: ...
```

Like PySpark, `DataSource`, `DataSourceReader`, `InputPartition`, and `DataSourceWriter` objects must be
picklable, because they are sent to worker processes or into a UDTF.

### 3.2 Registration (`Session.data_source`)

```python
session.data_source.register(GitHubIssuesDataSource)
session.data_source.registered_names()     # -> ["githubissues", ...]
session.data_source.unregister("githubissues")
```

- The registry belongs to the session (`Session.data_source -> DataSourceRegistration`), the same way
  `session.udf` and `session.udtf` work.
- Names are matched case-insensitively (stored lower-cased). Registering a name that is already in use
  replaces the old entry with a warning, as in Spark.
- Registering a **reserved** name (`csv`, `json`, `avro`, `parquet`, `orc`, `xml`, `dbapi`, `jdbc`,
  `documents`, …) raises `ValueError`, so built-in formats can't be silently overridden.
- As a convenience, `session.read.format(MyDataSource)` also accepts the class itself without
  registering it.

### 3.3 Reading

```python
from snowflake.snowpark.datasource import DataSource, DataSourceReader, InputPartition
from snowflake.snowpark.types import StructType

class GitHubIssuesDataSource(DataSource):
    @classmethod
    def name(cls):
        return "github_issues"

    def schema(self):
        return "number INT, title STRING, state STRING, created_at TIMESTAMP"

    def reader(self, schema: StructType):
        return GitHubIssuesReader(self.options, schema)


class GitHubIssuesReader(DataSourceReader):
    def __init__(self, options, schema):
        self.repo = options["repo"]
        self.pages = int(options.get("pages", 10))

    def partitions(self):
        return [InputPartition(p) for p in range(1, self.pages + 1)]

    def read(self, partition):
        import requests
        resp = requests.get(
            f"https://api.github.com/repos/{self.repo}/issues",
            params={"page": partition.value, "per_page": 100, "state": "all"},
        )
        for i in resp.json():
            yield (i["number"], i["title"], i["state"], i["created_at"])


session.data_source.register(GitHubIssuesDataSource)

# local mode (default): read() runs in client threads
df = session.read.format("github_issues").option("repo", "snowflakedb/snowpark-python").load()

# server-side mode: read() runs in a UDTF inside Snowflake
df = (
    session.read.format("github_issues")
    .option("repo", "snowflakedb/snowpark-python")
    .option("snowpark.external_access_integration", "GITHUB_EAI")
    .option("snowpark.packages", ["requests"])
    .load()
)
```

`session.read.schema(...)` overrides `DataSource.schema()`, the same way Spark's `.schema()` does.

### 3.4 Writing (Phase 2)

```python
df.write.format("github_issues").mode("append").option("repo", "...").save()
```

`mode("append")` → `writer(schema, overwrite=False)`, and `mode("overwrite")` → `overwrite=True`. Other modes
(`errorifexists`, `ignore`) raise `NotImplementedError`, the same as in PySpark.

## 4. Execution Model: Reads

### 4.1 Common planning steps (always on the client, eagerly at `load()`)

```
load()
  ├─ resolve format → DataSource class (registry, or class passed directly)
  ├─ ds = cls(user_options)                      # user options only, without "snowpark.*" keys
  ├─ schema = reader_schema or ds.schema()       # parse DDL → StructType (§6)
  ├─ reader = ds.reader(schema)
  ├─ partitions = list(reader.partitions())      # must be picklable
  └─ dispatch to local or UDTF mode
```

`partitions()` always runs on the client, as in Spark, where it runs on the driver. This lets it use local
credentials or files to plan.

### 4.2 Local mode (default)

This reuses `local_ingestion()` in
[`_internal/data_source/utils.py`](../src/snowflake/snowpark/_internal/data_source/utils.py) unchanged in
structure:

1. `create_data_source_table_and_stage(schema)` creates the temp table and temp stage.
2. Partitions go onto `partition_queue`. Each of `max_workers` thread (or process) workers pulls one
   partition and calls `reader.read(partition)`.
3. Output is grouped into batches of `snowpark.batch_size` rows (default 100,000). A `pyarrow.RecordBatch`
   counts as one batch. Each batch becomes Parquet in memory (§6.3) and goes onto `parquet_queue`.
4. `process_parquet_queue_with_threads` does `PUT` + `COPY INTO ... MATCH_BY_COLUMN_NAME`.
5. The result is `session.table(temp_table)`, with column order and nullability taken from the schema.

The process pool (`snowpark.fetch_with_process=true`) cloudpickles the reader into child processes. Like
`dbapi`, the thread pool is the default because most custom sources are I/O-bound (HTTP).

This mode is **eager**: data is copied in when `load()` is called, the same as `dbapi` local ingestion.

### 4.3 Server-side (UDTF) mode

This mode is used when `snowpark.external_access_integration` is set.

```
client:
  temp table PARTITIONS(IDX INT, PARTITION BINARY)    ← one row per cloudpickle.dumps(partition)
  register temp UDTF  CustomSourceIngestion           ← closure captures cloudpickle.dumps(reader)
  df = SELECT out.* FROM PARTITIONS, TABLE(udtf(PARTITION) OVER (PARTITION BY IDX)) out

server (per partition):
  reader = cloudpickle.loads(pickled_reader)          # once per UDTF instance (__init__)
  for item in reader.read(cloudpickle.loads(partition)):
      yield row(s)
```

Design points:

- **Partition transport.** Partitions are arbitrary Python objects, so they're stored as cloudpickled
  `BINARY` rather than the SQL strings `dbapi` uses. `OVER (PARTITION BY IDX)` gives each partition its
  own UDTF partition, so Snowflake can spread them across warehouse nodes.
- **Output typing.** The UDTF's `output_schema` uses the real target types, not `VariantType` followed by
  casts as `BaseDriver.udtf_ingestion` does. We already know the user's schema exactly, and declaring the
  real types avoids a cast layer and the VARIANT 128 MB limit per value. If a value doesn't match its
  type, the UDTF fails and we add the partition index to the error (§10).
- **Arrow output.** When `read()` yields `RecordBatch`es, use a vectorized UDTF
  (`end_partition` returning `pandas.DataFrame`) so rows aren't converted one at a time. This is Phase 2;
  v1 turns each batch into rows inside the UDTF.
- **Packages and imports.** The user's module must be importable on the server. We pass
  `snowpark.packages`, `snowpark.imports`, and `snowpark.artifact_repository` through to
  `session.udtf.register` (the `dbapi` `udtf_configs` keys, just renamed). The UDTF handler is
  cloudpickled, so classes defined in `__main__` or a notebook work without `imports`.
- **Laziness.** Registering the UDTF and writing the partition table happen eagerly. The data is read
  lazily, when an action runs. As with `dbapi` UDTF mode, errors from `read()` surface as
  `SnowparkSQLException` during the action.

### 4.4 Filter pushdown and column pruning (Phase 3)

Spark 4.1 adds `DataSourceReader.pushFilters(filters) -> Iterable[Filter]`. Snowpark can't support this
in local mode, because the data is already copied in before the user calls `.filter()`.

Supporting it means making custom sources lazy:

- Add a new logical plan leaf, `PythonDataSourceRelation(ds, schema, options)`. `load()` returns a
  DataFrame over this leaf without reading anything.
- When an action runs, before the plan is turned into SQL, the analyzer walks the `Filter` /
  `Project` nodes directly above the leaf. It turns simple predicates (`=`, `<`, `>`, `IN`, `IS NULL`,
  `STARTS WITH`, `AND`) into `snowflake.snowpark.datasource` filter objects (`EqualTo`, `GreaterThan`,
  … with the same names as PySpark), and calls `pushFilters`. Filters the source rejects stay in
  Snowflake SQL. Filters the source accepts are removed from the SQL.
- Only then are `partitions()` and the ingestion strategy run. The leaf is replaced by the temp table
  (local mode) or the UDTF call (server mode).

This changes the query compiler a lot (the leaf has to be resolved when the plan is executed, not when
it's built). It's also similar to work Snowpark Connect needs. We're deliberately leaving it out of v1 so
it doesn't hold back the basic API.

## 5. Execution Model: Writes (Phase 2)

```
client: writer = ds.writer(df.schema, overwrite)

local mode:
  for batch in df.to_local_iterator() split into N chunks (thread pool):
      messages[i] = writer.write(iter(rows))
  on success: writer.commit(messages)
  on any failure: writer.abort(messages)          # messages from failed tasks are None

server mode:
  SELECT msg FROM (SELECT *, <bucket> AS B FROM df),
         TABLE(write_udtf(<cols>) OVER (PARTITION BY B))
  write_udtf.end_partition: writer.write(iter(buffered_rows)) → yield cloudpickle.dumps(message)
  client collects messages → writer.commit(messages) / writer.abort(messages)
```

- `commit()` and `abort()` always run on the client, matching Spark, where they run on the driver.
- In server mode the bucket count is `snowpark.num_partitions` (default: number of warehouse nodes × 8).
  The bucket is `ABS(HASH(*)) % n`, or `SEQ8() % n` when there are no columns to hash. Rows are buffered
  per UDTF partition, because a UDTF can't hand out a lazy iterator that spans `process` calls. Phase 2+
  could switch to a vectorized `end_partition` with pandas to reduce memory per row.
- **Idempotency.** Snowflake may retry UDTF partitions, so `write()` can run more than once for the same
  data. We'll document this, as Spark does for task retries. Writers should be idempotent or should stage
  data and make it visible only in `commit()`.

## 6. Schema and Type Conversion

### 6.1 Schema sources (priority order)

1. `session.read.schema(StructType | str)`
2. `DataSource.schema()`. If it isn't implemented and no schema was given → `ValueError`.

DDL strings are parsed with `type_string_to_type_object` (already used by
`DataSourcePartitioner.formatting_custom_schema`). Common Spark DDL type names (`STRING`, `INT`, `LONG`,
`BIGINT`, `DOUBLE`, `BOOLEAN`, `TIMESTAMP`, `DATE`, `BINARY`, `DECIMAL(p,s)`, `ARRAY<...>`, `MAP<...>`,
`STRUCT<...>`) need to parse. We'll check this and fill in any gaps in `type_string_to_type_object`
(`LONG` and `STRUCT<...>` probably need work).

### 6.2 Row → value mapping

| `read()` yields | Handling |
|---|---|
| `tuple` / `list` | Matched to schema fields by position. The length must equal the field count. |
| `Row` (Snowpark) | Matched by field name if the Row has names, otherwise by position. |
| `dict` | Not in PySpark. Accepted as an extension; matched by name. |
| `pyarrow.RecordBatch` | Columns matched by name and cast to the target Arrow schema. |
| `pyspark.sql.Row` | Duck-typed, treated as a `Row` (`asDict()`), which helps compatibility (§12). |

Values follow the same Python types `session.create_dataframe` accepts (`int`, `float`, `Decimal`, `str`,
`bytes`, `datetime`, `date`, `time`, `list`/`dict` for semi-structured, …).

### 6.3 Batch → Parquet

`dbapi` goes rows → `pandas.DataFrame` → Parquet. pandas changes nullable ints into floats and has
trouble with `Decimal`/`time` columns, so `dbapi` works around this per driver. For custom sources we
use **pyarrow directly** instead:

- Add `snowpark_type_to_arrow_type(DataType) -> pa.DataType` and `struct_to_arrow_schema(StructType)`.
  Nothing like these exists today. Semi-structured types (`ArrayType`, `MapType`, `StructType`,
  `VariantType`) map to JSON strings, which `COPY` with `MATCH_BY_COLUMN_NAME` then parses. Timestamps
  map to `pa.timestamp("us", tz=...)` according to `TimestampTimeZone`.
- Rows become `pa.Table.from_arrays(...)` using the target Arrow schema; batches are cast with `cast(...)`.
  A type mismatch raises an error that includes the column name and the partition index.
- `pq.write_table` writes to a `BytesIO` and puts the result onto the existing `parquet_queue`.

## 7. Options and Engine Configuration

- User options are passed to `DataSource.__init__` in a case-insensitive `Dict[str, str]`
  (`CaseInsensitiveDict`, the same semantics as Spark). Values are converted with `str()`, as Spark does.
  Code that expects lists gets JSON.
- Keys with the reserved prefix `snowpark.` configure the engine. They are **removed** before the
  options reach the data source:

| Option | Mode | Meaning | `dbapi` equivalent |
|---|---|---|---|
| `snowpark.external_access_integration` | read/write | Turns on server (UDTF) mode | `udtf_configs["external_access_integration"]` |
| `snowpark.packages`, `snowpark.imports`, `snowpark.artifact_repository` | server | Passed through to `udtf.register` | `udtf_configs[...]` |
| `snowpark.secrets` | server | `{alias: secret_name}` → `udtf.register(secrets=...)` | — |
| `snowpark.max_workers` | local | Thread or process pool size | `max_workers` |
| `snowpark.fetch_with_process` | local | Use processes instead of threads | `fetch_with_process` |
| `snowpark.batch_size` | local | Rows per Parquet file | `fetch_size` × `fetch_merge_count` |
| `snowpark.num_partitions` | write/server | Number of write buckets | — |

Because these options are removed before the source sees them, `session.read.option(...)` can carry
both kinds and nothing has to be added to `load()`'s signature.

## 8. Reusing the Existing `dbapi` Ingestion Machinery

`local_ingestion()` and `worker_process()` are already almost independent of the source. The worker
needs only an object with:

```python
read(partition) -> Iterator[batch]
data_source_data_to_pandas_df(batch) -> pd.DataFrame
```

`local_ingestion` uses `partitioner` only to call `partitioner.reader()`, and `partitioned_queries` only
as a list of picklable items. Proposed refactor (no behavior change for `dbapi`):

1. Define an internal protocol `_PartitionBatchReader` with
   `read(partition) -> Iterator[batch]` and `batch_to_parquet(batch) -> BytesIO`. Change
   `_task_fetch_data_from_source` to call `batch_to_parquet`. The `dbapi` `DataSourceReader` implements it
   with the existing pandas path, and the custom-source adapter (§9) uses the pyarrow path from §6.3.
2. Change `local_ingestion(partitioner=..., partitioned_queries=...)` to
   `local_ingestion(reader_factory: Callable[[], _PartitionBatchReader], partitions: List[Any], ...)`.
3. Rename the internal `datasource_reader.DataSourceReader` to `DbapiPartitionReader`, so it isn't
   confused with the new public `snowflake.snowpark.datasource.DataSourceReader`.

Longer term (outside this design), `dbapi` and `jdbc` could be rebuilt as built-in `DataSource`
implementations on this engine. They would keep their current public signatures as thin wrappers.

## 9. Module Layout

```
src/snowflake/snowpark/
  datasource.py                          # public: DataSource, DataSourceReader, InputPartition,
                                         #         DataSourceWriter, WriterCommitMessage, (Phase 3) filters
  session.py                             # + Session.data_source property
  dataframe_reader.py                    # format() accepts registered names / classes; load() dispatches
  dataframe_writer.py                    # format()/save() dispatch (Phase 2)
  _internal/data_source/
    custom/
      __init__.py
      registration.py                    # DataSourceRegistration (register/unregister/lookup, reserved names)
      options.py                         # CaseInsensitiveDict, split of snowpark.* engine options
      reader_adapter.py                  # _CustomPartitionReader(_PartitionBatchReader): rows/Arrow → Parquet
      udtf_reader.py                     # UDTF handler builder + partitions table
      writer.py                          # local/UDTF write paths, commit/abort coordination (Phase 2)
      arrow_types.py                     # snowpark_type_to_arrow_type, struct_to_arrow_schema
    utils.py                             # local_ingestion generalized (§8)
```

Changes to `DataFrameReader`:

- `_format.setter`: names found in the session registry (or `DataSource` subclasses) become valid
  formats and are added to `_data_source_format`, so `load()` rejects a `path` for them.
- `load()`: for a custom format, call `_load_custom_data_source(cls, self._cur_options, self._user_schema)`.

## 10. Error Handling, Retries, Security

- **Errors.** Exceptions from `schema()`, `reader()`, or `partitions()` propagate unchanged. They happen
  during planning, and users expect to see their own errors there. Exceptions from `read()` in local mode
  are wrapped in `SnowparkDataframeReaderException("... partition {idx}: {exc!r}")` with the original
  exception chained, matching `local_ingestion`. In server mode the UDTF re-raises with the partition
  index as a prefix, so the `SnowparkSQLException` says which partition failed.
- **Retries.** Local reads reuse `_retry_run` (3 attempts) by default. `snowpark.max_retries=0` turns
  retries off for sources where `read()` isn't safe to repeat. Writes are never retried on the client.
- **Credentials.** Options are pickled into the UDTF handler, which is stored on a temp stage. The docs will
  tell users not to put secrets in options for server mode, and to use `snowpark.secrets` +
  `_snowflake.get_generic_secret_string(alias)` inside `read()` instead. Local mode never sends options to
  Snowflake.
- **Pickling failures.** Check `cloudpickle.dumps(reader)` and `dumps(partitions)` up front, and give a
  clear error ("DataSourceReader must be picklable; avoid holding open connections in `__init__`; open
  them inside `read()`").

## 11. AST, Telemetry, Local Testing

- **AST.** Follow `dbapi`: build the internal DataFrames with `_emit_ast=False`. In local mode, emit the
  result as `session.read.table(temp_table)`, since running custom Python code on the server-side AST
  path is out of scope. Mark the DataFrame with `set_api_call_source(df, "DataFrameReader.custom_data_source")`.
- **Telemetry.** Add `send_data_source_perf_telemetry` entries with `ingestion_mode`, partition count,
  row-type kind (tuple/row/arrow), and timings. **Never send the source name or class name**, because they
  may reveal customer-internal systems. Send only `"custom"`.
- **Local testing (`MockServerConnection`).** Local mode can work by skipping PUT/COPY and calling
  `session.create_dataframe(rows, schema)` → `save_as_table(temp)`. Server mode raises
  `NotImplementedError` in local testing, the same as UDTFs with external access.

## 12. PySpark Compatibility

We aim for "rename the import" compatibility:

```python
# from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition
from snowflake.snowpark.datasource import DataSource, DataSourceReader, InputPartition
```

| PySpark 4 | Snowpark | Notes |
|---|---|---|
| `spark.dataSource.register(cls)` | `session.data_source.register(cls)` | Snowpark uses snake_case naming. `session.dataSource` alias? (open question) |
| `DataSource.name/schema/reader/writer` | same | |
| `DataSourceReader.partitions/read` | same | |
| `DataSourceReader.pushFilters` (4.1) | Phase 3 | |
| `DataSourceWriter.write/commit/abort` | same (Phase 2) | |
| `DataSourceArrowWriter` | Phase 3 | `write(Iterator[RecordBatch])` |
| `DataSourceStreamReader` / `SimpleDataSourceStreamReader` / `DataSourceStreamWriter` | not supported | |
| `schema()` returns `pyspark.sql.types.StructType` | accepted | Changed to Snowpark types with a mapping, if pyspark is installed. |
| `read()` yields `pyspark.sql.Row` | accepted | Duck-typed. |

**Running PySpark data sources unchanged (implemented).** `register()` and `format()` also accept
`pyspark.sql.datasource.DataSource` subclasses, such as third-party PySpark data source libraries.
Snowpark does **not** depend on PySpark:

- The accepted API is the structural protocol `DataSourceLike` / `DataSourceReaderLike`
  (`typing.Protocol`). Validation is duck-typed: a class with callable `name` and `reader`.
- PySpark classes are detected by the module names in the MRO (`pyspark.*`), never by `isinstance`
  against an imported PySpark class. `snowflake/snowpark/_internal/data_source/custom/pyspark_compat.py`
  never imports `pyspark`.
- Schemas are converted through Spark's JSON schema format (`StructType.jsonValue()` /
  `StructType.fromJson()`), using the `pyspark.sql.types` module that the data source already imported
  (`sys.modules`). A `StructType` returned by `schema()` is passed to `reader()` as is; a DDL string or a
  `DataFrameReader.schema()` schema is converted to a PySpark `StructType` with the original field names
  (from `StructField._name`, so `userId` stays `userId`, not `USERID`).
- Spark's `TIMESTAMP` means local time zone, so it maps to `TIMESTAMP_LTZ` (also for DDL strings);
  `TIMESTAMP_NTZ` maps to `TIMESTAMP_NTZ`.
- `pyspark.sql.Row` (`__fields__`) and `pyspark.sql.types.VariantVal` (`toJson()`) are duck-typed in `read()`
  output. A `VariantVal` of JSON null is loaded as SQL NULL.
- `partitions()` raising `NotImplementedError` (older PySpark) falls back to a single `None` partition.
  A stream-only source (`reader()` not implemented) gets a clear "only batch reads" error.
- In UDTF mode, `pyspark` is added to the UDTF packages automatically when the source, reader or
  partitions are PySpark classes (Snowflake's Anaconda channel has `pyspark`). PySpark classes are pickled
  by reference; the user's own modules are still pickled by value.

**Snowpark Connect** can use the same path to handle `spark.dataSource.register` from Spark Connect clients.

## 13. Alternatives Considered

1. **A new, Snowpark-native interface** (for example, one `fetch(options) -> Iterator[pd.DataFrame]`).
   It's simpler, but it ignores an interface many users already know and a set of existing open-source
   data sources (for example, `pyspark-data-sources`). We rejected it in favor of Spark compatibility.
2. **Server mode only (UDTF).** It's simpler and lazy, but it needs an external access integration even
   for sources only the client can reach (localhost, VPN, local files). Local mode is the most common
   `dbapi` mode today, so we keep both.
3. **Lazy local mode in v1** (read when the action runs). It would allow pushdown, but needs the new
   logical plan leaf and plan-time side effects (§4.4). We postponed it to Phase 3 to keep v1 small; the
   v1 public API doesn't block it.
4. **Stored procedure for server-side reads** (run the whole ingestion in a sproc). That runs on a single
   node, so it loses partition parallelism. A UDTF with `PARTITION BY` scales better.

## 14. Phased Delivery

| Phase | Scope | Release tag |
|---|---|---|
| **1** | `datasource.py` (read classes), `Session.data_source`, `read.format(...).load()`, local mode (rows + Arrow), server UDTF mode (row-wise), pyarrow Parquet path, refactor of `local_ingestion` (§8), local testing support, docs and doctests | PrPr |
| **2** | Writers (local + UDTF), vectorized UDTF for Arrow reads, PySpark adapter, `snowpark.secrets`, Snowpark Connect integration | PrPr → PuPr |
| **3** | Lazy `PythonDataSourceRelation`, `pushFilters` + column pruning, `DataSourceArrowWriter`; possibly rebuild `dbapi` on this engine | PuPr → GA |

Phase 1 test plan:

- Unit tests (`tests/unit/`): option splitting, registry (reserved names, case, overriding), DDL parsing
  of Spark type names, row/Arrow → Arrow table conversion for each type, pickling checks.
- Integration tests (`tests/integ/datasource/`): an in-memory range source, a source that yields Arrow,
  multiple partitions with `max_workers` 1 and N, thread vs. process, a failing partition (error message
  includes the index), a type mismatch, server mode against an EAI-free source (data created in the
  reader), and a source whose class is defined in `__main__`.
- Mock tests: local mode under `local_testing_mode=True`.
- Check that `dbapi` behaves the same after the §8 refactor; the existing `tests/integ/datasource`
  suite must pass unchanged.

## 15. Open Questions

1. **Default mode inside a stored procedure or Notebook.** Local mode works there (as `dbapi` does) but
   still uses PUT/COPY. Should we automatically pick server mode when running inside Snowflake and an EAI
   is configured?
2. **`session.dataSource` camelCase alias** for direct PySpark compatibility. The Snowpark API normally
   avoids camelCase aliases on `Session`.
3. **UDTF output typing.** Should we keep the `dbapi` Variant + cast approach, which accepts mismatched
   values better (for example, numeric strings into INT), or use strict declared types (proposed)? We could
   offer `snowpark.lenient_types=true`.
4. **Partition count limits.** A source returning about 1M tiny partitions would make a huge partitions
   table and queue. Should we warn above some threshold, or coalesce partitions automatically?
5. **Session-scoped vs. global registry.** Spark's registry is per SparkSession. Should we also allow
   process-wide registration (`snowflake.snowpark.datasource.register`) so that libraries can register
   sources when imported?
