# Snowpark Python Custom Data Sources: Getting Started

> **Status: preview build.** This feature is not in a released version of `snowflake-snowpark-python` yet.
> APIs may change. Please send feedback to the Snowpark Python team.

Custom data sources let you read data from **any** system, such as a REST API, a SaaS application, or
a proprietary file format, into a Snowpark `DataFrame`. You write a small Python class, and Snowpark
handles parallel reading, type conversion, and loading the data into Snowflake.

The API follows [PySpark's Python Data Source API](https://spark.apache.org/docs/latest/api/python/tutorial/sql/python_data_source.html).
If you already have a PySpark data source, including third-party libraries such as
[`pyspark-data-sources`](https://github.com/allisonwang-db/pyspark-data-sources), Snowpark can read it
**without any code changes**.

- [1. Install](#1-install)
- [2. Quick start: write a data source](#2-quick-start-write-a-data-source)
- [3. Options and schemas](#3-options-and-schemas)
- [4. What read() can yield](#4-what-read-can-yield)
- [5. Run read() inside Snowflake (server-side ingestion)](#5-run-read-inside-snowflake-server-side-ingestion)
- [6. Use PySpark data sources](#6-use-pyspark-data-sources)
- [7. Reference: engine options](#7-reference-engine-options)
- [8. Limitations and troubleshooting](#8-limitations-and-troubleshooting)

## 1. Install

Install the preview build of Snowpark from its branch. The `pandas` extra installs `pyarrow`, which
custom data sources need. Python 3.10 or later is required.

```bash
pip install "snowflake-snowpark-python[pandas] @ git+https://github.com/snowflakedb/snowpark-python.git@worktree-custom-data-source-phase1"
```

Only to use data sources written with PySpark (section 6), also install PySpark 4. The lightweight
`pyspark-client` package is enough; Java is not needed.

```bash
pip install pyspark-client          # PySpark 4.0 or later
pip install pyspark-data-sources faker   # optional: the third-party library used in the examples
```

Server-side ingestion (section 5) runs your code in Snowflake with the Snowpark version available
there. You don't need to install anything in Snowflake: the data source modules are uploaded with your code.

Create a session as usual, for example from your `connections.toml`:

```python
from snowflake.snowpark import Session

session = Session.builder.getOrCreate()
```

## 2. Quick start: write a data source

A data source has two parts:

- A `DataSource` subclass. It receives the options, and returns the schema and a reader.
- A `DataSourceReader` subclass. `partitions()` splits the work, and `read(partition)` yields the rows
  of one partition. Partitions are read in parallel.

```python
from snowflake.snowpark.datasource import DataSource, DataSourceReader, InputPartition


class NumbersDataSource(DataSource):
    @classmethod
    def name(cls):
        return "numbers"  # the format name used in session.read.format()

    def schema(self):
        return "n INT, square INT, label STRING"

    def reader(self, schema):
        return NumbersReader(int(self.options.get("count", 10)))


class NumbersReader(DataSourceReader):
    def __init__(self, count):
        self.count = count

    def partitions(self):
        # 3 partitions, read in parallel
        return [InputPartition(i) for i in range(3)]

    def read(self, partition):
        for n in range(partition.value, self.count, 3):
            yield (n, n * n, f"number {n}")


session.data_source.register(NumbersDataSource)
df = session.read.format("numbers").option("count", 6).load()
df.sort("n").show()
```

```text
-----------------------------------
|"N"  |"SQUARE"  |"LABEL"         |
-----------------------------------
|0    |0         |number 0        |
|1    |1         |number 1        |
|2    |4         |number 2        |
|3    |9         |number 3        |
|4    |16        |number 4        |
|5    |25        |number 5        |
-----------------------------------
```

The result is a regular Snowpark `DataFrame`, so you can transform it or save it to a table:

```python
df.filter(df["n"] > 2).write.save_as_table("numbers_copy", mode="overwrite", table_type="temp")
```

You can also pass the class directly, without registering it:

```python
df = session.read.format(NumbersDataSource).load()
```

**How it runs.** `load()` calls `schema()`, `reader()` and `partitions()` on your machine. It then calls
`read()` for each partition in parallel threads, uploads the data as Parquet files, and copies them
into a temporary table. The reader and partitions are sent to the worker threads with pickle, so
**they must be picklable**. Open connections and clients inside `read()`, not in `__init__`.

## 3. Options and schemas

**Options.** Everything set with `.option()` / `.options()` is passed to your data source as
`self.options`, a case-insensitive dictionary with string values (like PySpark). An argument passed to
`load(path)` becomes the `path` option.

```python
class GreetingDataSource(DataSource):
    def schema(self):
        return "greeting STRING"

    def reader(self, schema):
        return GreetingReader(self.options["path"], self.options.get("excited", "false"))


class GreetingReader(DataSourceReader):
    def __init__(self, path, excited):
        self.path = path
        self.excited = excited == "true"  # booleans arrive as "true" / "false"

    def read(self, partition):  # partitions() defaults to a single partition
        yield (f"hello {self.path}" + ("!" if self.excited else ""),)


session.read.format(GreetingDataSource).option("EXCITED", True).load("world").show()
```

Options that start with `snowpark.` configure Snowpark itself and are not passed to the data source
(see [section 7](#7-reference-engine-options)).

**Schemas.** `schema()` returns a DDL string such as `"id INT, name STRING"` or a Snowpark
`StructType`. Users can override it with `DataFrameReader.schema()`; then `schema()` isn't called.
`reader(schema)` receives the final schema, so the reader can adapt to it:

```python
from snowflake.snowpark.types import StringType, StructField, StructType


class EchoSchemaDataSource(DataSource):
    def schema(self):
        return "a STRING"

    def reader(self, schema):
        return EchoSchemaReader([f.name for f in schema.fields])


class EchoSchemaReader(DataSourceReader):
    def __init__(self, names):
        self.names = names

    def read(self, partition):
        yield {name: f"value of {name}" for name in self.names}


user_schema = StructType([StructField("x", StringType()), StructField("y", StringType())])
session.read.format(EchoSchemaDataSource).schema(user_schema).load().show()
session.read.format(EchoSchemaDataSource).schema("p STRING, q STRING").load().show()
```

Supported column types: numbers (`INT`, `BIGINT`, `DOUBLE`, `DECIMAL(p, s)`, ...), `STRING`, `BINARY`,
`BOOLEAN`, `DATE`, `TIME`, `TIMESTAMP_NTZ` / `TIMESTAMP_LTZ` / `TIMESTAMP_TZ`, `VARIANT`, `ARRAY`,
`OBJECT`, and `MAP`. `GEOGRAPHY`, `GEOMETRY`, `VECTOR`, `FILE` and `DECFLOAT` are not supported.

## 4. What read() can yield

`read()` can yield any mix of:

| Item | Matched to the schema |
|---|---|
| `tuple` or `list` | by position |
| `dict` | by name; missing keys become `NULL` |
| `snowflake.snowpark.Row` or `pyspark.sql.Row` | by name (or by position if the row has no names) |
| `pyarrow.RecordBatch` or `pyarrow.Table` | by column name, or by position if the names don't match; must have every schema column |

Names are matched case-insensitively, unless the schema field name is quoted (for example `"camelCase"`).
Yielding Arrow batches is the fastest way to return a lot of data.

```python
import datetime
import decimal

import pyarrow as pa

from snowflake.snowpark import Row


class MixedDataSource(DataSource):
    def schema(self):
        return "id INT, price DECIMAL(10, 2), created TIMESTAMP_NTZ, tags ARRAY, info VARIANT"

    def reader(self, schema):
        return MixedReader()


class MixedReader(DataSourceReader):
    def read(self, partition):
        created = datetime.datetime(2024, 1, 2, 3, 4, 5)
        yield (1, decimal.Decimal("9.99"), created, ["a", "b"], {"source": "tuple"})
        yield {"id": 2, "info": {"source": "dict"}}
        yield Row(id=3, price=1.5, created=created, tags=[], info="row")
        yield pa.record_batch(
            {
                "id": [4, 5],
                "price": [2.0, 3.0],
                "created": [created, None],
                "tags": [["c"], None],
                "info": [{"source": "arrow"}, None],
            }
        )


session.read.format(MixedDataSource).load().sort("id").show()
```

Values are converted to the column types: for example, numbers to `DECIMAL`, ISO strings to dates
and timestamps, and Python objects to JSON for `VARIANT`, `ARRAY` and `OBJECT` columns. If a value
can't be converted, `load()` fails with an error that names the column and the partition.

## 5. Run read() inside Snowflake (server-side ingestion)

By default, `read()` runs on your machine and the data is uploaded to Snowflake. To run `read()`
**inside Snowflake** instead, set the `snowpark.external_access_integration` option. Snowpark then
reads each partition in parallel in a temporary Python UDTF, and the data never passes through your
machine. Reading is lazy: it happens when you run an action, such as `collect()` or `save_as_table()`,
on the returned DataFrame.

An [external access integration](https://docs.snowflake.com/en/developer-guide/external-network-access/creating-using-external-network-access)
(EAI) allows the UDTF to reach your external system. An administrator creates it once:

```sql
CREATE OR REPLACE NETWORK RULE api_network_rule
  MODE = EGRESS TYPE = HOST_PORT VALUE_LIST = ('api.example.com');

CREATE OR REPLACE SECRET api_token TYPE = GENERIC_STRING SECRET_STRING = '...';

CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION api_access_integration
  ALLOWED_NETWORK_RULES = (api_network_rule)
  ALLOWED_AUTHENTICATION_SECRETS = (api_token)
  ENABLED = TRUE;

GRANT USAGE ON INTEGRATION api_access_integration TO ROLE my_role;
```

Then read with the same data source as before:

```python
EAI = "API_ACCESS_INTEGRATION"

df = (
    session.read.format("numbers")
    .option("count", 6)
    .option("snowpark.external_access_integration", EAI)
    .load()
)
df.sort("n").show()
```

Packages that `read()` needs in Snowflake go in `snowpark.packages`, and secrets go in
`snowpark.secrets`. Read secrets inside `read()` with the `_snowflake` module, which is only available
in Snowflake. This example calls a REST API with a token; replace the URL, integration and secret
with your own:

```python
class ApiDataSource(DataSource):
    def schema(self):
        return "id INT, name STRING"

    def reader(self, schema):
        return ApiReader(self.options["url"])


class ApiReader(DataSourceReader):
    def __init__(self, url):
        self.url = url

    def read(self, partition):
        import _snowflake  # available in Snowflake only
        import requests

        token = _snowflake.get_generic_secret_string("token")
        response = requests.get(self.url, headers={"Authorization": f"Bearer {token}"}, timeout=30)
        response.raise_for_status()
        yield from response.json()  # a list of {"id": ..., "name": ...} dicts


df = (
    session.read.format(ApiDataSource)
    .option("url", "https://api.example.com/items")
    .option("snowpark.external_access_integration", "API_ACCESS_INTEGRATION")
    .option("snowpark.packages", ["requests"])
    .option("snowpark.secrets", {"token": "api_token"})  # alias -> secret name
    .load()
)
```

Classes defined in your own local modules or notebook are uploaded automatically. Packages installed
with pip are **not**: add them to `snowpark.packages` (see section 6 for packages from PyPI), or
upload them with `snowpark.imports`.

## 6. Use PySpark data sources

Snowpark can read data sources written with `pyspark.sql.datasource` as they are. Snowpark itself
doesn't depend on PySpark; it uses the PySpark that your data source already imports.

### A third-party library

This example uses the `fake` data source from [`pyspark-data-sources`](https://github.com/allisonwang-db/pyspark-data-sources)
(`pip install pyspark-client pyspark-data-sources faker`):

```python
from pyspark_datasources import FakeDataSource

session.data_source.register(FakeDataSource)
session.read.format("fake").option("numRows", 3).load().show()
session.read.format("fake").schema("name string, company string, email string").load().show()
```

```text
---------------------------------------------------------------------------
|"NAME"         |"COMPANY"                   |"EMAIL"                     |
---------------------------------------------------------------------------
|Danny Ward     |Roberts, Johnson and Black  |stephaniebaker@example.com  |
|Ian Gallagher  |Johnson and Sons            |rogersedwin@example.org     |
|Derek Wilson   |Brooks-Pope                 |apatrick@example.net        |
---------------------------------------------------------------------------
```

Other sources in the library work the same way, for example:

```python
from pyspark_datasources import GithubDataSource

session.data_source.register(GithubDataSource)
session.read.format("github").load("snowflakedb/snowpark-python").select("id", "title").show(3)
```

(GitHub limits unauthenticated requests; add `.option("token", "<your GitHub token>")` if you hit the limit.)

To run a third-party PySpark source **inside Snowflake**, the library and its dependencies must be
available in the UDTF. Install them from PyPI with an artifact repository (`pyspark` itself is added
automatically):

```python
df = (
    session.read.format("fake")
    .option("numRows", 4)
    .option("snowpark.external_access_integration", EAI)
    .option("snowpark.artifact_repository", "snowflake.snowpark.pypi_shared_repository")
    .option("snowpark.packages", ["pyspark-data-sources", "faker"])
    .load()
)
df.show()
```

Alternatively, upload the installed library with
`.option("snowpark.imports", [os.path.dirname(pyspark_datasources.__file__)])`.

### Your own PySpark data source

A data source written for Spark works without changes:

```python
from pyspark.sql import Row as SparkRow
from pyspark.sql.datasource import DataSource as SparkDataSource
from pyspark.sql.datasource import DataSourceReader as SparkDataSourceReader
from pyspark.sql.types import IntegerType, StringType
from pyspark.sql.types import StructField as SparkField, StructType as SparkStructType


class UsersDataSource(SparkDataSource):
    @classmethod
    def name(cls):
        return "users"

    def schema(self):
        return SparkStructType([SparkField("userId", IntegerType()), SparkField("userName", StringType())])

    def reader(self, schema):
        return UsersReader(schema)


class UsersReader(SparkDataSourceReader):
    def __init__(self, schema):
        self.schema = schema  # a pyspark.sql.types.StructType

    def read(self, partition):
        for i in range(3):
            yield SparkRow(userId=i, userName=f"user {i}")


session.data_source.register(UsersDataSource)
session.read.format("users").load().show()
```

What to know about PySpark data sources:

- `reader()` receives a `pyspark.sql.types.StructType` with the field names as written (`userId`).
  Snowflake column names are case-insensitive, so the DataFrame columns are `USERID` and `USERNAME`.
- Spark's `TIMESTAMP` is read as `TIMESTAMP_LTZ`; `TIMESTAMP_NTZ` as `TIMESTAMP_NTZ`.
- `pyspark.sql.Row` and `pyspark.sql.types.VariantVal` values are supported.
- `DataFrameReader.schema()` also accepts a `pyspark.sql.types.StructType`.
- Only batch reads are supported. Streaming readers, writers and filter pushdown are not.

## 7. Reference: engine options

| Option | Description |
|---|---|
| `snowpark.external_access_integration` | EAI name. Setting it enables server-side ingestion. |
| `snowpark.packages` | Packages that `read()` needs in Snowflake (server-side). |
| `snowpark.artifact_repository` | Artifact repository for `snowpark.packages`, for example `snowflake.snowpark.pypi_shared_repository` for PyPI. |
| `snowpark.imports` | Stage files or local files and directories to upload (server-side). |
| `snowpark.secrets` | `{alias: secret_name}`. Read with `_snowflake.get_generic_secret_string(alias)` (server-side). |
| `snowpark.max_workers` | Number of parallel threads or processes on the client. Defaults to the CPU count. |
| `snowpark.fetch_with_process` | Use processes instead of threads on the client. Defaults to `False`. |
| `snowpark.batch_size` | Maximum rows per uploaded Parquet file on the client. Defaults to 100000. |

The data source API is documented in the `snowflake.snowpark.datasource` module
(`help(snowflake.snowpark.datasource)`).

## 8. Limitations and troubleshooting

- **"must be picklable"**: the reader or a partition holds something like a connection, client, lock or
  open file. Create it inside `read()` instead.
- **`ModuleNotFoundError` in server-side ingestion**: a package used by the reader is missing in Snowflake.
  Add it to `snowpark.packages` (with `snowpark.artifact_repository` for PyPI packages), or upload it
  with `snowpark.imports`.
- **Failed partitions**: if `read()` fails before yielding any data, Snowpark retries the partition. If
  it fails after yielding data, the read fails to avoid duplicate rows.
- **Process mode** (`snowpark.fetch_with_process`) in a script needs the usual `if __name__ == "__main__":` guard.
- **Local testing mode** (`Session.builder.config("local_testing", True)`) supports client-side reads only.
- Not supported yet: writing to data sources, streaming, filter pushdown and column pruning.
