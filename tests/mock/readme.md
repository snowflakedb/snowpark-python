# Snowpark Local Testing

Snowpark Local Testing allows the usage of creating DataFrames and
performing operations without a connection to Snowflake.

## Quickstart

Instead of using `Session.SessionBuilder` to create a session, in local testing,
instantiate `Session` with a `MockServerConnection` object.

```python
from snowflake.snowpark.session import Session
from snowflake.snowpark.mock.connection import MockServerConnection
session = Session(MockServerConnection())
df = session.create_dataframe(
    [
        [1, 2, "welcome"],
        [3, 4, "to"],
        [5, 6, "the"],
        [7, 8, "private"],
        [9, 0, "preview"]
    ],
    schema=['a', 'b', 'c']
)
df.select('c').show()
```

## General Usage Documentation

### Installation

The Snowpark local testing framework can be installed from the [development branch](https://github.com/snowflakedb/snowpark-python/tree/dev/local-testing) on the public repository. In your requirements.txt, add the following:

```bash
pip install "snowflake-snowpark-python[pandas]@git+https://github.com/snowflakedb/snowpark-python@dev/local-testing"
```

#### Install from specific commit

The installation instructions above will install Snowpark from the development branch, meaning that any new commits pushed to that branch will be installed to your Python environment the next time you install the package. You can also set the installation to a specific commit using the syntax below:

```bash
pip install "snowflake-snowpark-python[pandas]@git+https://github.com/snowflakedb/snowpark-python@dev/local-testing@29385014c755fe20122fac536358a8ebdeb761bc"
```

If you use this approach, you will need to manually update the commit if you want to use features that are added to the branch in future commits.


## Patching Built-In Functions

Not all the built-in functions under `snowflake.snowpark.functions` have been re-implemented for the local testing framework.
So if you use a function which is not compatible, you will need to use the `@patch` decorator from `snowflake.snowpark.mock.functions` to create a patch.

To define and implement the patched function, the signature (the parameter list) must align with the built-in function, and the local testing framework
will pass parameters to the patched function by the following rules:
- for parameter of `ColumnOrName` type in the signature of built-in functions, `ColumnEmulator` will be passed as the parameter  of the patched functions, `ColumnEmulator` is a pandas.Series-like object containing the column data.
- for parameter of `LiteralType` type in the signature of built-in functions, the literal value will be passed as the parameter of the patched functions.
- for parameter of non-string Python type in the signature of built-in functions, the raw value will be passed as the parameter of the patched functions.

As for the returning type of the patched functions, returning an instance of `ColumnEmulator` is expected in correspondence with the returning type of `Column` of built-in functions.

For example, the built-in function `to_timestamp()` could be patched as so:

```python
import datetime
from snowflake.snowpark.session import Session
from snowflake.snowpark.mock.connection import MockServerConnection
from snowflake.snowpark.mock.functions import patch
from snowflake.snowpark.functions import to_timestamp
from snowflake.snowpark.mock.snowflake_data_type import ColumnEmulator
from snowflake.snowpark.types import TimestampType

@patch(to_timestamp)
def mock_to_timestamp(column: ColumnEmulator, format = None) -> ColumnEmulator:
    ret_column = ColumnEmulator(data=[datetime.datetime.strptime(row, '%Y-%m-%dT%H:%M:%S%z') for row in column])
    ret_column.sf_type = TimestampType()
    return ret_column

session = Session(MockServerConnection())
df = session.create_dataframe(
    [
        ["2023-06-20T12:00:00+00:00"],
    ],
    schema=["a"],
)
df = df.select(to_timestamp("a"))
df.collect()
```

Let's go through the above patch conceptually: the first line of the method iterates down the rows of the given column, using `strptime()` to do the timestamp conversion. The following line sets the type of the column, and then the column is returned. Note that the implementation of the patch does not necessarily need to re-implement the built-in; the patch could return static values *if* that fulfills the test case.

Let's do another example, this time for `parse_json()`. Similarly, the implementation iterates down the given column and uses a Python method to transform the data--in this case, using `json.loads()`.

```python
import json
from snowflake.snowpark.mock.functions import patch
from snowflake.snowpark.functions import parse_json
from snowflake.snowpark.types import VariantType
from snowflake.snowpark.mock.snowflake_data_type import ColumnEmulator

@patch(parse_json)
def mock(col: ColumnEmulator):
    ret_column = ColumnEmulator(data=[json.loads(row) for row in col])
    ret_column.sf_type = VariantType()
    return ret_column
```

## SQL and Table Operations

`Session.sql` is not supported in Local Testing due to the complexity of parsing SQL texts. Please use DataFrame API where possible, otherwise we suggest mocking this method using Python's builtin mock module:

#### Example

```python
from unittest import mock
from functools import partial
def test_something(pytestconfig, session):

    def mock_sql(session, sql_string):  # patch for SQL operations
        if sql_string == "select 1,2,3":
            return session.create_dataframe([[1,2,3]])
        if sql_string == "select * from shared_table":
            return session.create_dataframe(session.table("shared_table"))
        elif sql_string == "drop table shared_table":
            session.table("shared_table").drop_table()
            return session.create_dataframe([])
        else:
          raise RuntimeError(f"Unexpected query execution: {sql_string}")

    if pytestconfig.getoption('--snowflake-session'):
        mock.patch.object(session, 'sql', wraps=partial(mock_sql, session))

    assert session.sql("select 1,2,3").collect() == [[1,2,3]]
    assert session.sql("select * from shared_table").collect() == [[1,2],[3,4]]

    session.table("shared_table").delete()
    assert session.sql("select * from shared_table").collect() == []
```

Currently, the only supported operations on `snowflake.snowpark.Table` are

- `DataFrame.save_as_table()`
- `Session.table()`
- `Table.drop_table()`

Where all tables created by `DataFrame.save_as_table` are saved as temporary tables in memory and can be retrieved using `Session.table`. You can use the supported `DataFrame` operations on `Table` as usual, but `Table`-specific API's other than `Table.drop_table` are not supported yet.


## Supported APIs

### Session

- `.create_dataframe()`

### DataFrame

- `.select()`
- `.sort()`
- `.filter()` and `.where()`
- `.agg()`
- `.join()`
- `.union()`
- `.take()`
- `.first()`
- `.sort()`
- `.with_column()`

### Scalar Functions

- `min()`
- `max()`
- `sum()`
- `count()`
- `contains()`
- `abs()`

> If a scalar function is not in the list above, you can [patch it](#patching-built-in-functions).

## Limitations

Apart from the unsupported APIs which are not listed in the above section, here is the list of known limitations that will be addressed in the future.
Please note that there will be unaware limitations, in this case please feel free to reach out to share feedbacks.

- SQL Simplifier must be enabled on the Session (this is the default in the latest Snowpark Python version)
- Altering warehouses, schemas, and other session properties is not currently supported
- Stored Procedures and UDFs are not supported
- Window Functions are not supported
- `DataFrame.join` does not support join with another DataFrame that has the same column names
- `DataFrame.with_column` does not support replace column of the same
- Raw SQL String is in general not supported, e.g., DataFrame.filter doesn't support raw SQL String expression.
- There could be gaps between local testing framework and SnowflakeDB in the following areas:
  - Results of calculation regarding the values and data types
  - Columns names
  - Error experience
