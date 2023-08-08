# Snowpark Local Testing

Hello and welcome to the Private Preview of the local testing framework for Snowpark. In this PrPr, you will be able to create DataFrames without a connection to Snowflake. This repository serves as a sample project, you can use this as a reference as you're getting started.

For issues or questions, contact jason.freeberg@snowflake.com

## Contents

* [Quickstart](#quickstart)
  + [Env setup](#env-setup)
  + [Basic usage](#basic-usage)
* [Overview](#overview)
* [General Usage Documentation](#general-usage-documentation)
  + [Installation](#installation)
    - [Install from specific commit](#install-from-specific-commit)
  + [Usage](#usage)
  + [Usage with PyTest](#usage-with-pytest)
  + [Usage with Behave](#usage-with-behave)
* [Patching Built-In Functions](#patching-built-in-functions)
* [SQL and Table Operations](#sql-and-table-operations)
  + [Example](#example)
* [Private Preview Notes](#private-preview-notes)
* [Supported APIs](#supported-apis)
  + [Session](#session)
  + [DataFrame](#dataframe)
  + [Scalar Functions](#scalar-functions)
* [Limitations](#limitations)

## Quickstart

### Env setup

1. Clone or download this repo if you haven't already
1. Switch to this directory: `cd local-testing-docs`
1. Create and activate the conda environment

    ```bash
    conda env create --file environment.yml
    conda activate snowpark-local-test
    ```

### Basic usage

1. Create a file, `quickstart.py`
1. Add the following imports:

    ```python
    from snowflake.snowpark.session import Session
    from snowflake.snowpark.mock.connection import MockServerConnection
    ```

1. Create a session object, passing the `MockServerConnection` class

    ```python
    session = Session(MockServerConnection())
    ```

1. Create a dataframe:

    ```python
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
    ```

1. Select and print the third column:

    ```python
    df.select('c').show()
    ```

And with that, you've created and operated on a DataFrame without a connection to Snowflake! See [`quickstart-complete.py`](quickstart-complete.py) if you got stuck.

## Overview

The Snowpark local testing framework allows you to create a Session object without any connection information, from there the API is entirely the same. This means you can run your DataFrame tests in "local mode", and then run the same tests against a live Snowflake connection. The local testing framework uses Pandas dataframe as an in-memory representation of the Snowpark DataFrame class. This is why the `pandas` module is specified in the installation instructions below.

A quick tour of the repo:

- [`project/`](project/): Example stored procedure and DataFrame transformers which are tested with PyTest and Behave
- [`pytest/`](pytest/): Example PyTest test suite using Snowpark local testing
- [`features/`](features/): Example Behave test suite using Snowpark local testing

## General Usage Documentation

### Installation

The Snowpark local testing framework can be installed from the [development branch](https://github.com/snowflakedb/snowpark-python/tree/dev/local-testing) on the public repository. In your requirements.txt, add the following:

```txt
snowflake-snowpark-python[pandas] @ git+https://github.com/snowflakedb/snowpark-python@dev/local-testing
```

Then install it using pip or conda, for example `conda environment create --file environment.yml`. You can look at the [`requirements.txt`](requirements.txt) and [`environment.yml`](environment.yml) of this project for reference.

#### Install from specific commit

The installation instructions above will install Snowpark from the development branch, meaning that any new commits pushed to that branch will be installed to your Python environment the next time you install the package. You can also set the installation to a specific commit using the syntax below:

```txt
snowflake-snowpark-python[pandas] @ git+https://github.com/snowflakedb/snowpark-python@dev/local-testing@29385014c755fe20122fac536358a8ebdeb761bc
```

If you use this approach, you will need to manually update the commit if you want to use features that are added to the branch in future commits.

### Usage

Before running the samples in this repository, you will need to create and activate the conda environment if you haven't done so already.

```bash
conda env create --file environment.yml
conda activate snowpark-local-test
```

And if you want to switch between local mode and a live Snowflake connection, set these environment variables with your connection information. (Tip: you can set these in your bash profile.)

```bash
export SNOWSQL_ACCOUNT=<replace with your account identifer>
export SNOWSQL_USER=<replace with your username>
export SNOWSQL_PWD=<replace with your password>
export SNOWSQL_DATABASE=<replace with your database>
export SNOWSQL_SCHEMA=<replace with your schema>
export SNOWSQL_WAREHOUSE=<replace with your warehouse>
```

### Usage with PyTest

The [`test/`](test/) directory shows an example PyTest suite for the transformers in `project/`. [`conftest.py`](test/conftest.py) adds a custom command line parameter, `--snowflake-session` to the `pytest` command. When you run PyTest without the parameter, the account specified in the environment variables above will be used. Alternatively, you can run it with `pytest --snowflake-session local` to use the local testing mode.

Within [`test_transformers.py`](test/test_transformers.py) you can see how this command line parameter is handled in the `@pytest.fixture` to set the appropriate session.

```bash
# run PyTest with live connection
pytest
```

```bash
# run PyTest in local mode
pytest --snowflake-session local
```

### Usage with Behave

The [`features/`](features/) directory shows an example configuration for using Behave with the local test utility. [`environment.py`](features/environment.py) has a `@before_all` method which sets the Session object depending on the value of `SNOWPARK_LOCAL`: if that env var is set to `1` then the test run will use the local session.

Run the following from the project root:

```bash
export SNOWPARK_LOCAL=1  # uses local session
behave
```

```bash
export SNOWPARK_LOCAL=0  # uses live connection
behave
```

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

### Example

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

    if pytestconfig.getoption('--snowflake-session') == 'local':
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

## Private Preview Notes

As explained in [Installation](#installation), updates will be pushed periodically to the branch `dev/local-testing`, so if you want to use those recent changes you will need to re-run the `pip install ...` or `conda env update ...` commands to re-install the Snowpark package from the branch.

## Supported APIs
<details>
  <summary>Expand</summary>

  ### Column
<!--
Column.alias
Column.as_
Column.asc
Column.asc_nulls_first
Column.asc_nulls_last
Column.astype
Column.between
Column.bitand
Column.bitor
Column.bitwiseAnd
Column.bitwiseOR
Column.bitwiseXOR
Column.bitxor
Column.cast
Column.collate
Column.desc
Column.desc_nulls_first
Column.desc_nulls_last
Column.endswith
Column.eqNullSafe
Column.equal_nan
Column.equal_null
Column.getItem
Column.getName
Column.get_name
Column.in_
Column.isNotNull
Column.isNull
Column.is_not_null
Column.is_null
Column.isin
Column.like
Column.name
Column.over
Column.regexp
Column.rlike
Column.startswith
Column.substr
Column.substring
Column.try_cast
Column.within_group
CaseExpr.when
CaseExpr.otherwise
-->

  ### DataFrame
  #### Methods
- DataFrame.agg <!--
DataFrame.approxQuantile
DataFrame.approx_quantile
DataFrame.cache_result
DataFrame.col-->
- DataFrame.collect<!--
DataFrame.collect_nowait
DataFrame.copy_into_table
DataFrame.corr
-->
- DataFrame.count<!--
DataFrame.cov
DataFrame.createOrReplaceTempView
DataFrame.createOrReplaceView
DataFrame.create_or_replace_temp_view
DataFrame.create_or_replace_view
-->
- DataFrame.crossJoin
- DataFrame.cross_join<!--
DataFrame.crosstab
DataFrame.cube
DataFrame.describe
DataFrame.distinct
DataFrame.drop
DataFrame.dropDuplicates
DataFrame.drop_duplicates
DataFrame.dropna
DataFrame.except_
DataFrame.explain
DataFrame.fillna
-->
- DataFrame.filter
- DataFrame.first<!--
DataFrame.flatten
DataFrame.groupBy
DataFrame.group_by
DataFrame.group_by_grouping_sets
DataFrame.intersect
-->
- DataFrame.join<!--
DataFrame.join_table_function
-->
- DataFrame.limit<!--
DataFrame.minus
-->
- DataFrame.natural_join<!--
DataFrame.orderBy
DataFrame.order_by
DataFrame.pivot
DataFrame.randomSplit
DataFrame.random_split
DataFrame.rename
DataFrame.replace
DataFrame.rollup
DataFrame.sample
DataFrame.sampleBy
DataFrame.sample_by
-->
DataFrame.select<!--
DataFrame.selectExpr
DataFrame.select_expr
-->
- DataFrame.show
- DataFrame.sort<!--
DataFrame.subtract
-->
- DataFrame.take
- DataFrame.toDF<!--
DataFrame.toLocalIterator
DataFrame.toPandas
-->
- DataFrame.to_df<!--
DataFrame.to_local_iterator
DataFrame.to_pandas
DataFrame.to_pandas_batches
-->
- DataFrame.union<!--
DataFrame.unionAll
DataFrame.unionAllByName
DataFrame.unionByName
DataFrame.union_all
DataFrame.union_all_by_name
DataFrame.union_by_name
DataFrame.unpivot
-->
- DataFrame.where
- DataFrame.withColumn<!--
DataFrame.withColumnRenamed
-->
- DataFrame.with_column<!--
DataFrame.with_column_renamed
DataFrame.with_columns
DataFrameNaFunctions.drop
DataFrameNaFunctions.fill
DataFrameNaFunctions.replace
DataFrameStatFunctions.approxQuantile
DataFrameStatFunctions.approx_quantile
DataFrameStatFunctions.corr
DataFrameStatFunctions.cov
DataFrameStatFunctions.crosstab
DataFrameStatFunctions.sampleBy
DataFrameStatFunctions.sample_by
-->

  ### Table
  #### Methods
<!--
Table.delete
-->
- Table.drop_table
<!--
Table.merge
Table.sample
Table.update
WhenMatchedClause.delete
WhenMatchedClause.update
WhenNotMatchedClause.insert
-->

</details>


## Limitations

Apart from the unsupported APIs which are not listed in the above section, here is the list of known limitations that will be addressed in the future.
Please note that there will be unaware limitations, in this case please feel free to reach out to share feedbacks.

- SQL Simplifier must be enabled on the Session (this is the default in the latest Snowpark Python version)
- Altering warehouses, schemas, and other session properties is not currently supported
- Window Functions are not supported
- `DataFrame.join` does not support join with another DataFrame that has the same column names
- `DataFrame.with_column` does not support replace column of the same
- Raw SQL String is in general not supported, e.g., DataFrame.filter doesn't support raw SQL String expression.
- `@sproc` and `@udf` are not support
- There could be gaps between local testing framework and SnowflakeDB in the following areas:
  - Results of calculation regarding the values and data types
  - Columns names
  - Error experience

> Snowflake employees can check [this engineering doc](https://snowflakecomputing.atlassian.net/wiki/spaces/EN/pages/2792784931/Snowpark+Local+Testing+Development+Milestone) to see the open tasks and scheduling.
