# Snowpark Style Guide

The Snowpark library provides intuitive APIs for querying and processing data in a data pipeline.
Using this library, you can build applications that process data in Snowflake without having to move data to the system where your application code runs.

This guide to Snowpark code style demonstrates common situations we've encountered and the associated best practices based on our experience working with our engineering teams.

Beyond Snowpark specifics, the general practices of clean code are important in Snowpark. The
[PEP 8][pep-8] is a good starting point for learning more about these practices.

# Table of Contents

1. [DataFrame Operations](#dataframe-operations)
   1. [Column Selection](#column-selection)
   2. [Aliasing Column Names](#aliasing-column-names)
   3. [Performant SQL Generation](#performant-sql-generation)
2. [Expressions](#expressions)
   1. [Use Multiple Lines](#use-multiple-lines)
3. [Explicitness](#explicitness)
   1. [Calling Methods and Functions with Parameter Names](#calling-methods-and-functions-with-parameter-names)
4. [User Defined Functions (UDFs)](#user-defined-functions-udfs)
   1. [Registration](#registration)
   2. [Type Hints](#type-hints)
   3. [pandas (Vectorized) UDFs](#pandas-vectorized-udfs)
5. [User Defined Table Functions (UDTFs)](#user-defined-table-functions-udtfs)
6. [Stored Procedures](#stored-procedures)
6. [Miscellaneous](#miscellaneous)

# DataFrame Operations

## Column Selection

In Snowpark, to provide a convenient experience for developers from various backgrounds,
different ways to do column selection are provided as shown in the following snippets:
```python
from snowflake.snowpark.functions import col, lit, sql_expr

# select by attribute
df = df.select(df.col_a, df.col_b)

# select by string
df = df.select("col_a", "col_b")

# select by using snowflake.snowpark.functions.col function
df = df.select(col("col_a"), col("col_b"))

# select by using snowflake.snowpark.functions.lit function
df = df.select(lit(2), lit("x"), lit([1,2]))

# select by using snowflake.snowpark.functions.sql_expr function
df = df.select(sql_expr("col_a + 1"), sql_expr("col_b + 1"))

# select by DataFrame indexing
df = df.select(df["col_a"], df["col_b"])

# select by using DataFrame.col function
df = df.select(df.col("col_a"), df.col("col_b"))
```

- The attribute approach is shorter and simpler, however, it cannot handle column names that contain
special characters.
- The string approach has the same benefits as the first one, but also supports names with special characters.
- The `col` function approach will wrap the col as a `Column` object. Use it when operations on a `Column` like aliasing the column name are needed.
- The `lit` function approach is used to select constant Python values.
- The `sql_expr` function approach will pass the value as raw SQL text. It provides assistance if you have a SQL expression to select.
- The indexing and `DataFrame.col` approaches offer a way to disambiguate among multiple `DataFrame` objects.
Suppose there are two `DataFrame` objects with the same column name `col_a` and you want to do a `join` operation, it could be accomplished by:
    ```python
    df = df1.join(df2, df2["col_a"] == df1["col_a"])
    ```

## Aliasing Column Names

Aliasing lets you provide meaningful names to columns.
Sometimes the column names in a database are either composed of long words, for the purpose of
being descriptive, or too short, lacking enough information. Keeping those long column names while manipulating the `DataFrame`
might reduce maintainability and readability so aliasing is useful.

There are two ways to do aliasing in Snowpark: `DataFrame.with_column_renamed` or `Column.alias`.
When you use `with_column_renamed` it separates column selection and renaming.
In contrast, `Column.alias` brings the benefit
of locality and makes the code more compact, which further improves readability and conciseness.

```python
# with_column_renamed
order = order.select(
    'restaurant_id',
    'order_id',
    'order_type',
    'order_customer_id',
    'price',
    'time'
)
order = order.with_column_renamed('order_customer_id', 'customer_id')
order = order.with_column_renamed('price', 'order_price')
order = order.with_column_renamed('time', 'order_time')

# alias
order = order.select(
    'restaurant_id',
    'order_id',
    'order_type',
    col('order_customer_id').alias('customer_id'),
    col('cost').alias('order_cost'),
    col('time').alias('order_time')
)
```

## Performant SQL Generation

Operations on a `DataFrame` are lazily evaluated to SQL statements and sent to Snowflake for execution.
Non-optimized SQL statements would take a longer time to process and execute, and long-running queries might time out.
Snowpark has implemented a SQL simplifier which is turned on by default. It is built upon certain rules.
To maximize the effectiveness of the SQL simplifier, we propose the following guidelines:

### Calling `DataFrame.with_columns` is preferred over `DataFrame.with_column`

Using `with_column` will add another layer of `select` statement within the internal SQL execution plan.
Instead of calling `DataFrame.with_column` multiple times, use `DataFrame.with_columns` to batch your operations.

```python
# unperformant
df = df.with_column("mean", (df["a"] + df["b"]) / 2)
df = df.with_column("sum", df["a"] +  df["b"]).sort(df["a"]).show()

# performant
df = df.with_columns(
   ["mean", "sum"],
   [(df["a"] + df["b"]) / 2, df["a"] + df["b"]]
).sort(df["a"]).show()
```

# Expressions

Chaining of expressions is widely used because they represent atomic logic. Here are
some best practices for writing the expressions.

## Use Multiple Lines

In general, don't put the expression into a single line. That makes code
difficult to read.

Wrapping the multi-line expressions within parentheses provides a graceful way to lay out the code.

```python
# Expressions are all in one line. Don't do this.
df = df.select('col_a', 'col_b').filter(df['col_a'] == 'value').filter(df['cob_b'] == True).join(right=another_dataframe, on='col_c').drop('col_d')

# Multiple lines are wrapped in parentheses.
df = (
    df
    .select('col_a', 'col_b')
    .filter(df['col_a'] == 'value')
    .filter(df['cob_b'] == True)
    .join(another_df, 'col_c')
    .drop('col_d')
)

```

# Explicitness

## Calling Methods and Functions with Parameter Names

It is a good habit to specify the names of parameters when calling methods or functions. This helps you to understand which
roles the parameters play instead of constantly referencing the API documentation.

```python
# without parameter name
df = df.join(another_df, 'id', 'inner')
df = df.unpivot("sales", "month", ["jan", "feb"]).sort("empid")

# with parameter name
df = df.join(another_df, using_columns='id', how='inner')
df = df.unpivot(value_column="sales", name_column="month", column_list=["jan", "feb"]).sort("empid")
```

# User Defined Functions (UDFs)

[User Defined Functions (UDFs)][udf-service-doc] let you extend the system to perform customized operations and Snowpark
lets you use the Python programming language to manipulate data and return either scalar or tabular results.

UDFs can be registered in several ways and each way has its own advantages and limitations.


## Registration

**Calling the `snowflake.snowpark.functions.udf` function**

The `snowflake.snowpark.functions.udf` function will register the function within the most
recently created session. If there is only one single active session in the program, it will use that session
for registration. However, if there are multiple active sessions in your
program and a different session is wanted, the `session` parameter has to be explicitly set.
In this case, you may need to keep a record of all the sessions and maintain extra logic to decide which
session to use. It is recommended to explicitly specify the session parameter when registering.

```python
from snowflake.snowpark.functions import udf
from snowflake.snowpark.types import IntegerType

def add(x, y):
     return x + y

# without session parameter, the most recently created session will be used
udf(
   add,
   return_type=IntegerType(),
   input_types=[IntegerType(), IntegerType()]
)

# explicitly specify an active session
udf(
   add,
   return_type=IntegerType(),
   input_types=[IntegerType(), IntegerType()],
   session=active_session
)
```

**Using the `snowflake.snowpark.functions.udf` as decorator**

`snowflake.snowpark.functions.udf` can be also used as a decorator to annotate the function to be registered.
With the decorator, it is more clear that the function is a UDF.

```python
# udf as decorator
from snowflake.snowpark.functions import udf
from snowflake.snowpark.types import IntegerType

@udf(
   return_type=IntegerType(),
   input_types=[IntegerType(), IntegerType()]
)
def add(x, y):
     return x + y
```

**Calling the `snowflake.snowpark.Session.udf.register` method**

The `udf` method on `Session` object returns `UDFRegistration` object which can be used for registration.
Inherently, `UDFRegistration` is bound with the `Session` object which means the UDF will be registered
through that session.

```python
from snowflake.snowpark.types import IntegerType

def add(x, y):
     return x + y

session.udf.register(
   add,
   return_type=IntegerType(),
   input_types=[IntegerType(), IntegerType()]
)
```

## pandas (Vectorized) UDFs

pandas UDFs are functions that receive batches of input rows as pandas DataFrames and return
a batch of results as pandas Series. Compared to the default row-by-row Python UDFs, your code will get better
performance if it operates efficiently on batches of rows and there will be less transformation logic required if
you are calling into libraries that operate on pandas DataFrames or pandas arrays.

pandas (Vectorized) UDFs can be registered similarly to UDFs by using the `snowflake.snowpark.functions.pandas_udf` or the
`snowflake.snowpark.functions.udf` function.

```python
from snowflake.snowpark.functions import pandas_udf, udf
from snowflake.snowpark.types import IntegerType, PandasSeriesType, PandasDataFrameType

# using pandas_udf as decorator to register pandas(Vectorized) UDFs
@pandas_udf(
  return_type=PandasSeriesType(IntegerType()),
  input_types=[PandasDataFrameType([IntegerType(), IntegerType()])],
)
def add_one_df_pandas_udf(df):
  return df[0] + df[1] + 1

# using pandas_udf to register
add_series_pandas_udf = pandas_udf(
  lambda df: df[0] + df[1] + 1,
  return_type=PandasSeriesType(IntegerType()),
  input_types=[PandasDataFrameType([IntegerType(), IntegerType()])],
)

# using udf to register pandas(Vectorized) UDFs
add_series_pandas_udf = udf(
  lambda df: df[0] + df[1] + 1,
  return_type=PandasSeriesType(IntegerType()),
  input_types=[PandasDataFrameType([IntegerType(), IntegerType()])],
)
```

## Type Hints

As illustrated in the section about [UDF Registration][#UDF Registration], the typing info must be provided.
Other than inputting types as parameters, you can also provide the typing information through Python type hints.
It makes the code more cohesive and self-explanatory.

```python
from snowflake.snowpark.functions import udf

@udf
def typed_add(x: int, y: int) -> int:
    return x + y
```

Apart from Python primitive types, Snowflake types (`Variant`, `Geography`, etc.)
are available in the module `snowflake.snowpark.types`.

```python
from snowflake.snowpark.functions import udf
from snowflake.snowpark.types import Variant

@udf
def typed_func(x: Variant) -> Variant:
    return x
```

# User Defined Table Functions (UDTFs)

Snowflake supports SQL UDFs that return a set of rows, consisting of 0, 1, or multiple rows,
each of which has 1 or more columns. Such UDFs are called [tabular UDFs, table UDFs, or,
most frequently, UDTFs (user-defined table functions)][udtf-service-doc].

UDTFs can be registered similarly to UDFs by using the `snowflake.snowpark.Session.udtf.register` method
or the `snowflake.snowpark.functions.udtf` function.

```python
from snowflake.snowpark.functions import udtf
from snowflake.snowpark.types import IntegerType, StructField, StructType

class GeneratorUDTF:
    def process(self, n):
        for i in range(n):
            yield (i, )

# using snowflake.snowpark.functions.udtf
udtf(
    GeneratorUDTF,
    output_schema=StructType([StructField("number", IntegerType())]),
    input_types=[IntegerType()]
)

# using snowflake.snowpark.Session.udtf.register
session.udtf.register(
   GeneratorUDTF,
    output_schema=StructType([StructField("number", IntegerType())]),
    input_types=[IntegerType()]
)
```

Type hints work for UDTFs as well:

```python
from snowflake.snowpark.functions import udtf
from typing import Iterable, Tuple

@udtf(output_schema=["number"])
class TypedGeneratorUDTF:
    def process(self, n: int) -> Iterable[Tuple[int]]:
        for i in range(n):
            yield (i, )
```

# Stored Procedures

[Stored procedures][sproc-service-doc] allow you to write procedural code that executes SQL.
In a stored procedure, you can use programmatic constructs to perform branching and looping.

Stored procedures can be registered similarly to UDFs and UDTFs by using the `snowflake.snowpark.Session.sproc.register` method.

```python
from snowflake.snowpark.functions import sproc
from snowflake.snowpark.types import IntegerType

def add(session_, x, y):
    return session_.sql(f"select {x} + {y}").collect()[0][0]

# using snowflake.snowpark.functions.sproc
sproc(
    add,
    return_type=IntegerType(),
    input_types=[IntegerType()]
)

# using snowflake.snowpark.Session.udtf.register
session.sproc.register(
    add,
    return_type=IntegerType(),
    input_types=[IntegerType()]
)
```

Type hints work for stored procedures as well:

```python
from snowflake.snowpark.functions import sproc

@sproc
def typed_add(session_, x: int, y: int) -> int:
    return session_.sql(f"select {x} + {y}").collect()[0][0]
```

# Miscellaneous

Test your code thoroughly with unit tests and integration tests. There are numerous [test cases in Snowpark][snowpark-tests] to
get inspiration from.


[pep-8]: https://peps.python.org/pep-0008/
[udf-service-doc]: https://docs.snowflake.com/en/sql-reference/user-defined-functions.html
[udtf-service-doc]: https://docs.snowflake.com/en/developer-guide/udf/sql/udf-sql-tabular-functions.html
[sproc-service-doc]: https://docs.snowflake.com/en/sql-reference/stored-procedures-overview.html
[snowpark-tests]: https://github.com/snowflakedb/snowpark-python/tree/main/tests
