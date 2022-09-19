# Snowpark Style Guide

The Snowpark library provides intuitive APIs for querying and processing data in a data pipeline.
Using this library, you can build applications that process data in Snowflake without having to move data to the system where your application code runs.

This guide to Snowpark code style demonstrates common situations we've encountered and the associated best practices based on our experience coaching and working with our engineering teams.

Beyond Snowpark specifics, the general practices of clean code are important in Snowpark -- the
[PEP 8][pep-8] is a strong starting point for learning more about these practices.

# Table of Contents

1. [DataFrame Operations](#dataframe-operations)
   1. [Column Selection](#column-selection)
   2. [Aliasing Column Name](#aliasing-column-name)
   3. [Performant SQL Generation](#performant-sql-generation)
2. [Expressions](#expressions)
   1. [Multiple-Lines](#multiple-lines)
3. [Explicitness](#explicitness)
   1. [Calling Methods and Functions with Parameter Names](#calling-methods-and-functions-with-parameter-names)
4. [User Defined Functions (UDFs)](#user-defined-functions-udfs)
   1. [Registration](#registration)
   2. [Type Hints](#type-hints)
5. [User Defined Table Functions (UDTFs)](#user-defined-table-functions-udfts)
6. [Stored Procedures](#stored-procedures)
6. [Miscellaneous](#miscellaneous)

# DataFrame Operations

## Column Selection

In Snowpark, to provide the best development experience for developers coming from various background, 
multiple approaches of column selection are provided for handy
usage as demonstrated in the following snippets:
```python
# select by attribute
df = df.select(df.col_a, df.col_b)

# select by string
df = df.select("col_a", "col_b")

# select by using snowflake.snowpark.functions.col function
df = df.select(col("col_a"), col("col_b"))

# select by Dataframe indexing
df = df.select(df["col_a"], df["col_b"])

# select by using DataFrame.col function
df = df.select(df.col("col_a"), df.col("col_b"))
```

- The attribute approach is shorter and simpler, however, it cannot tackle column names containing
special characters.
- The string approach has the same benefits as the first one, but also supporting names with special characters.
- The `col` function approach will wrap the col as a `Column` object. It's the best of usage when operations on `Column` like aliasing the column name are needed.
- The indexing and `DataFrame.col` approaches offer a way to disambiguate among multiple `DataFrame`.
Suppose there are two `DataFrame` with same column name `col_a` and a `join` operation is wanted, it could be accomplished by:
    ```python
    df = df1.join(df2, df2["col_a"] == df1["col_a"])
    ```

## Aliasing Column Name

Aliasing is typically useful for providing meaningful names for columns.
Sometimes the column names in the database is either composed of long words for the purpose of
being descriptive or too short lacking enough information. Keeping those long column names while manipulating `DataFrame`
could somehow reduce maintainability and readability such that aliasing is wanted.

There are two ways to do aliasing in Snowpark : either `DataFrame.with_column_renamed` or `Column.alias`
suffices the purpose. `with_column_renamed` separates column selection and renaming.
In contrast, `Column.alias` brings the benefit
of locality as well as makes code more compact which further improves readability and conciseness.

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

Operations upon `DataFrame` will ultimately get turned into SQL statements and send to Snowflake for execution.
Non-optimized SQL statements would take longer time to process and execute and sometimes the service even times out.
Snowpark has implemented a SQL simplifier which is turned on by default, and it is build upon certain rules.
To maximize the effectiveness of the simplifier, we propose the following guidelines based on our practices:

<TO BE FILLED>

# Expressions

Chaining of expressions are widely used as they represent atomic logic. Here we are give 
some best practice based on our experiences for writing the expressions.

## Multiple-Lines

In general, it should be avoided to lay out all the expression into a single line which makes code
difficult to read with expressions left behind getting easier to be omitted.

Wrapping the multi-lines expressions within parentheses provides a graceful way to laying out the code.

```python
# all in one line
df = df.select('col_a', 'col_b').filter('col_a' == 'value').filter('cob_b' == True).join(another_dataframe, 'col_c').drop('col_d')

# multiple lines wrapped in parentheses
df = (
    df
    .select('col_a', 'col_b')
    .filter('col_a' == 'value')
    .filter('cob_b' == True)
    .join(another_df, 'col_c')
    .drop('col_d')
)

```

# Explicitness

## Calling Methods and Functions with Parameter Names

It is a good habit to specify the names of parameters when calling methods or functions which helps understand which
roles the parameters play instead of constantly referencing to the API doc to figure out.

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

UDFs could be registered in several ways and each has its own advantages and limitations.


## Registration

**calling `snowflake.snowpark.functions.udf` function**

The `snowflake.snowpark.functions.udf` function will register the function within the most
recently created session. If there is only one single active session in the program, it will use that session
for registration. However, if there are multiple active sessions in your
program and a different session is wanted, the `session` parameter has to be explicitly set.
In this case, you may need to keep a record of all the sessions and maintain extra logic to decide on which
session to use.

```python
from snowflake.snowpark.functions import udf

def add(x, y):
     return x + y

# without session parameter, the most recently created session will be used
udf(
   add,
   return_type=IntegerType(),
   input_types=[IntegerType(), IntegerType()]
)

# or explicitly specifying an active session
udf(
   add,
   return_type=IntegerType(),
   input_types=[IntegerType(), IntegerType()],
   session=active_session
)
```

**using `snowflake.snowpark.functions.udf` as decorator**

`snowflake.snowpark.functions.udf` could be also used as a decorator to annotate the function to be registered.
With the decorator, it is clearer whether the function is a UDF.

```python
# udf as decorator
from snowflake.snowpark.functions import udf

@udf(
   return_type=IntegerType(),
   input_types=[IntegerType(), IntegerType()]
)
def add(x, y):
     return x + y
```

**calling `snowflake.snowpark.Session.udf.register` method**

The `udf` method on `Session` object returns `UDFRegistration` object which could be used for registration.
Inherently, `UDFRegistration` is bound with the `Session` object which means the UDF will be registered
through that session.

```python
def add(x, y):
     return x + y

session.udf.register(
   add,
   return_type=IntegerType(),
   input_types=[IntegerType(), IntegerType()]
)
```

## Type Hints

As illustrated in the above [UDF Registration][#UDF Registration], the typing info needs to be provided.
Other than inputting types as parameters, you could also provide the typing information through Python type hints.
It makes the code more cohesive and self-explanatory.

```python
@udf
def typed_add(x: int, y: int) -> int:
    return x + y
```

# User Defined Table Functions (UDFTs)

Snowflake supports SQL UDFs that return a set of rows, consisting of 0, 1, or multiple rows,
each of which has 1 or more columns. Such UDFs are called [tabular UDFs, table UDFs, or,
most frequently, UDTFs (user-defined table functions)][udtf-service-doc].

UDFTs could be registered similarly as UDFs by `snowflake.snowpark.Session.udtf.register` method
or `snowflake.snowpark.functions.udtf` function.

```python
from snowflake.snowpark.functions import udtf
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

Type hint works for UDTF as well:

```python
@udtf
class TypedGeneratorUDTF:
    def process(self, n: int) -> Iterable[Tuple[int]]:
        for i in range(n):
            yield (i, )
```

# Stored Procedures

[Stored procedures][sproc-service-doc] allow you to write procedural code that executes SQL.
In a stored procedure, you can use programmatic constructs to perform branching and looping.

Stored procedures could be registered similarly as UDFs and UDTFs by `snowflake.snowpark.Session.sproc.register` method

```python
from snowflake.snowpark.functions import sproc
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

Type hint works for Stored Procedures as well:

```python
@sproc
def typed_add(session_, x: int, y: int) -> int:
    return session_.sql(f"select {x} + {y}").collect()[0][0]
```

# Miscellaneous
- Test your code thoroughly through units and integration tests. There are numerous [test cases in Snowpark][snowpark-tests] which you
could get inspiration from, feel free to copy and modify accommodating your scenarios.


[pep-8]: https://peps.python.org/pep-0008/
[udf-service-doc]: https://docs.snowflake.com/en/sql-reference/user-defined-functions.html
[udtf-service-doc]: https://docs.snowflake.com/en/developer-guide/udf/sql/udf-sql-tabular-functions.html
[sproc-service-doc]: https://docs.snowflake.com/en/sql-reference/stored-procedures-overview.html
[snowpark-tests]: https://github.com/snowflakedb/snowpark-python/tree/main/tests
