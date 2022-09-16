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
2. [Multiple-line Expressions](#multiple-line-expressions)
3. [Explicitness](#explicitness)
4. [Modularization](#modularization)
5. [Comments](#comments)
6. [Miscellaneous](#miscellaneous)

# DataFrame Operations

## Column Selection

In Snowpark, to provide the best development experience for developers coming from various background, 
multiple approaches of column selection are provided for handy
usage as demonstrated in the following snippets:
```python
# bad: select by attribute
df = df.select(df.col_a, df.col_b)

# good: select by string
df = df.select("col_a", "col_b")

# good: select by using snowflake.snowpark.functions.col function
df = df.select(col("col_a"), col("col_b"))

# good: select by Dataframe indexing
df = df.select(df["col_a"], df["col_b"])

# good: select by using DataFrame.col function
df = df.select(df.col("col_a"), df.col("col_b"))
```

- In general, the first styles shall be avoided and referencing
the column by its name, using a string is recommended, as in the second example or other forms depending
on the scenarios. The limitations of the first style are discussed thoroughly
in the [Palantir PySpark Style Guide][palantir-pyspark-style-guide].

- The `col` function approach will wrap the col as a `Column` object. It's best of usage when operations on `Column` like aliasing the column name are needed.
- The indexing and `DataFrame.col` approach offer a way to disambiguate among multiple `DataFrame`.
Suppose there are two `DataFrame` with same column names and a `join` operation is wanted, it could be accomplished by:
    ```python
    df = df1.join(df2, df2["col_a"] == df1["col_a"])
    ```

## Aliasing Column Name

Aliasing is typically useful for providing meaningful names for columns.
Sometimes the column names in the database is either composed of long words for the purpose of
being descriptive or too short lacking information. Keeping those long column names while manipulating `DataFrame`
could somehow reduce maintainability and readability such that aliasing is wanted.

There are two ways to do aliasing in Snowpark : either `DataFrame.with_column_renamed` or `Column.alias` could
suffice the purpose, however, the latter one is preferred to the former one. The `Column.alias` brings the benefit
of locality as well as makes code more compact which further improve readability and conciseness.

```python
# bad
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

# good
order = order.select(
    'restaurant_id',
    'order_id',
    'order_type',
    col('order_customer_id').alias('customer_id'),
    col('cost').alias('order_cost'),
    col('time').alias('order_time')
)
```

# Expressions

Chaining of expressions are widely used as they represent atomic logic. Here we are give 
some best practice based on our experiences for writing the expressions.

## Multiple-Lines

In general, it should be avoided to lay out all the expression into a single line which makes code
difficult to read with expressions left behind getting easier to be omitted.

Wrapping the multi-lines expressions within parentheses provides a graceful way for laying out.

```python
# bad: all in one line
df = df.select('col_a', 'col_b').filter('col_a' == 'value').filter('cob_b' == True).join(another_dataframe, 'col_c').drop('col_d')

# good: multiple lines wrapped in parentheses
df = (
    df
    .select('col_a', 'col_b')
    .filter('col_a' == 'value')
    .filter('cob_b' == True)
    .join(another_df, 'col_c')
    .drop('col_d')
)

```

## Separation

Instead of having one single chain of expressions, consider separating it into multiple groups
which makes the developer easier to understand how `DataFrame` is constructed and enhance maintainability.


```python
df = (
    df
    .select('col_a', 'col_b')
    .filter('col_a' == 'value')
    .filter('cob_b' == True)
    .join(another_df, 'col_c')
    .drop('col_d')
)

# consider separating into multiple groups as following

# select and filter the data
df = (
    df
    .select('col_a', 'col_b')
    .filter('col_a' == 'value')
    .filter('cob_b' == True)
)

# join with another DataFrame
df = (
    df
    .join(another_df, 'col_c')
    .drop('col_d')
)
```

## Encapsulation

If there are multiple groups of operations n

```python

```


# Explicitness

## Calling Methods and Functions with Parameter Names

```python
# bad
flights = flights.join(aircraft, 'aircraft_id', 'inner')

# good
flights = flights.join(aircraft, using_columns='aircraft_id', how='inner')
```


## Aliasing Imports

```python
# no aliasing
from snowflake.snowpark import types
types.IntegerType
types.BinaryType
types.StringType

# aliasing for disambiguity
from snowflake.snowpark import types as snowflake_types
snowflake_types.IntegerType
snowflake_types.BinaryType
snowflake_types.StringType
```

# Comments

```python

```
TODO: detailed comments explaining code

# Miscellaneous


[pep-8]: https://peps.python.org/pep-0008/
[palantir-pyspark-style-guide]: https://github.com/palantir/pyspark-style-guide#prefer-implicit-column-selection-to-direct-access-except-for-disambiguation