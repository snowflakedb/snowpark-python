# Snowpark Style Guide

The Snowpark library provides intuitive APIs for querying and processing data in a data pipeline.
Using this library, you can build applications that process data in Snowflake without having to move data to the system where your application code runs.

This guide to Snowpark code style demonstrates common situations we've encountered and the associated best practices based on our experience coaching and working with our engineering teams.

Beyond Snowpark specifics, the general practices of clean code are important in Snowpark -- the
[PEP 8][pep-8] is a strong starting point for learning more about these practices.

# Table of Contents

1. [DataFrame Operations](#dataframe-operations)
   1. [Column Selection](#column-selection)
   2. [Aliasing](#aliasing)
2. [Multiple-line Expressions](#multiple-line-expressions)
3. [Explicitly Specifying Parameters](#explicitly-specifying-parameters)
4. [Modularization](#modularization)
5. [Comments](#comments)
6. [Miscellaneous](#miscellaneous)

# DataFrame Operations

## Column selection

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

## Aliasing

TODO: `alias` is preferred to `with_column`

# Multiple-line Expressions

TODO: long expression df operation, logical

# Explicitly Specifying Parameters

TODO: specifying argument name, verbose for explicitness

# Modularization

TODO: for complicated operation, write a function

# Comments

TODO: detailed comments explaining code

# Miscellaneous


[pep-8]: https://peps.python.org/pep-0008/
[palantir-pyspark-style-guide]: https://github.com/palantir/pyspark-style-guide#prefer-implicit-column-selection-to-direct-access-except-for-disambiguation