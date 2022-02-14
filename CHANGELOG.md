# Release History
## 0.4.0 (2022-02-15)

### New Features
- You can now specify which Anaconda packages to use when defining UDFs.
  - Added `add_packages()`, `get_packages()`, `clear_packages()`, and `remove_package()`, to class `Session`.
  - Added `add_requirements()` to `Session` so you can use a requirements file to specify which packages this session will use.
  - Added parameter `packages` to function `snowflake.snowpark.functions.udf()` and method `UserDefinedFunction.register()` to indicate UDF-level Anaconda package dependencies when creating a UDF.
  - Added parameter `imports` to `snowflake.snowpark.functions.udf()` and `UserDefinedFunction.register()` to specify UDF-level code imports.
- Added a parameter `session` to function `udf()` and `UserDefinedFunction.register()` so you can specify which session to use to create a UDF if you have multiple sessions.
- Added types `Geography` and `Variant` to `snowflake.snowpark.types` to be used as type hints for Geography and Variant data when defining a UDF.
- Added support for Geography geoJSON data.
- Added `Table`, a subclass of `DataFrame` for table operations:
  - Methods `update` and `delete` update and delete rows of a table in Snowflake.
  - Method `merge` merges data from a `DataFrame` to a `Table`.
  - Override method `DataFrame.sample()` with an additional parameter `seed`, which works on tables but not on view and sub-queries.
- Added `DataFrame.to_local_iterator()` and `DataFrame.to_pandas_batches()` to allow getting results from an iterator when the result set returned from the Snowflake database is too large.
- Added `DataFrame.cache_result()` for caching the operations performed on a `DataFrame` in a temporary table.
  Subsequent operations on the original `DataFrame` have no effect on the cached result `DataFrame`.
- Added property `DataFrame.queries` to get SQL queries that will be executed to evaluate the `DataFrame`.
- Added `Session.query_history()` as a context manager to track SQL queries executed on a session, including all SQL queries to evaluate `DataFrame`s created from a session. Both query ID and query text are recorded.
- You can now create a `Session` instance from an existing established `snowflake.connector.SnowflakeConnection`. Use parameter `connection` in `Session.builder.configs()`.
- Added `use_database()`, `use_schema()`, `use_warehouse()`, and `use_role()` to class `Session` to switch database/schema/warehouse/role after a session is created.
- Added `DataFrameWriter.copy_into_table()` to unload a `DataFrame` to stage files.
- Added `DataFrame.unpivot()`.
- Added `Column.within_group()` for sorting the rows by columns with some aggregation functions.
- Added functions `listagg()` and `mode()` to `snowflake.snowflake.functions`.
- Added an optional argument `ignore_nulls` in function `lead()` and `lag()`.
- The `condition` parameter of function `when()` and `iff()` now accepts SQL expressions.

### Improvements
- All function and method names have been renamed to use the snake case naming style, which is more Pythonic. For convenience, some camel case names are kept as aliases to the snake case APIs. It is recommended to use the snake case APIs.
  - Deprecated these methods on class `Session` and replaced them with their snake case equivalents: `getImports()`, `addImports()`, `removeImport()`, `clearImports()`, `getSessionStage()`, `getDefaultSchema()`, `getDefaultSchema()`, `getCurrentDatabase()`, `getFullyQualifiedCurrentSchema()`.
  - Deprecated these methods on class `DataFrame` and replaced them with their snake case equivalents: `groupingByGroupingSets()`, `naturalJoin()`, `withColumns()`, `joinTableFunction()`.
- Property `DataFrame.columns` is now consistent with `DataFrame.schema.names` and the Snowflake database `Identifier Requirements`.
- `Column.__bool__()` now raises a `TypeError`. This will ban the use of logical operators `and`, `or`, `not` on `Column` object, for instance `col("a") > 1 and col("b") > 2` will raise the `TypeError`. Use `(col("a") > 1) & (col("b") > 2)` instead.
- Changed `PutResult` and `GetResult` to subclass `NamedTuple`.
- Fixed a bug which raised an error when the local path or stage location has a space or other special characters.


### Dependency updates
- Updated ``snowflake-connector-python`` to 2.7.4.

## 0.3.0 (2022-01-09)
### New Features
- Added `Column.isin()`, with an alias `Column.in_()`.
- Added `Column.try_cast()`, which is a special version of `cast()`. It tries to cast a string expression to other types and returns `null` if the cast is not possible.
- Added `Column.startswith()` and `Column.substr()` to process string columns.
- `Column.cast()` now also accepts a `str` value to indicate the cast type in addition to a `DataType` instance.
- Added `DataFrame.describe()` to summarize stats of a `DataFrame`.
- Added `DataFrame.explain()` to print the query plan of a `DataFrame`.
- `DataFrame.filter()` and `DataFrame.select_expr()` now accepts a sql expression.
- Added a new `bool` parameter `create_temp_table` to methods `DataFrame.saveAsTable()` and `Session.write_pandas()` to optionally create a temp table.
- Added `DataFrame.minus()` and `DataFrame.subtract()` as aliases to `DataFrame.except_()`.
- Added `regexp_replace()`, `concat()`, `concat_ws()`, `to_char()`, `current_timestamp()`, `current_date()`, `current_time()`, `months_between()`, `cast()`, `try_cast()`, `greatest()`, `least()`, and `hash()` to module `snowflake.snowpark.functions`.

### Bug Fixes
- Fixed an issue where `Session.createDataFrame(pandas_df)` and `Session.write_pandas(pandas_df)` raise an exception when the `Pandas DataFrame` has spaces in the column name.
- `DataFrame.copy_into_table()` sometimes prints an `error` level log entry while it actually works. It's fixed now.
- Fixed an API docs issue where some `DataFrame` APIs are missing from the docs.

### Dependency updates
- Update ``snowflake-connector-python`` to 2.7.2, which upgrades ``pyarrow`` dependency to 6.0.x. Refer to the [python connector 2.7.2 release notes](https://pypi.org/project/snowflake-connector-python/2.7.2/) for more details.

## 0.2.0 (2021-12-02)
### New Features
- Updated the `Session.createDataFrame()` method for creating a `DataFrame` from a Pandas DataFrame.
- Added the `Session.write_pandas()` method for writing a `Pandas DataFrame` to a table in Snowflake and getting a `Snowpark DataFrame` object back.
- Added new classes and methods for calling window functions.
- Added the new functions `cume_dist()`, to find the cumulative distribution of a value with regard to other values within a window partition,
  and `row_number()`, which returns a unique row number for each row within a window partition.
- Added functions for computing statistics for DataFrames in the `DataFrameStatFunctions` class.
- Added functions for handling missing values in a DataFrame in the `DataFrameNaFunctions` class.
- Added new methods `rollup()`, `cube()`, and `pivot()` to the `DataFrame` class.
- Added the `GroupingSets` class, which you can use with the DataFrame groupByGroupingSets method to perform a SQL GROUP BY GROUPING SETS.
- Added the new `FileOperation(session)`
  class that you can use to upload and download files to and from a stage.
- Added the `DataFrame.copy_into_table()`
  method for loading data from files in a stage into a table.
- In CASE expressions, the functions `when()` and `otherwise()`
  now accept Python types in addition to `Column` objects.
- When you register a UDF you can now optionally set the `replace` parameter to `True` to overwrite an existing UDF with the same name.

### Improvements
- UDFs are now compressed before they are uploaded to the server. This makes them about 10 times smaller, which can help
  when you are using large ML model files.
- When the size of a UDF is less than 8196 bytes, it will be uploaded as in-line code instead of uploaded to a stage.

### Bug Fixes
- Fixed an issue where the statement `df.select(when(col("a") == 1, 4).otherwise(col("a"))), [Row(4), Row(2), Row(3)]` raised an exception.
- Fixed an issue where `df.toPandas()` raised an exception when a DataFrame was created from large local data.

## 0.1.0 (2021-10-26)

Start of Private Preview
