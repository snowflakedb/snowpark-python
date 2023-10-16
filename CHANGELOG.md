# Release History

## 1.9.0 (2023-10-13)

### New Features

- Added support for the Python 3.11 runtime environment.

### Dependency updates

- Added back the dependency of `typing-extensions`.

### Bug Fixes

- Fixed a bug where imports from permanent stage locations were ignored for temporary stored procedures, UDTFs, UDFs, and UDAFs.
- Revert back to using CTAS (create table as select) statement for `Dataframe.writer.save_as_table` which does not need insert permission for writing tables.

### New Features
- Support `PythonObjJSONEncoder` json-serializable objects for `ARRAY` and `OBJECT` literals.

## 1.8.0 (2023-09-14)

### New Features

- Added support for VOLATILE/IMMUTABLE keyword when registering UDFs.
- Added support for specifying clustering keys when saving dataframes using `DataFrame.save_as_table`.
- Accept `Iterable` objects input for `schema` when creating dataframes using `Session.create_dataframe`.
- Added the property `DataFrame.session` to return a `Session` object.
- Added the property `Session.session_id` to return an integer that represents session ID.
- Added the property `Session.connection` to return a `SnowflakeConnection` object .

- Added support for creating a Snowpark session from a configuration file or environment variables.

### Dependency updates

- Updated ``snowflake-connector-python`` to 3.2.0.

### Bug Fixes

- Fixed a bug where automatic package upload would raise `ValueError` even when compatible package version were added in `session.add_packages`.
- Fixed a bug where table stored procedures were not registered correctly when using `register_from_file`.
- Fixed a bug where dataframe joins failed with `invalid_identifier` error.
- Fixed a bug where `DataFrame.copy` disables SQL simplfier for the returned copy.
- Fixed a bug where `session.sql().select()` would fail if any parameters are specified to `session.sql()`

## 1.7.0 (2023-08-28)

### New Features

- Added parameters `external_access_integrations` and `secrets` when creating a UDF, UDTF or Stored Procedure from Snowpark Python to allow integration with external access.
- Added support for these new functions in `snowflake.snowpark.functions`:
  - `array_flatten`
  - `flatten`
- Added support for `apply_in_pandas` in `snowflake.snowpark.relational_grouped_dataframe`.
- Added support for replicating your local Python environment on Snowflake via `Session.replicate_local_environment`.

### Bug Fixes

- Fixed a bug where `session.create_dataframe` fails to properly set nullable columns where nullability was affected by order or data was given.
- Fixed a bug where `DataFrame.select` could not identify and alias columns in presence of table functions when output columns of table function overlapped with columns in dataframe.

### Behavior Changes

- When creating stored procedures, UDFs, UDTFs, UDAFs with parameter `is_permanent=False` will now create temporary objects even when `stage_name` is provided. The default value of `is_permanent` is `False` which is why if this value is not explicitly set to `True` for permanent objects, users will notice a change in behavior.
- `types.StructField` now enquotes column identifier by default.

## 1.6.1 (2023-08-02)

### New Features

- Added support for these new functions in `snowflake.snowpark.functions`:
  - `array_sort`
  - `sort_array`
  - `array_min`
  - `array_max`
  - `explode_outer`
- Added support for pure Python packages specified via `Session.add_requirements` or `Session.add_packages`. They are now usable in stored procedures and UDFs even if packages are not present on the Snowflake Anaconda channel.
  - Added Session parameter `custom_packages_upload_enabled` and `custom_packages_force_upload_enabled` to enable the support for pure Python packages feature mentioned above. Both parameters default to `False`.
- Added support for specifying package requirements by passing a Conda environment yaml file to `Session.add_requirements`.
- Added support for asynchronous execution of multi-query dataframes that contain binding variables.
- Added support for renaming multiple columns in `DataFrame.rename`.
- Added support for Geometry datatypes.
- Added support for `params` in `session.sql()` in stored procedures.
- Added support for user-defined aggregate functions (UDAFs). This feature is currently in private preview.
- Added support for vectorized UDTFs (user-defined table functions). This feature is currently in public preview.
- Added support for Snowflake Timestamp variants (i.e., `TIMESTAMP_NTZ`, `TIMESTAMP_LTZ`, `TIMESTAMP_TZ`)
  - Added `TimestampTimezone` as an argument in `TimestampType` constructor.
  - Added type hints `NTZ`, `LTZ`, `TZ` and `Timestamp` to annotate functions when registering UDFs.

### Improvements

- Removed redundant dependency `typing-extensions`.
- `DataFrame.cache_result` now creates temp table fully qualified names under current database and current schema.

### Bug Fixes

- Fixed a bug where type check happens on pandas before it is imported.
- Fixed a bug when creating a UDF from `numpy.ufunc`.
- Fixed a bug where `DataFrame.union` was not generating the correct `Selectable.schema_query` when SQL simplifier is enabled.

### Behavior Changes

- `DataFrameWriter.save_as_table` now respects the `nullable` field of the schema provided by the user or the inferred schema based on data from user input.

### Dependency updates

- Updated ``snowflake-connector-python`` to 3.0.4.

## 1.5.1 (2023-06-20)

### New Features

- Added support for the Python 3.10 runtime environment.

## 1.5.0 (2023-06-09)

### Behavior Changes

- Aggregation results, from functions such as `DataFrame.agg` and `DataFrame.describe`, no longer strip away non-printing characters from column names.

### New Features

- Added support for the Python 3.9 runtime environment.
- Added support for new functions in `snowflake.snowpark.functions`:
  - `array_generate_range`
  - `array_unique_agg`
  - `collect_set`
  - `sequence`
- Added support for registering and calling stored procedures with `TABLE` return type.
- Added support for parameter `length` in `StringType()` to specify the maximum number of characters that can be stored by the column.
- Added the alias `functions.element_at()` for `functions.get()`.
- Added the alias `Column.contains` for `functions.contains`.
- Added experimental feature `DataFrame.alias`.
- Added support for querying metadata columns from stage when creating `DataFrame` using `DataFrameReader`.
- Added support for `StructType.add` to append more fields to existing `StructType` objects.
- Added support for parameter `execute_as` in `StoredProcedureRegistration.register_from_file()` to specify stored procedure caller rights.

### Bug Fixes

- Fixed a bug where the `Dataframe.join_table_function` did not run all of the necessary queries to set up the join table function when SQL simplifier was enabled.
- Fixed type hint declaration for custom types - `ColumnOrName`, `ColumnOrLiteralStr`, `ColumnOrSqlExpr`, `LiteralType` and `ColumnOrLiteral` that were breaking `mypy` checks.
- Fixed a bug where `DataFrameWriter.save_as_table` and `DataFrame.copy_into_table` failed to parse fully qualified table names.

## 1.4.0 (2023-04-24)

### New Features

- Added support for `session.getOrCreate`.
- Added support for alias `Column.getField`.
- Added support for new functions in `snowflake.snowpark.functions`:
  - `date_add` and `date_sub` to make add and subtract operations easier.
  - `daydiff`
  - `explode`
  - `array_distinct`.
  - `regexp_extract`.
  - `struct`.
  - `format_number`.
  - `bround`.
  - `substring_index`
- Added parameter `skip_upload_on_content_match` when creating UDFs, UDTFs and stored procedures using `register_from_file` to skip uploading files to a stage if the same version of the files are already on the stage.
- Added support for `DataFrameWriter.save_as_table` method to take table names that contain dots.
- Flattened generated SQL when `DataFrame.filter()` or `DataFrame.order_by()` is followed by a projection statement (e.g. `DataFrame.select()`, `DataFrame.with_column()`).
- Added support for creating dynamic tables _(in private preview)_ using `Dataframe.create_or_replace_dynamic_table`.
- Added an optional argument `params` in `session.sql()` to support binding variables. Note that this is not supported in stored procedures yet.

### Bug Fixes

- Fixed a bug in `strtok_to_array` where an exception was thrown when a delimiter was passed in.
- Fixed a bug in `session.add_import` where the module had the same namespace as other dependencies.

## 1.3.0 (2023-03-28)

### New Features

- Added support for `delimiters` parameter in `functions.initcap()`.
- Added support for `functions.hash()` to accept a variable number of input expressions.
- Added API `Session.RuntimeConfig` for getting/setting/checking the mutability of any runtime configuration.
- Added support managing case sensitivity in `Row` results from `DataFrame.collect` using `case_sensitive` parameter.
- Added API `Session.conf` for getting, setting or checking the mutability of any runtime configuration.
- Added support for managing case sensitivity in `Row` results from `DataFrame.collect` using `case_sensitive` parameter.
- Added indexer support for `snowflake.snowpark.types.StructType`.
- Added a keyword argument `log_on_exception` to `Dataframe.collect` and `Dataframe.collect_no_wait` to optionally disable error logging for SQL exceptions.

### Bug Fixes

- Fixed a bug where a DataFrame set operation(`DataFrame.substract`, `DataFrame.union`, etc.) being called after another DataFrame set operation and `DataFrame.select` or `DataFrame.with_column` throws an exception.
- Fixed a bug where chained sort statements are overwritten by the SQL simplifier.

### Improvements

- Simplified JOIN queries to use constant subquery aliases (`SNOWPARK_LEFT`, `SNOWPARK_RIGHT`) by default. Users can disable this at runtime with `session.conf.set('use_constant_subquery_alias', False)` to use randomly generated alias names instead.
- Allowed specifying statement parameters in `session.call()`.
- Enabled the uploading of large pandas DataFrames in stored procedures by defaulting to a chunk size of 100,000 rows.

## 1.2.0 (2023-03-02)

### New Features

- Added support for displaying source code as comments in the generated scripts when registering stored procedures. This
  is enabled by default, turn off by specifying `source_code_display=False` at registration.
- Added a parameter `if_not_exists` when creating a UDF, UDTF or Stored Procedure from Snowpark Python to ignore creating the specified function or procedure if it already exists.
- Accept integers when calling `snowflake.snowpark.functions.get` to extract value from array.
- Added `functions.reverse` in functions to open access to Snowflake built-in function
  [reverse](https://docs.snowflake.com/en/sql-reference/functions/reverse).
- Added parameter `require_scoped_url` in snowflake.snowflake.files.SnowflakeFile.open() `(in Private Preview)` to replace `is_owner_file` is marked for deprecation.

### Bug Fixes

- Fixed a bug that overwrote `paramstyle` to `qmark` when creating a Snowpark session.
- Fixed a bug where `df.join(..., how="cross")` fails with `SnowparkJoinException: (1112): Unsupported using join type 'Cross'`.
- Fixed a bug where querying a `DataFrame` column created from chained function calls used a wrong column name.

## 1.1.0 (2023-01-26)

### New Features:

- Added `asc`, `asc_nulls_first`, `asc_nulls_last`, `desc`, `desc_nulls_first`, `desc_nulls_last`, `date_part` and `unix_timestamp` in functions.
- Added the property `DataFrame.dtypes` to return a list of column name and data type pairs.
- Added the following aliases:
  - `functions.expr()` for `functions.sql_expr()`.
  - `functions.date_format()` for `functions.to_date()`.
  - `functions.monotonically_increasing_id()` for `functions.seq8()`
  - `functions.from_unixtime()` for `functions.to_timestamp()`

### Bug Fixes:

- Fixed a bug in SQL simplifier that didn’t handle Column alias and join well in some cases. See https://github.com/snowflakedb/snowpark-python/issues/658 for details.
- Fixed a bug in SQL simplifier that generated wrong column names for function calls, NaN and INF.

### Improvements

- The session parameter `PYTHON_SNOWPARK_USE_SQL_SIMPLIFIER` is `True` after Snowflake 7.3 was released. In snowpark-python, `session.sql_simplifier_enabled` reads the value of `PYTHON_SNOWPARK_USE_SQL_SIMPLIFIER` by default, meaning that the SQL simplfier is enabled by default after the Snowflake 7.3 release. To turn this off, set `PYTHON_SNOWPARK_USE_SQL_SIMPLIFIER` in Snowflake to `False` or run `session.sql_simplifier_enabled = False` from Snowpark. It is recommended to use the SQL simplifier because it helps to generate more concise SQL.

## 1.0.0 (2022-11-01)

### New Features

- Added `Session.generator()` to create a new `DataFrame` using the Generator table function.
- Added a parameter `secure` to the functions that create a secure UDF or UDTF.

## 0.12.0 (2022-10-14)

### New Features

- Added new APIs for async job:
  - `Session.create_async_job()` to create an `AsyncJob` instance from a query id.
  - `AsyncJob.result()` now accepts argument `result_type` to return the results in different formats.
  - `AsyncJob.to_df()` returns a `DataFrame` built from the result of this asynchronous job.
  - `AsyncJob.query()` returns the SQL text of the executed query.
- `DataFrame.agg()` and `RelationalGroupedDataFrame.agg()` now accept variable-length arguments.
- Added parameters `lsuffix` and `rsuffix` to `DataFram.join()` and `DataFrame.cross_join()` to conveniently rename overlapping columns.
- Added `Table.drop_table()` so you can drop the temp table after `DataFrame.cache_result()`. `Table` is also a context manager so you can use the `with` statement to drop the cache temp table after use.
- Added `Session.use_secondary_roles()`.
- Added functions `first_value()` and `last_value()`. (contributed by @chasleslr)
- Added `on` as an alias for `using_columns` and `how` as an alias for `join_type` in `DataFrame.join()`.

### Bug Fixes

- Fixed a bug in `Session.create_dataframe()` that raised an error when `schema` names had special characters.
- Fixed a bug in which options set in `Session.read.option()` were not passed to `DataFrame.copy_into_table()` as default values.
- Fixed a bug in which `DataFrame.copy_into_table()` raises an error when a copy option has single quotes in the value.

## 0.11.0 (2022-09-28)

### Behavior Changes

- `Session.add_packages()` now raises `ValueError` when the version of a package cannot be found in Snowflake Anaconda channel. Previously, `Session.add_packages()` succeeded, and a `SnowparkSQLException` exception was raised later in the UDF/SP registration step.

### New Features:

- Added method `FileOperation.get_stream()` to support downloading stage files as stream.
- Added support in `functions.ntiles()` to accept int argument.
- Added the following aliases:
  - `functions.call_function()` for `functions.call_builtin()`.
  - `functions.function()` for `functions.builtin()`.
  - `DataFrame.order_by()` for `DataFrame.sort()`
  - `DataFrame.orderBy()` for `DataFrame.sort()`
- Improved `DataFrame.cache_result()` to return a more accurate `Table` class instead of a `DataFrame` class.
- Added support to allow `session` as the first argument when calling `StoredProcedure`.

### Improvements

- Improved nested query generation by flattening queries when applicable.
  - This improvement could be enabled by setting `Session.sql_simplifier_enabled = True`.
  - `DataFrame.select()`, `DataFrame.with_column()`, `DataFrame.drop()` and other select-related APIs have more flattened SQLs.
  - `DataFrame.union()`, `DataFrame.union_all()`, `DataFrame.except_()`, `DataFrame.intersect()`, `DataFrame.union_by_name()` have flattened SQLs generated when multiple set operators are chained.
- Improved type annotations for async job APIs.

### Bug Fixes

- Fixed a bug in which `Table.update()`, `Table.delete()`, `Table.merge()` try to reference a temp table that does not exist.

## 0.10.0 (2022-09-16)

### New Features:

- Added experimental APIs for evaluating Snowpark dataframes with asynchronous queries:
  - Added keyword argument `block` to the following action APIs on Snowpark dataframes (which execute queries) to allow asynchronous evaluations:
    - `DataFrame.collect()`, `DataFrame.to_local_iterator()`, `DataFrame.to_pandas()`, `DataFrame.to_pandas_batches()`, `DataFrame.count()`, `DataFrame.first()`.
    - `DataFrameWriter.save_as_table()`, `DataFrameWriter.copy_into_location()`.
    - `Table.delete()`, `Table.update()`, `Table.merge()`.
  - Added method `DataFrame.collect_nowait()` to allow asynchronous evaluations.
  - Added class `AsyncJob` to retrieve results from asynchronously executed queries and check their status.
- Added support for `table_type` in `Session.write_pandas()`. You can now choose from these `table_type` options: `"temporary"`, `"temp"`, and `"transient"`.
- Added support for using Python structured data (`list`, `tuple` and `dict`) as literal values in Snowpark.
- Added keyword argument `execute_as` to `functions.sproc()` and `session.sproc.register()` to allow registering a stored procedure as a caller or owner.
- Added support for specifying a pre-configured file format when reading files from a stage in Snowflake.

### Improvements:

- Added support for displaying details of a Snowpark session.

### Bug Fixes:

- Fixed a bug in which `DataFrame.copy_into_table()` and `DataFrameWriter.save_as_table()` mistakenly created a new table if the table name is fully qualified, and the table already exists.

### Deprecations:

- Deprecated keyword argument `create_temp_table` in `Session.write_pandas()`.
- Deprecated invoking UDFs using arguments wrapped in a Python list or tuple. You can use variable-length arguments without a list or tuple.

### Dependency updates

- Updated ``snowflake-connector-python`` to 2.7.12.

## 0.9.0 (2022-08-30)

### New Features:

- Added support for displaying source code as comments in the generated scripts when registering UDFs.
  This feature is turned on by default. To turn it off, pass the new keyword argument `source_code_display` as `False` when calling `register()` or `@udf()`.
- Added support for calling table functions from `DataFrame.select()`, `DataFrame.with_column()` and `DataFrame.with_columns()` which now take parameters of type `table_function.TableFunctionCall` for columns.
- Added keyword argument `overwrite` to `session.write_pandas()` to allow overwriting contents of a Snowflake table with that of a Pandas DataFrame.
- Added keyword argument `column_order` to `df.write.save_as_table()` to specify the matching rules when inserting data into table in append mode.
- Added method `FileOperation.put_stream()` to upload local files to a stage via file stream.
- Added methods `TableFunctionCall.alias()` and `TableFunctionCall.as_()` to allow aliasing the names of columns that come from the output of table function joins.
- Added function `get_active_session()` in module `snowflake.snowpark.context` to get the current active Snowpark session.

### Bug Fixes:

- Fixed a bug in which batch insert should not raise an error when `statement_params` is not passed to the function.
- Fixed a bug in which column names should be quoted when `session.create_dataframe()` is called with dicts and a given schema.
- Fixed a bug in which creation of table should be skipped if the table already exists and is in append mode when calling `df.write.save_as_table()`.
- Fixed a bug in which third-party packages with underscores cannot be added when registering UDFs.

### Improvements:

- Improved function `function.uniform()` to infer the types of inputs `max_` and `min_` and cast the limits to `IntegerType` or `FloatType` correspondingly.

## 0.8.0 (2022-07-22)

### New Features:

- Added keyword only argument `statement_params` to the following methods to allow for specifying statement level parameters:
  - `collect`, `to_local_iterator`, `to_pandas`, `to_pandas_batches`,
    `count`, `copy_into_table`, `show`, `create_or_replace_view`, `create_or_replace_temp_view`, `first`, `cache_result`
    and `random_split` on class `snowflake.snowpark.Dateframe`.
  - `update`, `delete` and `merge` on class `snowflake.snowpark.Table`.
  - `save_as_table` and `copy_into_location` on class `snowflake.snowpark.DataFrameWriter`.
  - `approx_quantile`, `statement_params`, `cov` and `crosstab` on class `snowflake.snowpark.DataFrameStatFunctions`.
  - `register` and `register_from_file` on class `snowflake.snowpark.udf.UDFRegistration`.
  - `register` and `register_from_file` on class `snowflake.snowpark.udtf.UDTFRegistration`.
  - `register` and `register_from_file` on class `snowflake.snowpark.stored_procedure.StoredProcedureRegistration`.
  - `udf`, `udtf` and `sproc` in `snowflake.snowpark.functions`.
- Added support for `Column` as an input argument to `session.call()`.
- Added support for `table_type` in `df.write.save_as_table()`. You can now choose from these `table_type` options: `"temporary"`, `"temp"`, and `"transient"`.

### Improvements:

- Added validation of object name in `session.use_*` methods.
- Updated the query tag in SQL to escape it when it has special characters.
- Added a check to see if Anaconda terms are acknowledged when adding missing packages.

### Bug Fixes:

- Fixed the limited length of the string column in `session.create_dataframe()`.
- Fixed a bug in which `session.create_dataframe()` mistakenly converted 0 and `False` to `None` when the input data was only a list.
- Fixed a bug in which calling `session.create_dataframe()` using a large local dataset sometimes created a temp table twice.
- Aligned the definition of `function.trim()` with the SQL function definition.
- Fixed an issue where snowpark-python would hang when using the Python system-defined (built-in function) `sum` vs. the Snowpark `function.sum()`.

### Deprecations:

- Deprecated keyword argument `create_temp_table` in `df.write.save_as_table()`.

## 0.7.0 (2022-05-25)

### New Features:

- Added support for user-defined table functions (UDTFs).
  - Use function `snowflake.snowpark.functions.udtf()` to register a UDTF, or use it as a decorator to register the UDTF.
    - You can also use `Session.udtf.register()` to register a UDTF.
  - Use `Session.udtf.register_from_file()` to register a UDTF from a Python file.
- Updated APIs to query a table function, including both Snowflake built-in table functions and UDTFs.
  - Use function `snowflake.snowpark.functions.table_function()` to create a callable representing a table function and use it to call the table function in a query.
  - Alternatively, use function `snowflake.snowpark.functions.call_table_function()` to call a table function.
  - Added support for `over` clause that specifies `partition by` and `order by` when lateral joining a table function.
  - Updated `Session.table_function()` and `DataFrame.join_table_function()` to accept `TableFunctionCall` instances.

### Breaking Changes:

- When creating a function with `functions.udf()` and `functions.sproc()`, you can now specify an empty list for the `imports` or `packages` argument to indicate that no import or package is used for this UDF or stored procedure. Previously, specifying an empty list meant that the function would use session-level imports or packages.
- Improved the `__repr__` implementation of data types in `types.py`. The unused `type_name` property has been removed.
- Added a Snowpark-specific exception class for SQL errors. This replaces the previous `ProgrammingError` from the Python connector.

### Improvements:

- Added a lock to a UDF or UDTF when it is called for the first time per thread.
- Improved the error message for pickling errors that occurred during UDF creation.
- Included the query ID when logging the failed query.

### Bug Fixes:

- Fixed a bug in which non-integral data (such as timestamps) was occasionally converted to integer when calling `DataFrame.to_pandas()`.
- Fixed a bug in which `DataFrameReader.parquet()` failed to read a parquet file when its column contained spaces.
- Fixed a bug in which `DataFrame.copy_into_table()` failed when the dataframe is created by reading a file with inferred schemas.

### Deprecations

`Session.flatten()` and `DataFrame.flatten()`.

### Dependency Updates:

- Restricted the version of `cloudpickle` <= `2.0.0`.

## 0.6.0 (2022-04-27)

### New Features:

- Added support for vectorized UDFs with the input as a Pandas DataFrame or Pandas Series and the output as a Pandas Series. This improves the performance of UDFs in Snowpark.
- Added support for inferring the schema of a DataFrame by default when it is created by reading a Parquet, Avro, or ORC file in the stage.
- Added functions `current_session()`, `current_statement()`, `current_user()`, `current_version()`, `current_warehouse()`, `date_from_parts()`, `date_trunc()`, `dayname()`, `dayofmonth()`, `dayofweek()`, `dayofyear()`, `grouping()`, `grouping_id()`, `hour()`, `last_day()`, `minute()`, `next_day()`, `previous_day()`, `second()`, `month()`, `monthname()`, `quarter()`, `year()`, `current_database()`, `current_role()`, `current_schema()`, `current_schemas()`, `current_region()`, `current_avaliable_roles()`, `add_months()`, `any_value()`, `bitnot()`, `bitshiftleft()`, `bitshiftright()`, `convert_timezone()`, `uniform()`, `strtok_to_array()`, `sysdate()`, `time_from_parts()`,  `timestamp_from_parts()`, `timestamp_ltz_from_parts()`, `timestamp_ntz_from_parts()`, `timestamp_tz_from_parts()`, `weekofyear()`, `percentile_cont()` to `snowflake.snowflake.functions`.

### Breaking Changes:

- Expired deprecations:
  - Removed the following APIs that were deprecated in 0.4.0: `DataFrame.groupByGroupingSets()`, `DataFrame.naturalJoin()`, `DataFrame.joinTableFunction`, `DataFrame.withColumns()`, `Session.getImports()`, `Session.addImport()`, `Session.removeImport()`, `Session.clearImports()`, `Session.getSessionStage()`, `Session.getDefaultDatabase()`, `Session.getDefaultSchema()`, `Session.getCurrentDatabase()`, `Session.getCurrentSchema()`, `Session.getFullyQualifiedCurrentSchema()`.

### Improvements:

- Added support for creating an empty `DataFrame` with a specific schema using the `Session.create_dataframe()` method.
- Changed the logging level from `INFO` to `DEBUG` for several logs (e.g., the executed query) when evaluating a dataframe.
- Improved the error message when failing to create a UDF due to pickle errors.

### Bug Fixes:

- Removed pandas hard dependencies in the `Session.create_dataframe()` method.

### Dependency Updates:

- Added `typing-extension` as a new dependency with the version >= `4.1.0`.

## 0.5.0 (2022-03-22)

### New Features

- Added stored procedures API.
  - Added `Session.sproc` property and `sproc()` to `snowflake.snowpark.functions`, so you can register stored procedures.
  - Added `Session.call` to call stored procedures by name.
- Added `UDFRegistration.register_from_file()` to allow registering UDFs from Python source files or zip files directly.
- Added `UDFRegistration.describe()` to describe a UDF.
- Added `DataFrame.random_split()` to provide a way to randomly split a dataframe.
- Added functions `md5()`, `sha1()`, `sha2()`, `ascii()`, `initcap()`, `length()`, `lower()`, `lpad()`, `ltrim()`, `rpad()`, `rtrim()`, `repeat()`, `soundex()`, `regexp_count()`, `replace()`, `charindex()`, `collate()`, `collation()`, `insert()`, `left()`, `right()`, `endswith()` to `snowflake.snowpark.functions`.
- Allowed `call_udf()` to accept literal values.
- Provided a `distinct` keyword in `array_agg()`.

### Bug Fixes:

- Fixed an issue that caused `DataFrame.to_pandas()` to have a string column if `Column.cast(IntegerType())` was used.
- Fixed a bug in `DataFrame.describe()` when there is more than one string column.

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
- Added functions `listagg()`, `mode()`, `div0()`, `acos()`, `asin()`, `atan()`, `atan2()`, `cos()`, `cosh()`, `sin()`, `sinh()`, `tan()`, `tanh()`, `degrees()`, `radians()`, `round()`, `trunc()`, and `factorial()` to `snowflake.snowflake.functions`.
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
- Changed `DataFrame.describe()` so that non-numeric and non-string columns are ignored instead of raising an exception.

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
