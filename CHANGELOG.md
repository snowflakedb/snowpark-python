# Release History

## 1.36.0 (2025-08-05)

### Snowpark Python API Updates

#### New Features

- `Session.create_dataframe` now accepts keyword arguments that are forwarded to the internal call to `Session.write_pandas` or `Session.write_arrow` when creating a DataFrame from a pandas DataFrame or a pyarrow Table.
- Added new APIs for `AsyncJob`:
  - `AsyncJob.is_failed()` returns a `bool` indicating if a job has failed. Can be used in combination with `AsyncJob.is_done()` to determine if a job is finished and errored.
  - `AsyncJob.status()` returns a string representing the current query status (e.g., "RUNNING", "SUCCESS", "FAILED_WITH_ERROR") for detailed monitoring without calling `result()`.
- Added a dataframe profiler. To use, you can call get_execution_profile() on your desired dataframe. This profiler reports the queries executed to evaluate a dataframe, and statistics about each of the query operators. Currently an experimental feature
- Added support for the following functions in `functions.py`:
  - `ai_sentiment`
- Updated the interface for experimental feature `context.configure_development_features`. All development features are disabled by default unless explicitly enabled by the user.

### Snowpark pandas API Updates

#### New Features

#### Improvements
- Hybrid execution row estimate improvements and a reduction of eager calls.
- Add a new configuration variable to control transfer costs out of Snowflake when using hybrid execution.
- Added support for creating permanent and immutable UDFs/UDTFs with `DataFrame/Series/GroupBy.apply`, `map`, and `transform` by passing the `snowflake_udf_params` keyword argument. See documentation for details.

#### Bug Fixes

- Fixed an issue where Snowpark pandas plugin would unconditionally disable `AutoSwitchBackend` even when users had explicitly configured it via environment variables or programmatically.

## 1.35.0 (2025-07-24)

### Snowpark Python API Updates

#### New Features

- Added support for the following functions in `functions.py`:
  - `ai_embed`
  - `try_parse_json`

#### Bug Fixes

- Fixed a bug in `DataFrameReader.dbapi` (PrPr) that `dbapi` fail in python stored procedure with process exit with code 1.
- Fixed a bug in `DataFrameReader.dbapi` (PrPr) that `custom_schema` accept illegal schema.
- Fixed a bug in `DataFrameReader.dbapi` (PrPr) that `custom_schema` does not work when connecting to Postgres and Mysql.
- Fixed a bug in schema inference that would cause it to fail for external stages.

#### Improvements

- Improved `query` parameter in `DataFrameReader.dbapi` (PrPr) so that parentheses are not needed around the query.
- Improved error experience in `DataFrameReader.dbapi` (PrPr) when exception happen during inferring schema of target data source.


### Snowpark Local Testing Updates

#### New Features

- Added local testing support for reading files with `SnowflakeFile` using local file paths, the Snow URL semantic (snow://...), local testing framework stages, and Snowflake stages (@stage/file_path).

### Snowpark pandas API Updates

#### New Features

- Added support for `DataFrame.boxplot`.

#### Improvements

- Reduced the number of UDFs/UDTFs created by repeated calls to `apply` or `map` with the same arguments on Snowpark pandas objects.
- Added an example for reading a file from a stage in the docstring for `pd.read_excel`.

#### Bug Fixes

- Added an upper bound to the row estimation when the cartesian product from an align or join results in a very large number. This mitigates a performance regression.
- Fix a `pd.read_excel` bug when reading files inside stage inner directory.

## 1.34.0 (2025-07-15)

### Snowpark Python API Updates

#### New Features

- Added a new option `TRY_CAST` to `DataFrameReader`. When `TRY_CAST` is True columns are wrapped in a `TRY_CAST` statement rather than a hard cast when loading data.
- Added a new option `USE_RELAXED_TYPES` to the `INFER_SCHEMA_OPTIONS` of `DataFrameReader`. When set to True this option casts all strings to max length strings and all numeric types to `DoubleType`.
- Added debuggability improvements to eagerly validate dataframe schema metadata. Enable it using `snowflake.snowpark.context.configure_development_features()`.
- Added a new function `snowflake.snowpark.dataframe.map_in_pandas` that allows users map a function across a dataframe. The mapping function takes an iterator of pandas dataframes as input and provides one as output.
- Added a ttl cache to describe queries. Repeated queries in a 15 second interval will use the cached value rather than requery Snowflake.
- Added a parameter `fetch_with_process` to `DataFrameReader.dbapi` (PrPr) to enable multiprocessing for parallel data fetching in local ingestion. By default, local ingestion uses multithreading. Multiprocessing may improve performance for CPU-bound tasks like Parquet file generation.
- Added a new function `snowflake.snowpark.functions.model` that allows users to call methods of a model.

#### Improvements

- Added support for row validation using XSD schema using `rowValidationXSDPath` option when reading XML files with a row tag using `rowTag` option.
- Improved SQL generation for `session.table().sample()` to generate a flat SQL statement.
- Added support for complex column expression as input for `functions.explode`.
- Added debuggability improvements to show which Python lines an SQL compilation error corresponds to. Enable it using `snowflake.snowpark.context.configure_development_features()`. This feature also depends on AST collection to be enabled in the session which can be done using `session.ast_enabled = True`.
- Set enforce_ordering=True when calling `to_snowpark_pandas()` from a snowpark dataframe containing DML/DDL queries instead of throwing a NotImplementedError.

#### Bug Fixes

- Fixed a bug caused by redundant validation when creating an iceberg table.
- Fixed a bug in `DataFrameReader.dbapi` (PrPr) where closing the cursor or connection could unexpectedly raise an error and terminate the program.
- Fixed ambiguous column errors when using table functions in `DataFrame.select()` that have output columns matching the input DataFrame's columns. This improvement works when dataframe columns are provided as `Column` objects.
- Fixed a bug where having a NULL in a column with DecimalTypes would cast the column to FloatTypes instead and lead to precision loss.

### Snowpark Local Testing Updates

#### Bug Fixes

- Fixed a bug when processing windowed functions that lead to incorrect indexing in results.
- When a scalar numeric is passed to fillna we will ignore non-numeric columns instead of producing an error.

### Snowpark pandas API Updates

#### New Features

- Added support for `DataFrame.to_excel` and `Series.to_excel`.
- Added support for `pd.read_feather`, `pd.read_orc`, and `pd.read_stata`.
- Added support for `pd.explain_switch()` to return debugging information on hybrid execution decisions.
- Support `pd.read_snowflake` when the global modin backend is `Pandas`.
- Added support for `pd.to_dynamic_table`, `pd.to_iceberg`, and `pd.to_view`.

#### Improvements

- Added modin telemetry on API calls and hybrid engine switches.
- Show more helpful error messages to Snowflake Notebook users when the `modin` or `pandas` version does not match our requirements.
- Added a data type guard to the cost functions for hybrid execution mode (PrPr) which checks for data type compatibility.
- Added automatic switching to the pandas backend in hybrid execution mode (PrPr) for many methods that are not directly implemented in Snowpark pandas.
- Set the 'type' and other standard fields for Snowpark pandas telemetry.

#### Dependency Updates

- Added tqdm and ipywidgets as dependencies so that progress bars appear when switching between modin backends.
- Updated the supported `modin` versions to >=0.33.0 and <0.35.0 (was previously >= 0.32.0 and <0.34.0).

#### Bug Fixes

- Fixed a bug in hybrid execution mode (PrPr) where certain Series operations would raise `TypeError: numpy.ndarray object is not callable`.
- Fixed a bug in hybrid execution mode (PrPr) where calling numpy operations like `np.where` on modin objects with the Pandas backend would raise an `AttributeError`. This fix requires `modin` version 0.34.0 or newer.
- Fixed issue in `df.melt` where the resulting values have an additional suffix applied.

## 1.33.0 (2025-06-19)

### Snowpark Python API Updates

#### New Features

- Added support for MySQL in `DataFrameWriter.dbapi` (PrPr) for both Parquet and UDTF-based ingestion.
- Added support for PostgreSQL in `DataFrameReader.dbapi` (PrPr) for both Parquet and UDTF-based ingestion.
- Added support for Databricks in `DataFrameWriter.dbapi` (PrPr) for UDTF-based ingestion.
- Added support to `DataFrameReader` to enable use of `PATTERN` when reading files with `INFER_SCHEMA` enabled.
- Added support for the following AI-powered functions in `functions.py`:
  - `ai_complete`
  - `ai_similarity`
  - `ai_summarize_agg` (originally `summarize_agg`)
  - different config options for `ai_classify`
- Added support for more options when reading XML files with a row tag using `rowTag` option:
  - Added support for removing namespace prefixes from col names using `ignoreNamespace` option.
  - Added support for specifying the prefix for the attribute column in the result table using `attributePrefix` option.
  - Added support for excluding attributes from the XML element using `excludeAttributes` option.
  - Added support for specifying the column name for the value when there are attributes in an element that has no child elements using `valueTag` option.
  - Added support for specifying the value to treat as a ``null`` value using `nullValue` option.
  - Added support for specifying the character encoding of the XML file using `charset` option.
  - Added support for ignoring surrounding whitespace in the XML element using `ignoreSurroundingWhitespace` option.
- Added support for parameter `return_dataframe` in `Session.call`, which can be used to set the return type of the functions to a `DataFrame` object.
- Added a new argument to `Dataframe.describe` called `strings_include_math_stats` that triggers `stddev` and `mean` to be calculated for String columns.
- Added support for retrieving `Edge.properties` when retrieving lineage from `DGQL` in `DataFrame.lineage.trace`.
- Added a parameter `table_exists` to `DataFrameWriter.save_as_table` that allows specifying if a table already exists. This allows skipping a table lookup that can be expensive.

#### Bug Fixes

- Fixed a bug in `DataFrameReader.dbapi` (PrPr) where the `create_connection` defined as local function was incompatible with multiprocessing.
- Fixed a bug in `DataFrameReader.dbapi` (PrPr) where databricks `TIMESTAMP` type was converted to Snowflake `TIMESTAMP_NTZ` type which should be `TIMESTAMP_LTZ` type.
- Fixed a bug in `DataFrameReader.json` where repeated reads with the same reader object would create incorrectly quoted columns.
- Fixed a bug in `DataFrame.to_pandas()` that would drop column names when converting a dataframe that did not originate from a select statement.
- Fixed a bug that `DataFrame.create_or_replace_dynamic_table` raises error when the dataframe contains a UDTF and `SELECT *` in UDTF not being parsed correctly.
- Fixed a bug where casted columns could not be used in the values-clause of in functions.

#### Improvements

- Improved the error message for `Session.write_pandas()` and `Session.create_dataframe()` when the input pandas DataFrame does not have a column.
- Improved `DataFrame.select` when the arguments contain a table function with output columns that collide with columns of current dataframe. With the improvement, if user provides non-colliding columns in `df.select("col1", "col2", table_func(...))` as string arguments, then the query generated by snowpark client will not raise ambiguous column error.
- Improved `DataFrameReader.dbapi` (PrPr) to use in-memory Parquet-based ingestion for better performance and security.
- Improved `DataFrameReader.dbapi` (PrPr) to use `MATCH_BY_COLUMN_NAME=CASE_SENSITIVE` in copy into table operation.

### Snowpark Local Testing Updates

#### New Features

- Added support for snow urls (snow://) in local file testing.

#### Bug Fixes

- Fixed a bug in `Column.isin` that would cause incorrect filtering on joined or previously filtered data.
- Fixed a bug in `snowflake.snowpark.functions.concat_ws` that would cause results to have an incorrect index.

### Snowpark pandas API Updates

#### Dependency Updates

- Updated `modin` dependency constraint from 0.32.0 to >=0.32.0, <0.34.0. The latest version tested with Snowpark pandas is `modin` 0.33.1.

#### New Features

- Added support for **Hybrid Execution (PrPr)**. By running `from modin.config import AutoSwitchBackend; AutoSwitchBackend.enable()`, Snowpark pandas will automatically choose whether to run certain pandas operations locally or on Snowflake. This feature is disabled by default.

#### Improvements

- Set the default value of the `index` parameter to `False` for `DataFrame.to_view`, `Series.to_view`, `DataFrame.to_dynamic_table`, and `Series.to_dynamic_table`.
- Added `iceberg_version` option to table creation functions.
- Reduced query count for many operations, including `insert`, `repr`, and `groupby`, that previously issued a query to retrieve the input data's size.

#### Bug Fixes

- Fixed a bug in `Series.where` when the `other` parameter is an unnamed `Series`.


## 1.32.0 (2025-05-15)

### Snowpark Python API Updates

#### Improvements

- Invoking snowflake system procedures does not invoke an additional `describe procedure` call to check the return type of the procedure.
- Added support for `Session.create_dataframe()` with the stage URL and FILE data type.
- Added support for different modes for dealing with corrupt XML records when reading an XML file using `session.read.option('mode', <mode>), option('rowTag', <tag_name>).xml(<stage_file_path>)`. Currently `PERMISSIVE`, `DROPMALFORMED` and `FAILFAST` are supported.
- Improved the error message of the XML reader when the specified row tag is not found in the file.
- Improved query generation for `Dataframe.drop` to use `SELECT * EXCLUDE ()` to exclude the dropped columns. To enable this feature, set `session.conf.set("use_simplified_query_generation", True)`.
- Added support for `VariantType` to `StructType.from_json`

#### Bug Fixes

- Fixed a bug in `DataFrameWriter.dbapi` (PrPr) that unicode or double-quoted column name in external database causes error because not quoted correctly.
- Fixed a bug where named fields in nested OBJECT data could cause errors when containing spaces.
- Fixed a bug duplicated `native_app_params` parameters in register udaf function.

### Snowpark Local Testing Updates

#### Bug Fixes

- Fixed a bug in `snowflake.snowpark.functions.rank` that would cause sort direction to not be respected.
- Fixed a bug in `snowflake.snowpark.functions.to_timestamp_*` that would cause incorrect results on filtered data.

### Snowpark pandas API Updates

#### New Features

- Added support for dict values in `Series.str.get`, `Series.str.slice`, and `Series.str.__getitem__` (`Series.str[...]`).
- Added support for `DataFrame.to_html`.
- Added support for `DataFrame.to_string` and `Series.to_string`.
- Added support for reading files from S3 buckets using `pd.read_csv`.
- Added `ENFORCE_EXISTING_FILE_FORMAT` option to the `DataFrameReader`, which allows to read a dataframe only based on an existing file format object when used together with `FORMAT_NAME`.

#### Improvements

- Make `iceberg_config` a required parameter for `DataFrame.to_iceberg` and `Series.to_iceberg`.

## 1.31.1 (2025-05-05)

### Snowpark Python API Updates

#### Bug Fixes

- Updated conda build configuration to deprecate Python 3.8 support, preventing installation in incompatible environments.

## 1.31.0 (2025-04-24)

### Snowpark Python API Updates

#### New Features

- Added support for `restricted caller` permission of `execute_as` argument in `StoredProcedure.register()`.
- Added support for non-select statement in `DataFrame.to_pandas()`.
- Added support for `artifact_repository` parameter to `Session.add_packages`, `Session.add_requirements`, `Session.get_packages`, `Session.remove_package`, and `Session.clear_packages`.
- Added support for reading an XML file using a row tag by `session.read.option('rowTag', <tag_name>).xml(<stage_file_path>)` (experimental).
  - Each XML record is extracted as a separate row.
  - Each field within that record becomes a separate column of type VARIANT, which can be further queried using dot notation, e.g., `col(a.b.c)`.
- Added updates to `DataFrameReader.dbapi` (PrPr):
  - Added `fetch_merge_count` parameter for optimizing performance by merging multiple fetched data into a single Parquet file.
  - Added support for Databricks.
  - Added support for ingestion with Snowflake UDTF.
- Added support for the following AI-powered functions in `functions.py` (Private Preview):
  - `prompt`
  - `ai_filter` (added support for `prompt()` function and image files, and changed the second argument name from `expr` to `file`)
  - `ai_classify`

#### Improvements

- Renamed the `relaxed_ordering` param into `enforce_ordering` for `DataFrame.to_snowpark_pandas`. Also the new default values is `enforce_ordering=False` which has the opposite effect of the previous default value, `relaxed_ordering=False`.
- Improved `DataFrameReader.dbapi` (PrPr) reading performance by setting the default `fetch_size` parameter value to 1000.
- Improve the error message for invalid identifier SQL error by suggesting the potentially matching identifiers.
- Reduced the number of describe queries issued when creating a DataFrame from a Snowflake table using `session.table`.
- Improved performance and accuracy of `DataFrameAnalyticsFunctions.time_series_agg()`.

#### Bug Fixes

- Fixed a bug in `DataFrame.group_by().pivot().agg` when the pivot column and aggregate column are the same.
- Fixed a bug in `DataFrameReader.dbapi` (PrPr) where a `TypeError` was raised when `create_connection` returned a connection object of an unsupported driver type.
- Fixed a bug where `df.limit(0)` call would not properly apply.
- Fixed a bug in `DataFrameWriter.save_as_table` that caused reserved names to throw errors when using append mode.

#### Deprecations

- Deprecated support for Python3.8.
- Deprecated argument `sliding_interval` in `DataFrameAnalyticsFunctions.time_series_agg()`.

### Snowpark Local Testing Updates

#### New Features

- Added support for Interval expression to `Window.range_between`.
- Added support for `array_construct` function.

#### Bug Fixes

- Fixed a bug in local testing where transient `__pycache__` directory was unintentionally copied during stored procedure execution via import.
- Fixed a bug in local testing that created incorrect result for `Column.like` calls.
- Fixed a bug in local testing that caused `Column.getItem` and `snowpark.snowflake.functions.get` to raise `IndexError` rather than return null.
- Fixed a bug in local testing where `df.limit(0)` call would not properly apply.
- Fixed a bug in local testing where a `Table.merge` into an empty table would cause an exception.

### Snowpark pandas API Updates

#### Dependency Updates

- Updated `modin` from 0.30.1 to 0.32.0.
- Added support for `numpy` 2.0 and above.

#### New Features

- Added support for `DataFrame.create_or_replace_view` and `Series.create_or_replace_view`.
- Added support for `DataFrame.create_or_replace_dynamic_table` and `Series.create_or_replace_dynamic_table`.
- Added support for `DataFrame.to_view` and `Series.to_view`.
- Added support for `DataFrame.to_dynamic_table` and `Series.to_dynamic_table`.
- Added support for `DataFrame.groupby.resample` for aggregations `max`, `mean`, `median`, `min`, and `sum`.
- Added support for reading stage files using:
  - `pd.read_excel`
  - `pd.read_html`
  - `pd.read_pickle`
  - `pd.read_sas`
  - `pd.read_xml`
- Added support for `DataFrame.to_iceberg` and `Series.to_iceberg`.
- Added support for dict values in `Series.str.len`.

#### Improvements

- Improve performance of `DataFrame.groupby.apply` and `Series.groupby.apply` by avoiding expensive pivot step.
- Added estimate for row count upper bound to `OrderedDataFrame` to enable better engine switching. This could potentially result in increased query counts.
- Renamed the `relaxed_ordering` param into `enforce_ordering` for `pd.read_snowflake`. Also the new default value is `enforce_ordering=False` which has the opposite effect of the previous default value, `relaxed_ordering=False`.

#### Bug Fixes

- Fixed a bug for `pd.read_snowflake` when reading iceberg tables and `enforce_ordering=True`.

## 1.30.0 (2025-03-27)

### Snowpark Python API Updates

#### New Features

- Added Support for relaxed consistency and ordering guarantees in `Dataframe.to_snowpark_pandas` by introducing the new parameter `relaxed_ordering`.
- `DataFrameReader.dbapi` (PrPr) now accepts a list of strings for the session_init_statement parameter, allowing multiple SQL statements to be executed during session initialization.

#### Improvements

- Improved query generation for `Dataframe.stat.sample_by` to generate a single flat query that scales well with large `fractions` dictionary compared to older method of creating a UNION ALL subquery for each key in `fractions`. To enable this feature, set `session.conf.set("use_simplified_query_generation", True)`.
- Improved performance of `DataFrameReader.dbapi` by enable vectorized option when copy parquet file into table.
- Improved query generation for `DataFrame.random_split` in the following ways. They can be enabled by setting `session.conf.set("use_simplified_query_generation", True)`:
  - Removed the need to `cache_result` in the internal implementation of the input dataframe resulting in a pure lazy dataframe operation.
  - The `seed` argument now behaves as expected with repeatable results across multiple calls and sessions.
- `DataFrame.fillna` and `DataFrame.replace` now both support fitting `int` and `float` into `Decimal` columns if `include_decimal` is set to True.
- Added documentation for the following UDF and stored procedure functions in `files.py` as a result of their General Availability.
  - `SnowflakeFile.write`
  - `SnowflakeFile.writelines`
  - `SnowflakeFile.writeable`
- Minor documentation changes for `SnowflakeFile` and `SnowflakeFile.open()`

#### Bug Fixes

- Fixed a bug for the following functions that raised errors `.cast()` is applied to their output
  - `from_json`
  - `size`

### Snowpark Local Testing Updates

#### Bug Fixes

- Fixed a bug in aggregation that caused empty groups to still produce rows.
- Fixed a bug in `Dataframe.except_` that would cause rows to be incorrectly dropped.
- Fixed a bug that caused `to_timestamp` to fail when casting filtered columns.

### Snowpark pandas API Updates

#### New Features

- Added support for list values in `Series.str.__getitem__` (`Series.str[...]`).
- Added support for `pd.Grouper` objects in group by operations. When `freq` is specified, the default values of the `sort`, `closed`, `label`, and `convention` arguments are supported; `origin` is supported when it is `start` or `start_day`.
- Added support for relaxed consistency and ordering guarantees in `pd.read_snowflake` for both named data sources (e.g., tables and views) and query data sources by introducing the new parameter `relaxed_ordering`.

#### Improvements

- Raise a warning whenever `QUOTED_IDENTIFIERS_IGNORE_CASE` is found to be set, ask user to unset it.
- Improved how a missing `index_label` in `DataFrame.to_snowflake` and `Series.to_snowflake` is handled when `index=True`. Instead of raising a `ValueError`, system-defined labels are used for the index columns.
- Improved error message for `groupby or DataFrame or Series.agg` when the function name is not supported.

## 1.29.1 (2025-03-12)

### Snowpark Python API Updates

#### Bug Fixes

- Fixed a bug in `DataFrameReader.dbapi` (PrPr) that prevents usage in stored procedure and snowbooks.

## 1.29.0 (2025-03-05)

### Snowpark Python API Updates

#### New Features

- Added support for the following AI-powered functions in `functions.py` (Private Preview):
  - `ai_filter`
  - `ai_agg`
  - `summarize_agg`
- Added support for the new FILE SQL type support, with the following related functions in `functions.py` (Private Preview):
  - `fl_get_content_type`
  - `fl_get_etag`
  - `fl_get_file_type`
  - `fl_get_last_modified`
  - `fl_get_relative_path`
  - `fl_get_scoped_file_url`
  - `fl_get_size`
  - `fl_get_stage`
  - `fl_get_stage_file_url`
  - `fl_is_audio`
  - `fl_is_compressed`
  - `fl_is_document`
  - `fl_is_image`
  - `fl_is_video`
- Added support for importing third-party packages from PyPi using Artifact Repository (Private Preview):
  - Use keyword arguments `artifact_repository` and `artifact_repository_packages` to specify your artifact repository and packages respectively when registering stored procedures or user defined functions.
  - Supported APIs are:
    - `Session.sproc.register`
    - `Session.udf.register`
    - `Session.udaf.register`
    - `Session.udtf.register`
    - `functions.sproc`
    - `functions.udf`
    - `functions.udaf`
    - `functions.udtf`
    - `functions.pandas_udf`
    - `functions.pandas_udtf`

#### Bug Fixes

- Fixed a bug where creating a Dataframe with large number of values raised `Unsupported feature 'SCOPED_TEMPORARY'.` error if thread-safe session was disabled.
- Fixed a bug where `df.describe` raised internal SQL execution error when the dataframe is created from reading a stage file and CTE optimization is enabled.
- Fixed a bug where `df.order_by(A).select(B).distinct()` would generate invalid SQL when simplified query generation was enabled using `session.conf.set("use_simplified_query_generation", True)`.
- Disabled simplified query generation by default.

#### Improvements

- Improved version validation warnings for `snowflake-snowpark-python` package compatibility when registering stored procedures. Now, warnings are only triggered if the major or minor version does not match, while bugfix version differences no longer generate warnings.
- Bumped cloudpickle dependency to also support `cloudpickle==3.0.0` in addition to previous versions.

### Snowpark Local Testing Updates

#### New Features

- Added support for literal values to `range_between` window function.

### Snowpark pandas API Updates

#### New Features

- Added support for list values in `Series.str.slice`.
- Added support for applying Snowflake Cortex functions `ClassifyText`, `Translate`, and `ExtractAnswer`.
- Added support for `Series.hist`.

#### Improvements

- Improved performance of `DataFrame.groupby.transform` and `Series.groupby.transform` by avoiding expensive pivot step.
- Improve error message for `pd.to_snowflake`, `DataFrame.to_snowflake`, and `Series.to_snowflake` when the table does not exist.
- Improve readability of docstring for the `if_exists` parameter in `pd.to_snowflake`, `DataFrame.to_snowflake`, and `Series.to_snowflake`.
- Improve error message for all pandas functions that use UDFs with Snowpark objects.

#### Bug Fixes

- Fixed a bug in `Series.rename_axis` where an `AttributeError` was being raised.
- Fixed a bug where `pd.get_dummies` didn't ignore NULL/NaN values by default.
- Fixed a bug where repeated calls to `pd.get_dummies` results in 'Duplicated column name error'.
- Fixed a bug in `pd.get_dummies` where passing list of columns generated incorrect column labels in output DataFrame.
- Update `pd.get_dummies` to return bool values instead of int.

## 1.28.0 (2025-02-20)

### Snowpark Python API Updates

#### New Features

- Added support for the following functions in `functions.py`
  - `normal`
  - `randn`
- Added support for `allow_missing_columns` parameter to `Dataframe.union_by_name` and `Dataframe.union_all_by_name`.

#### Improvements

- Improved query generation for `Dataframe.distinct` to generate `SELECT DISTINCT` instead of `SELECT` with `GROUP BY` all columns. To disable this feature, set `session.conf.set("use_simplified_query_generation", False)`.

#### Deprecations

- Deprecated Snowpark Python function `snowflake_cortex_summarize`. Users can install snowflake-ml-python and use the snowflake.cortex.summarize function instead.
- Deprecated Snowpark Python function `snowflake_cortex_sentiment`. Users can install snowflake-ml-python and use the snowflake.cortex.sentiment function instead.

#### Bug Fixes

- Fixed a bug where session-level query tag was overwritten by a stacktrace for dataframes that generate multiple queries. Now, the query tag will only be set to the stacktrace if `session.conf.set("collect_stacktrace_in_query_tag", True)`.
- Fixed a bug in `Session._write_pandas` where it was erroneously passing `use_logical_type` parameter to `Session._write_modin_pandas_helper` when writing a Snowpark pandas object.
- Fixed a bug in options sql generation that could cause multiple values to be formatted incorrectly.
- Fixed a bug in `Session.catalog` where empty strings for database or schema were not handled correctly and were generating erroneous sql statements.

#### Experimental Features

- Added support for writing pyarrow Tables to Snowflake tables.

### Snowpark pandas API Updates

#### New Features

- Added support for applying Snowflake Cortex functions `Summarize` and `Sentiment`.
- Added support for list values in `Series.str.get`.

#### Bug Fixes

- Fixed a bug in `apply` where kwargs were not being correctly passed into the applied function.

### Snowpark Local Testing Updates

#### New Features
- Added support for the following functions
    - `hour`
    - `minute`
- Added support for NULL_IF parameter to csv reader.
- Added support for `date_format`, `datetime_format`, and `timestamp_format` options when loading csvs.

#### Bug Fixes

- Fixed a bug in Dataframe.join that caused columns to have incorrect typing.
- Fixed a bug in when statements that caused incorrect results in the otherwise clause.


## 1.27.0 (2025-02-03)

### Snowpark Python API Updates

#### New Features

- Added support for the following functions in `functions.py`
  - `array_reverse`
  - `divnull`
  - `map_cat`
  - `map_contains_key`
  - `map_keys`
  - `nullifzero`
  - `snowflake_cortex_sentiment`
  - `acosh`
  - `asinh`
  - `atanh`
  - `bit_length`
  - `bitmap_bit_position`
  - `bitmap_bucket_number`
  - `bitmap_construct_agg`
  - `bitshiftright_unsigned`
  - `cbrt`
  - `equal_null`
  - `from_json`
  - `ifnull`
  - `localtimestamp`
  - `max_by`
  - `min_by`
  - `nth_value`
  - `nvl`
  - `octet_length`
  - `position`
  - `regr_avgx`
  - `regr_avgy`
  - `regr_count`
  - `regr_intercept`
  - `regr_r2`
  - `regr_slope`
  - `regr_sxx`
  - `regr_sxy`
  - `regr_syy`
  - `try_to_binary`
  - `base64`
  - `base64_decode_string`
  - `base64_encode`
  - `editdistance`
  - `hex`
  - `hex_encode`
  - `instr`
  - `log1p`
  - `log2`
  - `log10`
  - `percentile_approx`
  - `unbase64`
- Added support for `seed` argument in `DataFrame.stat.sample_by`. Note that it only supports a `Table` object, and will be ignored for a `DataFrame` object.
- Added support for specifying a schema string (including implicit struct syntax) when calling `DataFrame.create_dataframe`.
- Added support for `DataFrameWriter.insert_into/insertInto`. This method also supports local testing mode.
- Added support for `DataFrame.create_temp_view` to create a temporary view. It will fail if the view already exists.
- Added support for multiple columns in the functions `map_cat` and `map_concat`.
- Added an option `keep_column_order` for keeping original column order in `DataFrame.with_column` and `DataFrame.with_columns`.
- Added options to column casts that allow renaming or adding fields in StructType columns.
- Added support for `contains_null` parameter to ArrayType.
- Added support for creating a temporary view via `DataFrame.create_or_replace_temp_view` from a DataFrame created by reading a file from a stage.
- Added support for `value_contains_null` parameter to MapType.
- Added support for using `Column` object in `Column.in_` and `functions.in_`.
- Added `interactive` to telemetry that indicates whether the current environment is an interactive one.
- Allow `session.file.get` in a Native App to read file paths starting with `/` from the current version
- Added support for multiple aggregation functions after `DataFrame.pivot`.

#### Experimental Features

- Added `Catalog` class to manage snowflake objects. It can be accessed via `Session.catalog`.
  - `snowflake.core` is a dependency required for this feature.
- Allow user input schema when reading JSON file on stage.
- Added support for specifying a schema string (including implicit struct syntax) when calling `DataFrame.create_dataframe`.

#### Improvements

- Updated README.md to include instructions on how to verify package signatures using `cosign`.

#### Bug Fixes

- Fixed a bug in local testing mode that caused a column to contain None when it should contain 0.
- Fixed a bug in `StructField.from_json` that prevented TimestampTypes with `tzinfo` from being parsed correctly.
- Fixed a bug in function `date_format` that caused an error when the input column was date type or timestamp type.
- Fixed a bug in dataframe that null value can be inserted in a non-nullable column.
- Fixed a bug in `replace` and `lit` which raised type hint assertion error when passing `Column` expression objects.
- Fixed a bug in `pandas_udf` and `pandas_udtf` where `session` parameter was erroneously ignored.
- Fixed a bug that raised incorrect type conversion error for system function called through `session.call`.

### Snowpark pandas API Updates

#### New Features

- Added support for `Series.str.ljust` and `Series.str.rjust`.
- Added support for `Series.str.center`.
- Added support for `Series.str.pad`.
- Added support for applying Snowpark Python function `snowflake_cortex_sentiment`.
- Added support for `DataFrame.map`.
- Added support for `DataFrame.from_dict` and `DataFrame.from_records`.
- Added support for mixed case field names in struct type columns.
- Added support for `SeriesGroupBy.unique`
- Added support for `Series.dt.strftime` with the following directives:
  - %d: Day of the month as a zero-padded decimal number.
  - %m: Month as a zero-padded decimal number.
  - %Y: Year with century as a decimal number.
  - %H: Hour (24-hour clock) as a zero-padded decimal number.
  - %M: Minute as a zero-padded decimal number.
  - %S: Second as a zero-padded decimal number.
  - %f: Microsecond as a decimal number, zero-padded to 6 digits.
  - %j: Day of the year as a zero-padded decimal number.
  - %X: Locale’s appropriate time representation.
  - %%: A literal '%' character.
- Added support for `Series.between`.
- Added support for `include_groups=False` in `DataFrameGroupBy.apply`.
- Added support for `expand=True` in `Series.str.split`.
- Added support for `DataFrame.pop` and `Series.pop`.
- Added support for `first` and `last` in `DataFrameGroupBy.agg` and `SeriesGroupBy.agg`.
- Added support for `Index.drop_duplicates`.
- Added support for aggregations `"count"`, `"median"`, `np.median`,
  `"skew"`, `"std"`, `np.std` `"var"`, and `np.var` in
  `pd.pivot_table()`, `DataFrame.pivot_table()`, and `pd.crosstab()`.

#### Improvements
- Improve performance of `DataFrame.map`, `Series.apply` and `Series.map` methods by mapping numpy functions to snowpark functions if possible.
- Added documentation for `DataFrame.map`.
- Improve performance of `DataFrame.apply` by mapping numpy functions to snowpark functions if possible.
- Added documentation on the extent of Snowpark pandas interoperability with scikit-learn.
- Infer return type of functions in `Series.map`, `Series.apply` and `DataFrame.map` if type-hint is not provided.
- Added `call_count` to telemetry that counts method calls including interchange protocol calls.

## 1.26.0 (2024-12-05)

### Snowpark Python API Updates

#### New Features

- Added support for property `version` and class method `get_active_session` for `Session` class.
- Added new methods and variables to enhance data type handling and JSON serialization/deserialization:
  - To `DataType`, its derived classes, and `StructField`:
    - `type_name`: Returns the type name of the data.
    - `simple_string`: Provides a simple string representation of the data.
    - `json_value`: Returns the data as a JSON-compatible value.
    - `json`: Converts the data to a JSON string.
  - To `ArrayType`, `MapType`, `StructField`, `PandasSeriesType`, `PandasDataFrameType` and `StructType`:
    - `from_json`: Enables these types to be created from JSON data.
  - To `MapType`:
    - `keyType`: keys of the map
    - `valueType`: values of the map
- Added support for method `appName` in `SessionBuilder`.
- Added support for `include_nulls` argument in `DataFrame.unpivot`.
- Added support for following functions in `functions.py`:
  - `size` to get size of array, object, or map columns.
  - `collect_list` an alias of `array_agg`.
  - `substring` makes `len` argument optional.
- Added parameter `ast_enabled` to session for internal usage (default: `False`).

#### Improvements

- Added support for specifying the following to `DataFrame.create_or_replace_dynamic_table`:
  - `iceberg_config` A dictionary that can hold the following iceberg configuration options:
    - `external_volume`
    - `catalog`
    - `base_location`
    - `catalog_sync`
    - `storage_serialization_policy`
- Added support for nested data types to `DataFrame.print_schema`
- Added support for `level` parameter to `DataFrame.print_schema`
- Improved flexibility of `DataFrameReader` and `DataFrameWriter` API by adding support for the following:
  - Added `format` method to `DataFrameReader` and `DataFrameWriter` to specify file format when loading or unloading results.
  - Added `load` method to `DataFrameReader` to work in conjunction with `format`.
  - Added `save` method to `DataFrameWriter` to work in conjunction with `format`.
  - Added support to read keyword arguments to `options` method for `DataFrameReader` and `DataFrameWriter`.
- Relaxed the cloudpickle dependency for Python 3.11 to simplify build requirements. However, for Python 3.11, `cloudpickle==2.2.1` remains the only supported version.

#### Bug Fixes

- Removed warnings that dynamic pivot features were in private preview, because
  dynamic pivot is now generally available.
- Fixed a bug in `session.read.options` where `False` Boolean values were incorrectly parsed as `True` in the generated file format.

#### Dependency Updates

- Added a runtime dependency on `python-dateutil`.

### Snowpark pandas API Updates

#### New Features

- Added partial support for `Series.map` when `arg` is a pandas `Series` or a
  `collections.abc.Mapping`. No support for instances of `dict` that implement
  `__missing__` but are not instances of `collections.defaultdict`.
- Added support for `DataFrame.align` and `Series.align` for `axis=1` and `axis=None`.
- Added support for `pd.json_normalize`.
- Added support for `GroupBy.pct_change` with `axis=0`, `freq=None`, and `limit=None`.
- Added support for `DataFrameGroupBy.__iter__` and `SeriesGroupBy.__iter__`.
- Added support for `np.sqrt`, `np.trunc`, `np.floor`, numpy trig functions, `np.exp`, `np.abs`, `np.positive` and `np.negative`.
- Added partial support for the dataframe interchange protocol method
  `DataFrame.__dataframe__()`.

#### Bug Fixes

- Fixed a bug in `df.loc` where setting a single column from a series results in unexpected `None` values.

#### Improvements

- Use UNPIVOT INCLUDE NULLS for unpivot operations in pandas instead of sentinel values.
- Improved documentation for pd.read_excel.

## 1.25.0 (2024-11-14)

### Snowpark Python API Updates

#### New Features

- Added the following new functions in `snowflake.snowpark.dataframe`:
  - `map`
- Added support for passing parameter `include_error` to `Session.query_history` to record queries that have error during execution.

#### Improvements

- When target stage is not set in profiler, a default stage from `Session.get_session_stage` is used instead of raising `SnowparkSQLException`.
- Allowed lower case or mixed case input when calling `Session.stored_procedure_profiler.set_active_profiler`.
- Added distributed tracing using open telemetry APIs for action function in `DataFrame`:
  - `cache_result`
- Removed opentelemetry warning from logging.

#### Bug Fixes

- Fixed the pre-action and post-action query propagation when `In` expression were used in selects.
- Fixed a bug that raised error `AttributeError` while calling `Session.stored_procedure_profiler.get_output` when `Session.stored_procedure_profiler` is disabled.

#### Dependency Updates

- Added a dependency on `protobuf>=5.28` and `tzlocal` at runtime.
- Added a dependency on `protoc-wheel-0` for the development profile.
- Require `snowflake-connector-python>=3.12.0, <4.0.0` (was `>=3.10.0`).

### Snowpark pandas API Updates

#### Dependency Updates

- Updated `modin` from 0.28.1 to 0.30.1.
- Added support for all `pandas` 2.2.x versions.

#### New Features

- Added support for `Index.to_numpy`.
- Added support for `DataFrame.align` and `Series.align` for `axis=0`.
- Added support for `size` in `GroupBy.aggregate`, `DataFrame.aggregate`, and `Series.aggregate`.
- Added support for `snowflake.snowpark.functions.window`
- Added support for `pd.read_pickle` (Uses native pandas for processing).
- Added support for `pd.read_html` (Uses native pandas for processing).
- Added support for `pd.read_xml` (Uses native pandas for processing).
- Added support for aggregation functions `"size"` and `len` in `GroupBy.aggregate`, `DataFrame.aggregate`, and `Series.aggregate`.
- Added support for list values in `Series.str.len`.

#### Bug Fixes

- Fixed a bug where aggregating a single-column dataframe with a single callable function (e.g. `pd.DataFrame([0]).agg(np.mean)`) would fail to transpose the result.
- Fixed bugs where `DataFrame.dropna()` would:
  - Treat an empty `subset` (e.g. `[]`) as if it specified all columns instead of no columns.
  - Raise a `TypeError` for a scalar `subset` instead of filtering on just that column.
  - Raise a `ValueError` for a `subset` of type `pandas.Index` instead of filtering on the columns in the index.
- Disable creation of scoped read only table to mitigate Disable creation of scoped read only table to mitigate `TableNotFoundError` when using dynamic pivot in notebook environment.
- Fixed a bug when concat dataframe or series objects are coming from the same dataframe when axis = 1.

#### Improvements

- Improve np.where with scalar x value by eliminating unnecessary join and temp table creation.
- Improve get_dummies performance by flattening the pivot with join.
- Improve align performance when aligning on row position column by removing unnecessary window functions.



### Snowpark Local Testing Updates

#### New Features

- Added support for patching functions that are unavailable in the `snowflake.snowpark.functions` module.
- Added support for `snowflake.snowpark.functions.any_value`

#### Bug Fixes

- Fixed a bug where `Table.update` could not handle `VariantType`, `MapType`, and `ArrayType` data types.
- Fixed a bug where column aliases were incorrectly resolved in `DataFrame.join`, causing errors when selecting columns from a joined DataFrame.
- Fixed a bug where `Table.update` and `Table.merge` could fail if the target table's index was not the default `RangeIndex`.

## 1.24.0 (2024-10-28)

### Snowpark Python API Updates

#### New Features

- Updated `Session` class to be thread-safe. This allows concurrent DataFrame transformations, DataFrame actions, UDF and stored procedure registration, and concurrent file uploads when using the same `Session` object.
  - The feature is disabled by default and can be enabled by setting `FEATURE_THREAD_SAFE_PYTHON_SESSION` to `True` for account.
  - Updating session configurations, like changing database or schema, when multiple threads are using the session may lead to unexpected behavior.
  - When enabled, some internally created temporary table names returned from `DataFrame.queries` API are not deterministic, and may be different when DataFrame actions are executed. This does not affect explicit user-created temporary tables.
- Added support for 'Service' domain to `session.lineage.trace` API.
- Added support for `copy_grants` parameter when registering UDxF and stored procedures.
- Added support for the following methods in `DataFrameWriter` to support daisy-chaining:
  - `option`
  - `options`
  - `partition_by`
- Added support for `snowflake_cortex_summarize`.

#### Improvements

- Improved the following new capability for function `snowflake.snowpark.functions.array_remove` it is now possible to use in python.
- Disables sql simplification when sort is performed after limit.
  - Previously, `df.sort().limit()` and `df.limit().sort()` generates the same query with sort in front of limit. Now, `df.limit().sort()` will generate query that reads `df.limit().sort()`.
  - Improve performance of generated query for `df.limit().sort()`, because limit stops table scanning as soon as the number of records is satisfied.
- Added a client side error message for when an invalid stage location is passed to DataFrame read functions.

#### Bug Fixes

- Fixed a bug where the automatic cleanup of temporary tables could interfere with the results of async query execution.
- Fixed a bug in `DataFrame.analytics.time_series_agg` function to handle multiple data points in same sliding interval.
- Fixed a bug that created inconsistent casing in field names of structured objects in iceberg schemas.

#### Deprecations

- Deprecated warnings will be triggered when using snowpark-python with Python 3.8. For more details, please refer to https://docs.snowflake.com/en/developer-guide/python-runtime-support-policy.

### Snowpark pandas API Updates

#### New Features

- Added support for `np.subtract`, `np.multiply`, `np.divide`, and `np.true_divide`.
- Added support for tracking usages of `__array_ufunc__`.
- Added numpy compatibility support for `np.float_power`, `np.mod`, `np.remainder`, `np.greater`, `np.greater_equal`, `np.less`, `np.less_equal`, `np.not_equal`, and `np.equal`.
- Added numpy compatibility support for `np.log`, `np.log2`, and `np.log10`
- Added support for `DataFrameGroupBy.bfill`, `SeriesGroupBy.bfill`, `DataFrameGroupBy.ffill`, and `SeriesGroupBy.ffill`.
- Added support for `on` parameter with `Resampler`.
- Added support for timedelta inputs in `value_counts()`.
- Added support for applying Snowpark Python function `snowflake_cortex_summarize`.
- Added support for `DataFrame.attrs` and `Series.attrs`.
- Added support for `DataFrame.style`.
- Added numpy compatibility support for `np.full_like`

#### Improvements

- Improved generated SQL query for `head` and `iloc` when the row key is a slice.
- Improved error message when passing an unknown timezone to `tz_convert` and `tz_localize` in `Series`, `DataFrame`, `Series.dt`, and `DatetimeIndex`.
- Improved documentation for `tz_convert` and `tz_localize` in `Series`, `DataFrame`, `Series.dt`, and `DatetimeIndex` to specify the supported timezone formats.
- Added additional kwargs support for `df.apply` and `series.apply` ( as well as `map` and `applymap` ) when using snowpark functions. This allows for some position independent compatibility between apply and functions where the first argument is not a pandas object.
- Improved generated SQL query for `iloc` and `iat` when the row key is a scalar.
- Removed all joins in `iterrows`.
- Improved documentation for `Series.map` to reflect the unsupported features.
- Added support for `np.may_share_memory` which is used internally by many scikit-learn functions. This method will always return false when called with a Snowpark pandas object.

#### Bug Fixes

- Fixed a bug where `DataFrame` and `Series` `pct_change()` would raise `TypeError` when input contained timedelta columns.
- Fixed a bug where `replace()` would sometimes propagate `Timedelta` types incorrectly through `replace()`. Instead raise `NotImplementedError` for `replace()` on `Timedelta`.
- Fixed a bug where `DataFrame` and `Series` `round()` would raise `AssertionError` for `Timedelta` columns. Instead raise `NotImplementedError` for `round()` on `Timedelta`.
- Fixed a bug where `reindex` fails when the new index is a Series with non-overlapping types from the original index.
- Fixed a bug where calling `__getitem__` on a DataFrameGroupBy object always returned a DataFrameGroupBy object if `as_index=False`.
- Fixed a bug where inserting timedelta values into an existing column would silently convert the values to integers instead of raising `NotImplementedError`.
- Fixed a bug where `DataFrame.shift()` on axis=0 and axis=1 would fail to propagate timedelta types.
- `DataFrame.abs()`, `DataFrame.__neg__()`, `DataFrame.stack()`, and `DataFrame.unstack()` now raise `NotImplementedError` for timedelta inputs instead of failing to propagate timedelta types.

### Snowpark Local Testing Updates

#### Bug Fixes

- Fixed a bug where `DataFrame.alias` raises `KeyError` for input column name.
- Fixed a bug where `to_csv` on Snowflake stage fails when data contains empty strings.

## 1.23.0 (2024-10-09)

### Snowpark Python API Updates

#### New Features

- Added the following new functions in `snowflake.snowpark.functions`:
  - `make_interval`
- Added support for using Snowflake Interval constants with `Window.range_between()` when the order by column is TIMESTAMP or DATE type.
- Added support for file writes. This feature is currently in private preview.
- Added `thread_id` to `QueryRecord` to track the thread id submitting the query history.
- Added support for `Session.stored_procedure_profiler`.

#### Improvements

#### Bug Fixes

- Fixed a bug where registering a stored procedure or UDxF with type hints would give a warning `'NoneType' has no len() when trying to read default values from function`.

### Snowpark pandas API Updates

#### New Features

- Added support for `TimedeltaIndex.mean` method.
- Added support for some cases of aggregating `Timedelta` columns on `axis=0` with `agg` or `aggregate`.
- Added support for `by`, `left_by`, `right_by`, `left_index`, and `right_index` for `pd.merge_asof`.
- Added support for passing parameter `include_describe` to `Session.query_history`.
- Added support for `DatetimeIndex.mean` and `DatetimeIndex.std` methods.
- Added support for `Resampler.asfreq`, `Resampler.indices`, `Resampler.nunique`, and `Resampler.quantile`.
- Added support for `resample` frequency `W`, `ME`, `YE` with `closed = "left"`.
- Added support for `DataFrame.rolling.corr` and `Series.rolling.corr` for `pairwise = False` and int `window`.
- Added support for string time-based `window` and `min_periods = None` for `Rolling`.
- Added support for `DataFrameGroupBy.fillna` and `SeriesGroupBy.fillna`.
- Added support for constructing `Series` and `DataFrame` objects with the lazy `Index` object as `data`, `index`, and `columns` arguments.
- Added support for constructing `Series` and `DataFrame` objects with `index` and `column` values not present in `DataFrame`/`Series` `data`.
- Added support for `pd.read_sas` (Uses native pandas for processing).
- Added support for applying `rolling().count()` and `expanding().count()` to `Timedelta` series and columns.
- Added support for `tz` in both `pd.date_range` and `pd.bdate_range`.
- Added support for `Series.items`.
- Added support for `errors="ignore"` in `pd.to_datetime`.
- Added support for `DataFrame.tz_localize` and `Series.tz_localize`.
- Added support for `DataFrame.tz_convert` and `Series.tz_convert`.
- Added support for applying Snowpark Python functions (e.g., `sin`) in `Series.map`, `Series.apply`, `DataFrame.apply` and `DataFrame.applymap`.

#### Improvements

- Improved `to_pandas` to persist the original timezone offset for TIMESTAMP_TZ type.
- Improved `dtype` results for TIMESTAMP_TZ type to show correct timezone offset.
- Improved `dtype` results for TIMESTAMP_LTZ type to show correct timezone.
- Improved error message when passing non-bool value to `numeric_only` for groupby aggregations.
- Removed unnecessary warning about sort algorithm in `sort_values`.
- Use SCOPED object for internal create temp tables. The SCOPED objects will be stored sproc scoped if created within stored sproc, otherwise will be session scoped, and the object will be automatically cleaned at the end of the scope.
- Improved warning messages for operations that lead to materialization with inadvertent slowness.
- Removed unnecessary warning message about `convert_dtype` in `Series.apply`.

#### Bug Fixes

- Fixed a bug where an `Index` object created from a `Series`/`DataFrame` incorrectly updates the `Series`/`DataFrame`'s index name after an inplace update has been applied to the original `Series`/`DataFrame`.
- Suppressed an unhelpful `SettingWithCopyWarning` that sometimes appeared when printing `Timedelta` columns.
- Fixed `inplace` argument for `Series` objects derived from other `Series` objects.
- Fixed a bug where `Series.sort_values` failed if series name overlapped with index column name.
- Fixed a bug where transposing a dataframe would map `Timedelta` index levels to integer column levels.
- Fixed a bug where `Resampler` methods on timedelta columns would produce integer results.
- Fixed a bug where `pd.to_numeric()` would leave `Timedelta` inputs as `Timedelta` instead of converting them to integers.
- Fixed `loc` set when setting a single row, or multiple rows, of a DataFrame with a Series value.

### Snowpark Local Testing Updates

#### Bug Fixes

- Fixed a bug where nullable columns were annotated wrongly.
- Fixed a bug where the `date_add` and `date_sub` functions failed for `NULL` values.
- Fixed a bug where `equal_null` could fail inside a merge statement.
- Fixed a bug where `row_number` could fail inside a Window function.
- Fixed a bug where updates could fail when the source is the result of a join.


## 1.22.1 (2024-09-11)
This is a re-release of 1.22.0. Please refer to the 1.22.0 release notes for detailed release content.


## 1.22.0 (2024-09-10)

### Snowpark Python API Updates

### New Features

- Added the following new functions in `snowflake.snowpark.functions`:
  - `array_remove`
  - `ln`

#### Improvements

- Improved documentation for `Session.write_pandas` by making `use_logical_type` option more explicit.
- Added support for specifying the following to `DataFrameWriter.save_as_table`:
  - `enable_schema_evolution`
  - `data_retention_time`
  - `max_data_extension_time`
  - `change_tracking`
  - `copy_grants`
  - `iceberg_config` A dicitionary that can hold the following iceberg configuration options:
      - `external_volume`
      - `catalog`
      - `base_location`
      - `catalog_sync`
      - `storage_serialization_policy`
- Added support for specifying the following to `DataFrameWriter.copy_into_table`:
  - `iceberg_config` A dicitionary that can hold the following iceberg configuration options:
      - `external_volume`
      - `catalog`
      - `base_location`
      - `catalog_sync`
      - `storage_serialization_policy`
- Added support for specifying the following parameters to `DataFrame.create_or_replace_dynamic_table`:
  - `mode`
  - `refresh_mode`
  - `initialize`
  - `clustering_keys`
  - `is_transient`
  - `data_retention_time`
  - `max_data_extension_time`

#### Bug Fixes

- Fixed a bug in `session.read.csv` that caused an error when setting `PARSE_HEADER = True` in an externally defined file format.
- Fixed a bug in query generation from set operations that allowed generation of duplicate queries when children have common subqueries.
- Fixed a bug in `session.get_session_stage` that referenced a non-existing stage after switching database or schema.
- Fixed a bug where calling `DataFrame.to_snowpark_pandas` without explicitly initializing the Snowpark pandas plugin caused an error.
- Fixed a bug where using the `explode` function in dynamic table creation caused a SQL compilation error due to improper boolean type casting on the `outer` parameter.

### Snowpark Local Testing Updates

#### New Features

- Added support for type coercion when passing columns as input to UDF calls.
- Added support for `Index.identical`.

#### Bug Fixes

- Fixed a bug where the truncate mode in `DataFrameWriter.save_as_table` incorrectly handled DataFrames containing only a subset of columns from the existing table.
- Fixed a bug where function `to_timestamp` does not set the default timezone of the column datatype.

### Snowpark pandas API Updates

#### New Features

- Added limited support for the `Timedelta` type, including the following features. Snowpark pandas will raise `NotImplementedError` for unsupported `Timedelta` use cases.
  - supporting tracking the Timedelta type through `copy`, `cache_result`, `shift`, `sort_index`, `assign`, `bfill`, `ffill`, `fillna`, `compare`, `diff`, `drop`, `dropna`, `duplicated`, `empty`, `equals`, `insert`, `isin`, `isna`, `items`, `iterrows`, `join`, `len`, `mask`, `melt`, `merge`, `nlargest`, `nsmallest`, `to_pandas`.
  - converting non-timedelta to timedelta via `astype`.
  - `NotImplementedError` will be raised for the rest of methods that do not support `Timedelta`.
  - support for subtracting two timestamps to get a Timedelta.
  - support indexing with Timedelta data columns.
  - support for adding or subtracting timestamps and `Timedelta`.
  - support for binary arithmetic between two `Timedelta` values.
  - support for binary arithmetic and comparisons between `Timedelta` values and numeric values.
  - support for lazy `TimedeltaIndex`.
  - support for `pd.to_timedelta`.
  - support for `GroupBy` aggregations `min`, `max`, `mean`, `idxmax`, `idxmin`, `std`, `sum`, `median`, `count`, `any`, `all`, `size`, `nunique`, `head`, `tail`, `aggregate`.
  - support for `GroupBy` filtrations `first` and `last`.
  - support for `TimedeltaIndex` attributes: `days`, `seconds`, `microseconds` and `nanoseconds`.
  - support for `diff` with timestamp columns on `axis=0` and `axis=1`
  - support for `TimedeltaIndex` methods: `ceil`, `floor` and `round`.
  - support for `TimedeltaIndex.total_seconds` method.
- Added support for index's arithmetic and comparison operators.
- Added support for `Series.dt.round`.
- Added documentation pages for `DatetimeIndex`.
- Added support for `Index.name`, `Index.names`, `Index.rename`, and `Index.set_names`.
- Added support for `Index.__repr__`.
- Added support for `DatetimeIndex.month_name` and `DatetimeIndex.day_name`.
- Added support for `Series.dt.weekday`, `Series.dt.time`, and `DatetimeIndex.time`.
- Added support for `Index.min` and `Index.max`.
- Added support for `pd.merge_asof`.
- Added support for `Series.dt.normalize` and `DatetimeIndex.normalize`.
- Added support for `Index.is_boolean`, `Index.is_integer`, `Index.is_floating`, `Index.is_numeric`, and `Index.is_object`.
- Added support for `DatetimeIndex.round`, `DatetimeIndex.floor` and `DatetimeIndex.ceil`.
- Added support for `Series.dt.days_in_month` and `Series.dt.daysinmonth`.
- Added support for `DataFrameGroupBy.value_counts` and `SeriesGroupBy.value_counts`.
- Added support for `Series.is_monotonic_increasing` and `Series.is_monotonic_decreasing`.
- Added support for `Index.is_monotonic_increasing` and `Index.is_monotonic_decreasing`.
- Added support for `pd.crosstab`.
- Added support for `pd.bdate_range` and included business frequency support (B, BME, BMS, BQE, BQS, BYE, BYS) for both `pd.date_range` and `pd.bdate_range`.
- Added support for lazy `Index` objects  as `labels` in `DataFrame.reindex` and `Series.reindex`.
- Added support for `Series.dt.days`, `Series.dt.seconds`, `Series.dt.microseconds`, and `Series.dt.nanoseconds`.
- Added support for creating a `DatetimeIndex` from an `Index` of numeric or string type.
- Added support for string indexing with `Timedelta` objects.
- Added support for `Series.dt.total_seconds` method.
- Added support for `DataFrame.apply(axis=0)`.
- Added support for `Series.dt.tz_convert` and `Series.dt.tz_localize`.
- Added support for `DatetimeIndex.tz_convert` and `DatetimeIndex.tz_localize`.

#### Improvements

- Improve concat, join performance when operations are performed on series coming from the same dataframe by avoiding unnecessary joins.
- Refactored `quoted_identifier_to_snowflake_type` to avoid making metadata queries if the types have been cached locally.
- Improved `pd.to_datetime` to handle all local input cases.
- Create a lazy index from another lazy index without pulling data to client.
- Raised `NotImplementedError` for Index bitwise operators.
- Display a more clear error message when `Index.names` is set to a non-like-like object.
- Raise a warning whenever MultiIndex values are pulled in locally.
- Improve warning message for `pd.read_snowflake` include the creation reason when temp table creation is triggered.
- Improve performance for `DataFrame.set_index`, or setting `DataFrame.index` or `Series.index` by avoiding checks require eager evaluation. As a consequence, when the new index that does not match the current `Series`/`DataFrame` object length, a `ValueError` is no longer raised. Instead, when the `Series`/`DataFrame` object is longer than the provided index, the `Series`/`DataFrame`'s new index is filled with `NaN` values for the "extra" elements. Otherwise, the extra values in the provided index are ignored.
- Properly raise `NotImplementedError` when ambiguous/nonexistent are non-string in `ceil`/`floor`/`round`.

#### Bug Fixes

- Stopped ignoring nanoseconds in `pd.Timedelta` scalars.
- Fixed AssertionError in tree of binary operations.
- Fixed bug in `Series.dt.isocalendar` using a named Series
- Fixed `inplace` argument for Series objects derived from DataFrame columns.
- Fixed a bug where `Series.reindex` and `DataFrame.reindex` did not update the result index's name correctly.
- Fixed a bug where `Series.take` did not error when `axis=1` was specified.


## 1.21.1 (2024-09-05)

### Snowpark Python API Updates

#### Bug Fixes

- Fixed a bug where using `to_pandas_batches` with async jobs caused an error due to improper handling of waiting for asynchronous query completion.

## 1.21.0 (2024-08-19)

### Snowpark Python API Updates

#### New Features

- Added support for `snowflake.snowpark.testing.assert_dataframe_equal` that is a utility function to check the equality of two Snowpark DataFrames.

#### Improvements

- Added support server side string size limitations.
- Added support to create and invoke stored procedures, UDFs and UDTFs with optional arguments.
- Added support for column lineage in the DataFrame.lineage.trace API.
- Added support for passing `INFER_SCHEMA` options to `DataFrameReader` via `INFER_SCHEMA_OPTIONS`.
- Added support for passing `parameters` parameter to `Column.rlike` and `Column.regexp`.
- Added support for automatically cleaning up temporary tables created by `df.cache_result()` in the current session, when the DataFrame is no longer referenced (i.e., gets garbage collected). It is still an experimental feature not enabled by default, and can be enabled by setting `session.auto_clean_up_temp_table_enabled` to `True`.
- Added support for string literals to the `fmt` parameter of `snowflake.snowpark.functions.to_date`.
- Added support for system$reference function.

#### Bug Fixes

- Fixed a bug where SQL generated for selecting `*` column has an incorrect subquery.
- Fixed a bug in `DataFrame.to_pandas_batches` where the iterator could throw an error if certain transformation is made to the pandas dataframe due to wrong isolation level.
- Fixed a bug in `DataFrame.lineage.trace` to split the quoted feature view's name and version correctly.
- Fixed a bug in `Column.isin` that caused invalid sql generation when passed an empty list.
- Fixed a bug that fails to raise NotImplementedError while setting cell with list like item.

### Snowpark Local Testing Updates

#### New Features

- Added support for the following APIs:
  - snowflake.snowpark.functions
    - `rank`
    - `dense_rank`
    - `percent_rank`
    - `cume_dist`
    - `ntile`
    - `datediff`
    - `array_agg`
  - snowflake.snowpark.column.Column.within_group
- Added support for parsing flags in regex statements for mocked plans. This maintains parity with the `rlike` and `regexp` changes above.

#### Bug Fixes

- Fixed a bug where Window Functions LEAD and LAG do not handle option `ignore_nulls` properly.
- Fixed a bug where values were not populated into the result DataFrame during the insertion of table merge operation.

#### Improvements

- Fix pandas FutureWarning about integer indexing.

### Snowpark pandas API Updates

#### New Features

- Added support for `DataFrame.backfill`, `DataFrame.bfill`, `Series.backfill`, and `Series.bfill`.
- Added support for `DataFrame.compare` and `Series.compare` with default parameters.
- Added support for `Series.dt.microsecond` and `Series.dt.nanosecond`.
- Added support for `Index.is_unique` and `Index.has_duplicates`.
- Added support for `Index.equals`.
- Added support for `Index.value_counts`.
- Added support for `Series.dt.day_name` and `Series.dt.month_name`.
- Added support for indexing on Index, e.g., `df.index[:10]`.
- Added support for `DataFrame.unstack` and `Series.unstack`.
- Added support for `DataFrame.asfreq` and `Series.asfreq`.
- Added support for `Series.dt.is_month_start` and `Series.dt.is_month_end`.
- Added support for `Index.all` and `Index.any`.
- Added support for `Series.dt.is_year_start` and `Series.dt.is_year_end`.
- Added support for `Series.dt.is_quarter_start` and `Series.dt.is_quarter_end`.
- Added support for lazy `DatetimeIndex`.
- Added support for `Series.argmax` and `Series.argmin`.
- Added support for `Series.dt.is_leap_year`.
- Added support for `DataFrame.items`.
- Added support for `Series.dt.floor` and `Series.dt.ceil`.
- Added support for `Index.reindex`.
- Added support for `DatetimeIndex` properties: `year`, `month`, `day`, `hour`, `minute`, `second`, `microsecond`,
    `nanosecond`, `date`, `dayofyear`, `day_of_year`, `dayofweek`, `day_of_week`, `weekday`, `quarter`,
    `is_month_start`, `is_month_end`, `is_quarter_start`, `is_quarter_end`, `is_year_start`, `is_year_end`
    and `is_leap_year`.
- Added support for `Resampler.fillna` and `Resampler.bfill`.
- Added limited support for the `Timedelta` type, including creating `Timedelta` columns and `to_pandas`.
- Added support for `Index.argmax` and `Index.argmin`.

#### Improvements

- Removed the public preview warning message when importing Snowpark pandas.
- Removed unnecessary count query from `SnowflakeQueryCompiler.is_series_like` method.
- `Dataframe.columns` now returns native pandas Index object instead of Snowpark Index object.
- Refactor and introduce `query_compiler` argument in `Index` constructor to create `Index` from query compiler.
- `pd.to_datetime` now returns a DatetimeIndex object instead of a Series object.
- `pd.date_range` now returns a DatetimeIndex object instead of a Series object.

#### Bug Fixes

- Made passing an unsupported aggregation function to `pivot_table` raise `NotImplementedError` instead of `KeyError`.
- Removed axis labels and callable names from error messages and telemetry about unsupported aggregations.
- Fixed AssertionError in `Series.drop_duplicates` and `DataFrame.drop_duplicates` when called after `sort_values`.
- Fixed a bug in `Index.to_frame` where the result frame's column name may be wrong where name is unspecified.
- Fixed a bug where some Index docstrings are ignored.
- Fixed a bug in `Series.reset_index(drop=True)` where the result name may be wrong.
- Fixed a bug in `Groupby.first/last` ordering by the correct columns in the underlying window expression.

## 1.20.0 (2024-07-17)

### Snowpark Python API Updates

#### Improvements

- Added distributed tracing using open telemetry APIs for table stored procedure function in `DataFrame`:
  - `_execute_and_get_query_id`
- Added support for the `arrays_zip` function.
- Improves performance for binary column expression and `df._in` by avoiding unnecessary cast for numeric values. You can enable this optimization by setting `session.eliminate_numeric_sql_value_cast_enabled = True`.
- Improved error message for `write_pandas` when the target table does not exist and `auto_create_table=False`.
- Added open telemetry tracing on UDxF functions in Snowpark.
- Added open telemetry tracing on stored procedure registration in Snowpark.
- Added a new optional parameter called `format_json` to the `Session.SessionBuilder.app_name` function that sets the app name in the `Session.query_tag` in JSON format. By default, this parameter is set to `False`.

#### Bug Fixes
- Fixed a bug where SQL generated for `lag(x, 0)` was incorrect and failed with error message `argument 1 to function LAG needs to be constant, found 'SYSTEM$NULL_TO_FIXED(null)'`.

### Snowpark Local Testing Updates

#### New Features

- Added support for the following APIs:
  - snowflake.snowpark.functions
    - random
- Added new parameters to `patch` function when registering a mocked function:
  - `distinct` allows an alternate function to be specified for when a sql function should be distinct.
  - `pass_column_index` passes a named parameter `column_index` to the mocked function that contains the pandas.Index for the input data.
  - `pass_row_index` passes a named parameter `row_index` to the mocked function that is the 0 indexed row number the function is currently operating on.
  - `pass_input_data` passes a named parameter `input_data` to the mocked function that contains the entire input dataframe for the current expression.
  - Added support for the `column_order` parameter to method `DataFrameWriter.save_as_table`.


#### Bug Fixes
- Fixed a bug that caused DecimalType columns to be incorrectly truncated to integer precision when used in BinaryExpressions.

### Snowpark pandas API Updates

#### New Features
- Added support for `DataFrameGroupBy.all`, `SeriesGroupBy.all`, `DataFrameGroupBy.any`, and `SeriesGroupBy.any`.
- Added support for `DataFrame.nlargest`, `DataFrame.nsmallest`, `Series.nlargest` and `Series.nsmallest`.
- Added support for `replace` and `frac > 1` in `DataFrame.sample` and `Series.sample`.
- Added support for `read_excel` (Uses local pandas for processing)
- Added support for `Series.at`, `Series.iat`, `DataFrame.at`, and `DataFrame.iat`.
- Added support for `Series.dt.isocalendar`.
- Added support for `Series.case_when` except when condition or replacement is callable.
- Added documentation pages for `Index` and its APIs.
- Added support for `DataFrame.assign`.
- Added support for `DataFrame.stack`.
- Added support for `DataFrame.pivot` and `pd.pivot`.
- Added support for `DataFrame.to_csv` and `Series.to_csv`.
- Added partial support for `Series.str.translate` where the values in the `table` are single-codepoint strings.
- Added support for `DataFrame.corr`.
- Allow `df.plot()` and `series.plot()` to be called, materializing the data into the local client
- Added support for `DataFrameGroupBy` and `SeriesGroupBy` aggregations `first` and `last`
- Added support for `DataFrameGroupBy.get_group`.
- Added support for `limit` parameter when `method` parameter is used in `fillna`.
- Added partial support for `Series.str.translate` where the values in the `table` are single-codepoint strings.
- Added support for `DataFrame.corr`.
- Added support for `DataFrame.equals` and `Series.equals`.
- Added support for `DataFrame.reindex` and `Series.reindex`.
- Added support for `Index.astype`.
- Added support for `Index.unique` and `Index.nunique`.
- Added support for `Index.sort_values`.

#### Bug Fixes
- Fixed an issue when using np.where and df.where when the scalar 'other' is the literal 0.
- Fixed a bug regarding precision loss when converting to Snowpark pandas `DataFrame` or `Series` with `dtype=np.uint64`.
- Fixed bug where `values` is set to `index` when `index` and `columns` contain all columns in DataFrame during `pivot_table`.

#### Improvements
- Added support for `Index.copy()`
- Added support for Index APIs: `dtype`, `values`, `item()`, `tolist()`, `to_series()` and `to_frame()`
- Expand support for DataFrames with no rows in `pd.pivot_table` and `DataFrame.pivot_table`.
- Added support for `inplace` parameter in `DataFrame.sort_index` and `Series.sort_index`.


## 1.19.0 (2024-06-25)

### Snowpark Python API Updates

#### New Features

- Added support for `to_boolean` function.
- Added documentation pages for Index and its APIs.

#### Bug Fixes

- Fixed a bug where python stored procedure with table return type fails when run in a task.
- Fixed a bug where df.dropna fails due to `RecursionError: maximum recursion depth exceeded` when the DataFrame has more than 500 columns.
- Fixed a bug where `AsyncJob.result("no_result")` doesn't wait for the query to finish execution.


### Snowpark Local Testing Updates

#### New Features

- Added support for the `strict` parameter when registering UDFs and Stored Procedures.

#### Bug Fixes

- Fixed a bug in convert_timezone that made the setting the source_timezone parameter return an error.
- Fixed a bug where creating DataFrame with empty data of type `DateType` raises `AttributeError`.
- Fixed a bug that table merge fails when update clause exists but no update takes place.
- Fixed a bug in mock implementation of `to_char` that raises `IndexError` when incoming column has nonconsecutive row index.
- Fixed a bug in handling of `CaseExpr` expressions that raises `IndexError` when incoming column has nonconsecutive row index.
- Fixed a bug in implementation of `Column.like` that raises `IndexError` when incoming column has nonconsecutive row index.

#### Improvements

- Added support for type coercion in the implementation of DataFrame.replace, DataFrame.dropna and the mock function `iff`.

### Snowpark pandas API Updates

#### New Features

- Added partial support for `DataFrame.pct_change` and `Series.pct_change` without the `freq` and `limit` parameters.
- Added support for `Series.str.get`.
- Added support for `Series.dt.dayofweek`, `Series.dt.day_of_week`, `Series.dt.dayofyear`, and `Series.dt.day_of_year`.
- Added support for `Series.str.__getitem__` (`Series.str[...]`).
- Added support for `Series.str.lstrip` and `Series.str.rstrip`.
- Added support for `DataFrameGroupBy.size` and `SeriesGroupBy.size`.
- Added support for `DataFrame.expanding` and `Series.expanding` for aggregations `count`, `sum`, `min`, `max`, `mean`, `std`, `var`, and `sem` with `axis=0`.
- Added support for `DataFrame.rolling` and `Series.rolling` for aggregation `count` with `axis=0`.
- Added support for `Series.str.match`.
- Added support for `DataFrame.resample` and `Series.resample` for aggregations `size`, `first`, and `last`.
- Added support for `DataFrameGroupBy.all`, `SeriesGroupBy.all`, `DataFrameGroupBy.any`, and `SeriesGroupBy.any`.
- Added support for `DataFrame.nlargest`, `DataFrame.nsmallest`, `Series.nlargest` and `Series.nsmallest`.
- Added support for `replace` and `frac > 1` in `DataFrame.sample` and `Series.sample`.
- Added support for `read_excel` (Uses local pandas for processing)
- Added support for `Series.at`, `Series.iat`, `DataFrame.at`, and `DataFrame.iat`.
- Added support for `Series.dt.isocalendar`.
- Added support for `Series.case_when` except when condition or replacement is callable.
- Added documentation pages for `Index` and its APIs.
- Added support for `DataFrame.assign`.
- Added support for `DataFrame.stack`.
- Added support for `DataFrame.pivot` and `pd.pivot`.
- Added support for `DataFrame.to_csv` and `Series.to_csv`.
- Added support for `Index.T`.

#### Bug Fixes

- Fixed a bug that causes output of GroupBy.aggregate's columns to be ordered incorrectly.
- Fixed a bug where `DataFrame.describe` on a frame with duplicate columns of differing dtypes could cause an error or incorrect results.
- Fixed a bug in `DataFrame.rolling` and `Series.rolling` so `window=0` now throws `NotImplementedError` instead of `ValueError`

#### Improvements

- Added support for named aggregations in `DataFrame.aggregate` and `Series.aggregate` with `axis=0`.
- `pd.read_csv` reads using the native pandas CSV parser, then uploads data to snowflake using parquet. This enables most of the parameters supported by `read_csv` including date parsing and numeric conversions. Uploading via parquet is roughly twice as fast as uploading via CSV.
- Initial work to support an `pd.Index` directly in Snowpark pandas. Support for `pd.Index` as a first-class component of Snowpark pandas is coming soon.
- Added a lazy index constructor and support for `len`, `shape`, `size`, `empty`, `to_pandas()` and `names`. For `df.index`, Snowpark pandas creates a lazy index object.
- For `df.columns`, Snowpark pandas supports a non-lazy version of an `Index` since the data is already stored locally.

## 1.18.0 (2024-05-28)

### Snowpark Python API Updates

#### Improvements

- Improved error message to remind users set `{"infer_schema": True}` when reading csv file without specifying its schema.
- Improved error handling for `Session.create_dataframe` when called with more than 512 rows and using `format` or `pyformat` `paramstyle`.

### Snowpark pandas API Updates

#### New Features

- Added `DataFrame.cache_result` and `Series.cache_result` methods for users to persist DataFrames and Series to a temporary table lasting the duration of the session to improve latency of subsequent operations.

#### Bug Fixes

#### Improvements

- Added partial support for `DataFrame.pivot_table` with no `index` parameter, as well as for `margins` parameter.
- Updated the signature of `DataFrame.shift`/`Series.shift`/`DataFrameGroupBy.shift`/`SeriesGroupBy.shift` to match pandas 2.2.1. Snowpark pandas does not yet support the newly-added `suffix` argument, or sequence values of `periods`.
- Re-added support for `Series.str.split`.

#### Bug Fixes

- Fixed how we support mixed columns for string methods (`Series.str.*`).

### Snowpark Local Testing Updates

#### New Features

- Added support for the following DataFrameReader read options to file formats `csv` and `json`:
  - PURGE
  - PATTERN
  - INFER_SCHEMA with value being `False`
  - ENCODING with value being `UTF8`
- Added support for `DataFrame.analytics.moving_agg` and `DataFrame.analytics.cumulative_agg_agg`.
- Added support for `if_not_exists` parameter during UDF and stored procedure registration.

#### Bug Fixes

- Fixed a bug that when processing time format, fractional second part is not handled properly.
- Fixed a bug that caused function calls on `*` to fail.
- Fixed a bug that prevented creation of map and struct type objects.
- Fixed a bug that function `date_add` was unable to handle some numeric types.
- Fixed a bug that `TimestampType` casting resulted in incorrect data.
- Fixed a bug that caused `DecimalType` data to have incorrect precision in some cases.
- Fixed a bug where referencing missing table or view raises confusing `IndexError`.
- Fixed a bug that mocked function `to_timestamp_ntz` can not handle None data.
- Fixed a bug that mocked UDFs handles output data of None improperly.
- Fixed a bug where `DataFrame.with_column_renamed` ignores attributes from parent DataFrames after join operations.
- Fixed a bug that integer precision of large value gets lost when converted to pandas DataFrame.
- Fixed a bug that the schema of datetime object is wrong when create DataFrame from a pandas DataFrame.
- Fixed a bug in the implementation of `Column.equal_nan` where null data is handled incorrectly.
- Fixed a bug where `DataFrame.drop` ignore attributes from parent DataFrames after join operations.
- Fixed a bug in mocked function `date_part` where Column type is set wrong.
- Fixed a bug where `DataFrameWriter.save_as_table` does not raise exceptions when inserting null data into non-nullable columns.
- Fixed a bug in the implementation of `DataFrameWriter.save_as_table` where
  - Append or Truncate fails when incoming data has different schema than existing table.
  - Truncate fails when incoming data does not specify columns that are nullable.

#### Improvements

- Removed dependency check for `pyarrow` as it is not used.
- Improved target type coverage of `Column.cast`, adding support for casting to boolean and all integral types.
- Aligned error experience when calling UDFs and stored procedures.
- Added appropriate error messages for `is_permanent` and `anonymous` options in UDFs and stored procedures registration to make it more clear that those features are not yet supported.
- File read operation with unsupported options and values now raises `NotImplementedError` instead of warnings and unclear error information.

## 1.17.0 (2024-05-21)

### Snowpark Python API Updates

#### New Features

- Added support to add a comment on tables and views using the functions listed below:
  - `DataFrameWriter.save_as_table`
  - `DataFrame.create_or_replace_view`
  - `DataFrame.create_or_replace_temp_view`
  - `DataFrame.create_or_replace_dynamic_table`

#### Improvements

- Improved error message to remind users to set `{"infer_schema": True}` when reading CSV file without specifying its schema.

### Snowpark pandas API Updates

#### New Features

- Start of Public Preview of Snowpark pandas API. Refer to the [Snowpark pandas API Docs](https://docs.snowflake.com/developer-guide/snowpark/python/snowpark-pandas) for more details.

### Snowpark Local Testing Updates

#### New Features

- Added support for NumericType and VariantType data conversion in the mocked function `to_timestamp_ltz`, `to_timestamp_ntz`, `to_timestamp_tz` and `to_timestamp`.
- Added support for DecimalType, BinaryType, ArrayType, MapType, TimestampType, DateType and TimeType data conversion in the mocked function `to_char`.
- Added support for the following APIs:
  - snowflake.snowpark.functions:
    - to_varchar
  - snowflake.snowpark.DataFrame:
    - pivot
  - snowflake.snowpark.Session:
    - cancel_all
- Introduced a new exception class `snowflake.snowpark.mock.exceptions.SnowparkLocalTestingException`.
- Added support for casting to FloatType

#### Bug Fixes

- Fixed a bug that stored procedure and UDF should not remove imports already in the `sys.path` during the clean-up step.
- Fixed a bug that when processing datetime format, the fractional second part is not handled properly.
- Fixed a bug that on Windows platform that file operations was unable to properly handle file separator in directory name.
- Fixed a bug that on Windows platform that when reading a pandas dataframe, IntervalType column with integer data can not be processed.
- Fixed a bug that prevented users from being able to select multiple columns with the same alias.
- Fixed a bug that `Session.get_current_[schema|database|role|user|account|warehouse]` returns upper-cased identifiers when identifiers are quoted.
- Fixed a bug that function `substr` and `substring` can not handle 0-based `start_expr`.

#### Improvements

- Standardized the error experience by raising `SnowparkLocalTestingException` in error cases which is on par with `SnowparkSQLException` raised in non-local execution.
- Improved error experience of `Session.write_pandas` method that `NotImplementError` will be raised when called.
- Aligned error experience with reusing a closed session in non-local execution.

## 1.16.0 (2024-05-07)

### New Features

- Support stored procedure register with packages given as Python modules.
- Added snowflake.snowpark.Session.lineage.trace to explore data lineage of snowfake objects.
- Added support for structured type schema parsing.

### Bug Fixes

- Fixed a bug when inferring schema, single quotes are added to stage files already have single quotes.

### Local Testing Updates

#### New Features

- Added support for StringType, TimestampType and VariantType data conversion in the mocked function `to_date`.
- Added support for the following APIs:
  - snowflake.snowpark.functions
    - get
    - concat
    - concat_ws

#### Bug Fixes

- Fixed a bug that caused `NaT` and `NaN` values to not be recognized.
- Fixed a bug where, when inferring a schema, single quotes were added to stage files that already had single quotes.
- Fixed a bug where `DataFrameReader.csv` was unable to handle quoted values containing a delimiter.
- Fixed a bug that when there is `None` value in an arithmetic calculation, the output should remain `None` instead of `math.nan`.
- Fixed a bug in function `sum` and `covar_pop` that when there is `math.nan` in the data, the output should also be `math.nan`.
- Fixed a bug that stage operation can not handle directories.
- Fixed a bug that `DataFrame.to_pandas` should take Snowflake numeric types with precision 38 as `int64`.

## 1.15.0 (2024-04-24)

### New Features

- Added `truncate` save mode in `DataFrameWrite` to overwrite existing tables by truncating the underlying table instead of dropping it.
- Added telemetry to calculate query plan height and number of duplicate nodes during collect operations.
- Added the functions below to unload data from a `DataFrame` into one or more files in a stage:
  - `DataFrame.write.json`
  - `DataFrame.write.csv`
  - `DataFrame.write.parquet`
- Added distributed tracing using open telemetry APIs for action functions in `DataFrame` and `DataFrameWriter`:
  - snowflake.snowpark.DataFrame:
    - collect
    - collect_nowait
    - to_pandas
    - count
    - show
  - snowflake.snowpark.DataFrameWriter:
    - save_as_table
- Added support for snow:// URLs to `snowflake.snowpark.Session.file.get` and `snowflake.snowpark.Session.file.get_stream`
- Added support to register stored procedures and UDxFs with a `comment`.
- UDAF client support is ready for public preview. Please stay tuned for the Snowflake announcement of UDAF public preview.
- Added support for dynamic pivot.  This feature is currently in private preview.

### Improvements

- Improved the generated query performance for both compilation and execution by converting duplicate subqueries to Common Table Expressions (CTEs). It is still an experimental feature not enabled by default, and can be enabled by setting `session.cte_optimization_enabled` to `True`.

### Bug Fixes

- Fixed a bug where `statement_params` was not passed to query executions that register stored procedures and user defined functions.
- Fixed a bug causing `snowflake.snowpark.Session.file.get_stream` to fail for quoted stage locations.
- Fixed a bug that an internal type hint in `utils.py` might raise AttributeError in case the underlying module can not be found.

### Local Testing Updates

#### New Features

- Added support for registering UDFs and stored procedures.
- Added support for the following APIs:
  - snowflake.snowpark.Session:
    - file.put
    - file.put_stream
    - file.get
    - file.get_stream
    - read.json
    - add_import
    - remove_import
    - get_imports
    - clear_imports
    - add_packages
    - add_requirements
    - clear_packages
    - remove_package
    - udf.register
    - udf.register_from_file
    - sproc.register
    - sproc.register_from_file
  - snowflake.snowpark.functions
    - current_database
    - current_session
    - date_trunc
    - object_construct
    - object_construct_keep_null
    - pow
    - sqrt
    - udf
    - sproc
- Added support for StringType, TimestampType and VariantType data conversion in the mocked function `to_time`.

#### Bug Fixes

- Fixed a bug that null filled columns for constant functions.
- Fixed a bug that implementation of to_object, to_array and to_binary to better handle null inputs.
- Fixed a bug that timestamp data comparison can not handle year beyond 2262.
- Fixed a bug that `Session.builder.getOrCreate` should return the created mock session.

## 1.14.0 (2024-03-20)

### New Features

- Added support for creating vectorized UDTFs with `process` method.
- Added support for dataframe functions:
  - to_timestamp_ltz
  - to_timestamp_ntz
  - to_timestamp_tz
  - locate
- Added support for ASOF JOIN type.
- Added support for the following local testing APIs:
  - snowflake.snowpark.functions:
    - to_double
    - to_timestamp
    - to_timestamp_ltz
    - to_timestamp_ntz
    - to_timestamp_tz
    - greatest
    - least
    - convert_timezone
    - dateadd
    - date_part
  - snowflake.snowpark.Session:
    - get_current_account
    - get_current_warehouse
    - get_current_role
    - use_schema
    - use_warehouse
    - use_database
    - use_role

### Bug Fixes

- Fixed a bug in `SnowflakePlanBuilder` that `save_as_table` does not filter column that name start with '$' and follow by number correctly.
- Fixed a bug that statement parameters may have no effect when resolving imports and packages.
- Fixed bugs in local testing:
  - LEFT ANTI and LEFT SEMI joins drop rows with null values.
  - DataFrameReader.csv incorrectly parses data when the optional parameter `field_optionally_enclosed_by` is specified.
  - Column.regexp only considers the first entry when `pattern` is a `Column`.
  - Table.update raises `KeyError` when updating null values in the rows.
  - VARIANT columns raise errors at `DataFrame.collect`.
  - `count_distinct` does not work correctly when counting.
  - Null values in integer columns raise `TypeError`.

### Improvements

- Added telemetry to local testing.
- Improved the error message of `DataFrameReader` to raise `FileNotFound` error when reading a path that does not exist or when there are no files under the path.

## 1.13.0 (2024-02-26)

### New Features

- Added support for an optional `date_part` argument in function `last_day`.
- `SessionBuilder.app_name` will set the query_tag after the session is created.
- Added support for the following local testing functions:
  - current_timestamp
  - current_date
  - current_time
  - strip_null_value
  - upper
  - lower
  - length
  - initcap

### Improvements

- Added cleanup logic at interpreter shutdown to close all active sessions.
- Closing sessions within stored procedures now is a no-op logging a warning instead of raising an error.

### Bug Fixes

- Fixed a bug in `DataFrame.to_local_iterator` where the iterator could yield wrong results if another query is executed before the iterator finishes due to wrong isolation level. For details, please see #945.
- Fixed a bug that truncated table names in error messages while running a plan with local testing enabled.
- Fixed a bug that `Session.range` returns empty result when the range is large.

## 1.12.1 (2024-02-08)

### Improvements

- Use `split_blocks=True` by default during `to_pandas` conversion, for optimal memory allocation. This parameter is passed to `pyarrow.Table.to_pandas`, which enables `PyArrow` to split the memory allocation into smaller, more manageable blocks instead of allocating a single contiguous block. This results in better memory management when dealing with larger datasets.

### Bug Fixes

- Fixed a bug in `DataFrame.to_pandas` that caused an error when evaluating on a Dataframe with an `IntergerType` column with null values.

## 1.12.0 (2024-01-30)

### New Features

- Exposed `statement_params` in `StoredProcedure.__call__`.
- Added two optional arguments to `Session.add_import`.
  - `chunk_size`: The number of bytes to hash per chunk of the uploaded files.
  - `whole_file_hash`: By default only the first chunk of the uploaded import is hashed to save time. When this is set to True each uploaded file is fully hashed instead.
- Added parameters `external_access_integrations` and `secrets` when creating a UDAF from Snowpark Python to allow integration with external access.
- Added a new method `Session.append_query_tag`. Allows an additional tag to be added to the current query tag by appending it as a comma separated value.
- Added a new method `Session.update_query_tag`. Allows updates to a JSON encoded dictionary query tag.
- `SessionBuilder.getOrCreate` will now attempt to replace the singleton it returns when token expiration has been detected.
- Added support for new functions in `snowflake.snowpark.functions`:
  - `array_except`
  - `create_map`
  - `sign`/`signum`
- Added the following functions to `DataFrame.analytics`:
  - Added the `moving_agg` function in `DataFrame.analytics` to enable moving aggregations like sums and averages with multiple window sizes.
  - Added the `cummulative_agg` function in `DataFrame.analytics` to enable commulative aggregations like sums and averages on multiple columns.
  - Added the `compute_lag` and `compute_lead` functions in `DataFrame.analytics` for enabling lead and lag calculations on multiple columns.
  - Added the `time_series_agg` function in `DataFrame.analytics` to enable time series aggregations like sums and averages with multiple time windows.

### Bug Fixes

- Fixed a bug in `DataFrame.na.fill` that caused Boolean values to erroneously override integer values.
- Fixed a bug in `Session.create_dataframe` where the Snowpark DataFrames created using pandas DataFrames were not inferring the type for timestamp columns correctly. The behavior is as follows:
  - Earlier timestamp columns without a timezone would be converted to nanosecond epochs and inferred as `LongType()`, but will now be correctly maintained as timestamp values and be inferred as `TimestampType(TimestampTimeZone.NTZ)`.
  - Earlier timestamp columns with a timezone would be inferred as `TimestampType(TimestampTimeZone.NTZ)` and loose timezone information but will now be correctly inferred as `TimestampType(TimestampTimeZone.LTZ)` and timezone information is retained correctly.
  - Set session parameter `PYTHON_SNOWPARK_USE_LOGICAL_TYPE_FOR_CREATE_DATAFRAME` to revert back to old behavior. It is recommended that you update your code to align with correct behavior because the parameter will be removed in the future.
- Fixed a bug that `DataFrame.to_pandas` gets decimal type when scale is not 0, and creates an object dtype in `pandas`. Instead, we cast the value to a float64 type.
- Fixed bugs that wrongly flattened the generated SQL when one of the following happens:
  - `DataFrame.filter()` is called after `DataFrame.sort().limit()`.
  - `DataFrame.sort()` or `filter()` is called on a DataFrame that already has a window function or sequence-dependent data generator column.
    For instance, `df.select("a", seq1().alias("b")).select("a", "b").sort("a")` won't flatten the sort clause anymore.
  - a window or sequence-dependent data generator column is used after `DataFrame.limit()`. For instance, `df.limit(10).select(row_number().over())` won't flatten the limit and select in the generated SQL.
- Fixed a bug where aliasing a DataFrame column raised an error when the DataFame was copied from another DataFrame with an aliased column. For instance,

  ```python
  df = df.select(col("a").alias("b"))
  df = copy(df)
  df.select(col("b").alias("c"))  # threw an error. Now it's fixed.
  ```

- Fixed a bug in `Session.create_dataframe` that the non-nullable field in a schema is not respected for boolean type. Note that this fix is only effective when the user has the privilege to create a temp table.
- Fixed a bug in SQL simplifier where non-select statements in `session.sql` dropped a SQL query when used with `limit()`.
- Fixed a bug that raised an exception when session parameter `ERROR_ON_NONDETERMINISTIC_UPDATE` is true.

### Behavior Changes (API Compatible)

- When parsing data types during a `to_pandas` operation, we rely on GS precision value to fix precision issues for large integer values. This may affect users where a column that was earlier returned as `int8` gets returned as `int64`. Users can fix this by explicitly specifying precision values for their return column.
- Aligned behavior for `Session.call` in case of table stored procedures where running `Session.call` would not trigger stored procedure unless a `collect()` operation was performed.
- `StoredProcedureRegistration` will now automatically add `snowflake-snowpark-python` as a package dependency. The added dependency will be on the client's local version of the library and an error is thrown if the server cannot support that version.

## 1.11.1 (2023-12-07)

### Bug Fixes

- Fixed a bug that numpy should not be imported at the top level of mock module.
- Added support for these new functions in `snowflake.snowpark.functions`:
  - `from_utc_timestamp`
  - `to_utc_timestamp`

## 1.11.0 (2023-12-05)

### New Features

- Add the `conn_error` attribute to `SnowflakeSQLException` that stores the whole underlying exception from `snowflake-connector-python`.
- Added support for `RelationalGroupedDataframe.pivot()` to access `pivot` in the following pattern `Dataframe.group_by(...).pivot(...)`.
- Added experimental feature: Local Testing Mode, which allows you to create and operate on Snowpark Python DataFrames locally without connecting to a Snowflake account. You can use the local testing framework to test your DataFrame operations locally, on your development machine or in a CI (continuous integration) pipeline, before deploying code changes to your account.

- Added support for `arrays_to_object` new functions in `snowflake.snowpark.functions`.
- Added support for the vector data type.

### Dependency Updates

- Bumped cloudpickle dependency to work with `cloudpickle==2.2.1`
- Updated ``snowflake-connector-python`` to `3.4.0`.

### Bug Fixes

- DataFrame column names quoting check now supports newline characters.
- Fix a bug where a DataFrame generated by `session.read.with_metadata` creates inconsistent table when doing `df.write.save_as_table`.

## 1.10.0 (2023-11-03)

### New Features

- Added support for managing case sensitivity in `DataFrame.to_local_iterator()`.
- Added support for specifying vectorized UDTF's input column names by using the optional parameter `input_names` in `UDTFRegistration.register/register_file` and `functions.pandas_udtf`. By default, `RelationalGroupedDataFrame.applyInPandas` will infer the column names from current dataframe schema.
- Add `sql_error_code` and `raw_message` attributes to `SnowflakeSQLException` when it is caused by a SQL exception.

### Bug Fixes

- Fixed a bug in `DataFrame.to_pandas()` where converting snowpark dataframes to pandas dataframes was losing precision on integers with more than 19 digits.
- Fixed a bug that `session.add_packages` can not handle requirement specifier that contains project name with underscore and version.
- Fixed a bug in `DataFrame.limit()` when `offset` is used and the parent `DataFrame` uses `limit`. Now the `offset` won't impact the parent DataFrame's `limit`.
- Fixed a bug in `DataFrame.write.save_as_table` where dataframes created from read api could not save data into snowflake because of invalid column name `$1`.

### Behavior change

- Changed the behavior of `date_format`:
  - The `format` argument changed from optional to required.
  - The returned result changed from a date object to a date-formatted string.
- When a window function, or a sequence-dependent data generator (`normal`, `zipf`, `uniform`, `seq1`, `seq2`, `seq4`, `seq8`) function is used, the sort and filter operation will no longer be flattened when generating the query.

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
- Fixed a bug where `df.join(..., how="cross")` fails with `SnowparkJoinException: (1112): Unsupported using join type 'Cross'`.
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
- Added keyword argument `overwrite` to `session.write_pandas()` to allow overwriting contents of a Snowflake table with that of a pandas DataFrame.
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

- Added support for vectorized UDFs with the input as a pandas DataFrame or pandas Series and the output as a pandas Series. This improves the performance of UDFs in Snowpark.
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

- Fixed an issue where `Session.createDataFrame(pandas_df)` and `Session.write_pandas(pandas_df)` raise an exception when the `pandas DataFrame` has spaces in the column name.
- `DataFrame.copy_into_table()` sometimes prints an `error` level log entry while it actually works. It's fixed now.
- Fixed an API docs issue where some `DataFrame` APIs are missing from the docs.

### Dependency updates

- Update ``snowflake-connector-python`` to 2.7.2, which upgrades ``pyarrow`` dependency to 6.0.x. Refer to the [python connector 2.7.2 release notes](https://pypi.org/project/snowflake-connector-python/2.7.2/) for more details.

## 0.2.0 (2021-12-02)

### New Features

- Updated the `Session.createDataFrame()` method for creating a `DataFrame` from a pandas DataFrame.
- Added the `Session.write_pandas()` method for writing a `pandas DataFrame` to a table in Snowflake and getting a `Snowpark DataFrame` object back.
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
