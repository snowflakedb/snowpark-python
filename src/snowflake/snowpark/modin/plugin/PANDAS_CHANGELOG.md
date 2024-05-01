## 1.15.0a1 (tbd)

### Bug Fixes
- Fixed overriding of subclasses' property docstrings for modin issue https://github.com/modin-project/modin/issues/7113.
- Fixed `@udf` decorator when `packages=None` are specified preventing error in `Series.apply`.
- Fixed incorrect regex used in `Series.str.contains`.
- Fixed DataFrame's `__getitem__` with boolean DataFrame key.
- Fixed incorrect regex used in `DataFrame/Series.replace`.
- Fixed AssertionError in `Series.sort_values` after repr and indexing operations.

### Behavior Changes
- Raise not implemented error instead of fallback to pandas in following APIs:
  - `pd.merge`, `DataFrame.merge` and `DataFrame.join` if given the `validate` parameter.
  - `pd.to_numeric` if `error == 'ignore'`.
  - `pd.to_datetime` if `format` is None or not supported in Snowflake or if `exact`, `infer_datetime_format` parameters are given or `origin == 'julian'` or `error == 'ignore'`.
  - `DataFrame/Series.all` if called on non-integer/boolean columns.
  - `DataFrame/Series.any` if called on non-integer/boolean columns.
  - `DataFrame/Series.astype` if casting from string to datetime or `errors == 'ignore'`.
  - `DataFrame/Series.dropna` if `axis == 1`
  - `DataFrame/Series.mask` if given `axis` or `level` parameters.
  - `DataFrame/Series.rename` if `mapper` is callable or the DataFrame/Series has MultiIndex.
  - `DataFrame/Series.sort_values` if given the `key` parameter.
  - `DataFrame/Series.sort_index` if given the `key` parameter.
  - `DataFrame.nunique` if `axis == 1`
  - `DataFrame.apply` if `axis == 0` or `func` is not callable or `result_type` is given or `args` and `kwargs` contain DataFrame or Series.
  - `Series.apply` if `axis == 0` or `func` is not callable or `result_type` is given.
  - `Series.applymap` if `na_action == 'igonre'`.
  - `DataFrame/Series.ffill` if given the `limit` or `downcast` parameter.
  - `DataFrame/Series.fillna` if given the `limit` or `downcast` parameter.
  - `dot` binary operation between `DataFrame/Series`.
  - `xor` binary operation between `DataFrame/Series`.
  - All `DataFrame/Series.groupby` operations if either `axis == 1`, both `by` and `level` are configured, or `by` contains any non-pandas hashable labels.

### Behavior Changes
- As a part of the transition to pandas 2.2.1, pandas `df.loc` and `__setitem__` have buggy behavior when a column key is used to assign a DataFrame item to a DataFrame (a scalar column key and DataFrame item are used for assignment (https://github.com/pandas-dev/pandas/issues/58482)). Snowpark pandas deviates from this behavior and will maintain the same behavior as pandas from versions 1.5.x.

### New Features
- Added support for `pd.NamedAgg` in `DataFrameGroupBy.agg` and `SeriesGroupBy.agg`.

## 1.14.0a2 (2024-04-18)

### Behavior Changes
- The `to_pandas` operation converts all integers to int64, instead of int8, int16 etc. To get an exact type, the user needs to explicitly specify precision values for their Snowflake column. This is a general behavior change across all of Snowpark.
- The following API changes are made to align Snowpark pandas with the pandas 2.2.1 API:
  - Updated DateOffset strings to pandas 2.2.1 versions.
  - As part of this transition, we have a set of transitional API and test bugs:
    - SNOW-1320623, SNOW-1321196 - pandas `df.loc` and `__setitem__` have buggy behavior when:
      - the column key has duplicates in a specific manner (https://github.com/pandas-dev/pandas/issues/58317), or
      - a new row and column are used in the row and column keys (https://github.com/pandas-dev/pandas/issues/58316).
      Snowpark pandas deviates from this behavior and will maintain the same behavior as pandas from versions 2.1.x.
    - SNOW-1320660 - `qcut` / `cut` with bin preparation is temporarily NotImplemented due to upstream changes.
    - SNOW-1321662 - `merge` fails when join is outer and sort is False.
    - SNOW-1321682 - `df.melt` w/ duplicated cols.
    - SNOW-1318223 - `series.py::_flex_method` list-like other (`pd.Index`) may not be supported in pandas now.
    - SNOW-1321719 - `test_bitwise_operators.py` xfails.
- Changed the dtype of the index of empty `DataFrame` and `Series` to be `int64` rather than `object` to match the behavior of pandas.
- Changed the import path of Snowpark pandas package to use Modin 0.28.1 instead. The new recommended import statement is `import modin.pandas as pd; import snowflake.snowpark.modin.plugin`.

### New Features
- Added support for `axis` argument for `df.where` and `df.mask` when `other` is a Series.
- Added back `_repr_html_` to DataFrame class for pretty printing (partially reverts commit 576ba26586caca3fa063da1fed465c61091b6d9c).
- Added support for `DataFrameGroupBy.nunique`.

## 1.14.0a1 (2024-04-11)

### Behavior Changes
- The following API changes are made to align Snowpark pandas with the pandas 2.1.4 API:
  - Removed `errors` and `try_cast` arguments from `DataFrame`/`Series.where` and `mask`.
  - Added the `dtype_backend` argument to `DataFrame`/`Series.convert_dtypes`; this argument is ignored by Snowpark pandas and only exists for compatibility.
  - Removed `is_copy` from `DataFrame`/`Series.take`.
  - Removed `squeeze` argument from `DataFrame`/`Series.groupby`. Changed the default value of `group_keys` to `True`, and `observed` to `no_default`.
  - Limited the length of generated labels and identifiers to 32 characters
  - Removed the `squeeze`, `prefix`, `mangle_dupe_cols`, `error_bad_lines`, and `warn_bad_lines` arguments from `pd.read_csv`. These were previously unsupported by Snowpark pandas, and existed only for compatibility.
  - Renamed the `skip_initial_space` argument in `pd.read_csv` to `skipinitialspace`; it remains unsupported and will raise an error if specified.
  - Added the `date_format` and `dtype_backend` arguments in `pd.read_csv`. These are currently unsupported and added only for compatibility. `dtype_backend` is ignored and will raise a warning if provided, and `date_format` will raise an error.
  - Added the `dtype_backend`, `filesystem`, and `filters` arguments in `pd.read_parquet`. These are currently unsupported and added only for compatibility. `dtype_backend` is ignored and will raise a warning if provided, and `filesystem` and `filters` will raise an error.
  - Removed the `numpy` argument from `pd.read_json`. This was previously unsupported, and existed only for compatibility.
  - Added the `dtype_backend` and `engine` arguments to `pd.read_json`. These are currently unsupported and added only for compatibility; they are ignored and will raise a warning if provided.

- The following methods are removed:
  - `DataFrame`/`Series.append`
  - `Series.is_monotonic`

### New Features
- Added support for `pd.cut` with `retbins=False` and `labels=False`.
- Added support for `Series.str.strip`.
- Added support for `Series.str.len`.
- Added support for `Series.str.capitalize`.
- Added support for `DataFrame.apply` and `Series.apply` to work with `@udf` decorated functions to allow working with package dependencies.
- Added support for `DataFrameGroupBy.transform`.
- Added support for `DataFrame.idxmax`, `DataFrame.idxmin`, `Series.idxmax`, and `Series.idxmin`.
- Added support for `Series.str.replace`.
- Added support for `Series.str.split`.
- Added support for `Series.str.title` and `Series.str.istitle`.
- Added support for `np.where`, `np.logical_*`, and `np.add` operators via `__array_ufunc__` and `__array_function__`.
- Added support for `DataFrameGroupby.head` and `DataFrameGroupBy.tail`.
- Added support for `DataFrameGroupBy.idxmax` and `DataFrameGroupBy.idxmin` for `GroupBy` `axis = 0`.
- Updated to `snowpark-python` v1.14.0.
- Updated to `pandas` 2.2.1 from 2.1.4.
- Added support for `axis` argument for `df.where` and `df.mask` when `other` is a Series.

### Bug Fixes
- Fixed broadcast when masking a DataFrame with a Series using `df.where` or `df.mask`.
- Error out when scalar is passed for condition to DataFrame/Series `where` or `mask`.
- Fixed property docstring generation for some classes that use the telemetry metaclass.
- Fixed an issue where creating a Snowpark pandas DataFrame from a Series with a tuple `name`, such as `pd.DataFrame(pd.Series(name=("A", 1)))`, did not create `MultiIndex` columns on the resulting frame.
- Added custom docstrings inplace to avoid module reload errors.
- Added a separate docstring class for BasePandasDataset.
- Fixed docstring overrides for subclasses.

## 1.13.0a1 (2024-03-15)
### Dependency Updates
- Upgraded `pandas` from 1.5.3 to 2.1.4.

### Behavior Changes
- Removed support for Python 3.8, as pandas 2.1.4 no longer supports this version. Please upgrade your environment to use Python 3.9 or newer.
- The following API changes are made as a result of moving from pandas 1.5.3 to 2.1.4:
  - Removed sized index types like `pd.Int64Index` and `pd.Float64Index`. Index objects are now explicitly constructed with a dtype parameter.
  - Changed the default dtype of an empty Series from `float64` to `object`.
  - Changed the default value of `numeric_only` to `False` for many operations. Previously, setting `numeric_only=None` would automatically drop non-numeric columns from a frame if possible; this behavior has been removed, and setting `numeric_only=None` gives the same behavior as `numeric_only=False`.
  - Removed the `level` parameter from aggregation functions (`sum`, `prod`, `count`, `any`, etc.).
  - Removed `Series.append`. Use `pd.concat` instead.
  - Removed the `inplace` parameter for `set_axis`.
  - Removed the `datetime_is_numeric` parameter for `describe`. All datetime data is now treated as numeric.
  - Removed the `loffset` and `base` parameters for `resample` and `Grouper`. Use `offset` and `origin` instead.
  - Added a name to the output of `value_counts`. The returned series will be named `count` when `normalize=False`, and `proportion` when `normalize=True`.
- The following errors have changed as a result of moving from pandas 1.5.3 to 2.1.4:
  - When attempting to call `DataFrame.aggregate` with a dict where a label has no associated functions (e.g. `df.aggregate({0: []})`), the error message has changed from "no result" to "No objects to concatenate."
  - Calling aggregation methods with `numeric_only=True` on non-numeric `Series` objects raises a `TypeError`.
  - Calling `DataFrame.aggregate` or `Series.aggregate` with a list of aggregations will not raise an error when invalid keyword arguments are passed. For example, `df.agg(["count"], invalid=0)` will not raise an error in Snowpark pandas even though "invalid" is not a valid argument to the `count` aggregation.
  - Calling `GroupBy.shift` with a non-integer value for the `periods` argument now always raises a `TypeError`. Previously, floating point values that happened to be integers (like `2.0` or `-2.0`) were valid.
  - Stopped automatically creating a Snowpark session when there is no active Snowpark session. Instead, Snowpark pandas requires a unique active Snowpark session.

### New Features
- Added `"quantile"` as a valid aggregation in `DataFrame.agg` and `Series.agg`.
- Added support for binary operations between `DataFrame`/`Series` and `Series`/`DataFrame` along `axis=1`.
- Added support for binary operations between a `Series` or `DataFrame` object and a list-like object for `axis=1`.
- Added support for `DataFrame.round` and `Series.round`.
- Added support for `df.melt` and `pd.melt`
- Added support for binary operations between two `DataFrame` objects.
- Added support for `DataFrame.sort_index` and `Series.sort_index` along `axis=0`.
- Added support for `DataFrame.skew` and `Series.skew` along `axis=0`
- Added support for reading `SELECT` SQL Queries into a `DataFrame` object via `pd.read_snowflake` and changed `name` argument of `pd.read_snowflake` to `name_or_query`.
- Added support for `Series.str.startswith` and `Series.str.endswith`.
- Added support for reading SQL Queries with CTEs and CTEs with anonymous stored procedures into a `DataFrame` object via `pd.read_snowflake`.
- Added support for `DataFrame.first_valid_index`, `DataFrame.last_valid_index`, `Series.first_valid_index`, and `Series.last_valid_index`.
- Added support for `DataFrame.ffill`, `DataFrame.pad`, `Series.ffill`, and, `Series.pad`.
- Added support for reading `CALL SQL` Queries into a `DataFrame` object via `pd.read_snowflake`.
- Added support for `Series.str.lower` and `Series.str.upper`.
- Added support for `Series.str.isdigit`, `Series.str.islower`, and `Series.str.isupper`.
- Added partial support for `DataFrameGroupBy.apply` on `axis=0`, for `func` returning a `DataFrame`.
- Added partial support for `DataFrameGroupBy.apply` on `axis=0`, for `func` returning an object that is neither a DataFrame nor a Series.
- Added support for `Series.groupby.cumcount`, `Series.groupby.cummax`, `Series.groupby.cummin`,  and `Series.groupby.cumsum`.
- Added support for `DataFrame.groupby.cumcount`, `DataFrame.groupby.cummax`, `DataFrame.groupby.cummin`,  and `DataFrame.groupby.cumsum`.
- Added support for `pd.qcut` with `retbins=False`.
- Added support for `Series.str.contains` and `Series.str.count`.
- Added partial support for `DataFrameGroupBy.apply` on `axis=0`, for `func` always returning a `Series` with the same index and same name.
- Added support for `DataFrameGroupBy.rank` and `SeriesGroupBy.rank`.

### Bug Fixes
- Allowed getting the Snowpark pandas session before creating a Snowpark pandas Dataframe or Series.
- Fixed an issue when using `pd.read_snowflake` together with `apply(..., axis=1)` where the row position column could not be disambiguated.
- Fixed the exception that you get when accessing a missing attribute of the Snowpark pandas module.
- Using dataframe or series apply(axis=1) when there are multiple sessions no longer raises an exception.
- Added docstring and doctests to correctly reflect difference between Snowpark pandas and native pandas functionality for `get` method.

### Improvements
- Improved performance for `DataFrame.apply` and `Series.apply` for `axis=1` for functions passed without type hints by micro-batching rows.
- Restructure Snowpark pandas documentation

## 1.12.1a1 (2024-02-20)

### New Features
- Added support for `DataFrame.cummin`, `DataFrame.cummax`, `DataFrame.cumsum`, `Series.cummin`, `Series.cummax`, and `Series.cumsum`.
- Added support for `groups` and `indices` properties of `groupby` object.
- Added support for `DataFrame.add_prefix`, `DataFrame.add_suffix`, `Series.add_prefix`, and `Series.add_suffix`.
- Added support for `DataFrame.rolling` and `Series.rolling` on `axis=0` with integer `window`, `min_periods>=1`, and `center` for aggregations `min`, `max`, `sum`, `mean`, `var`, and `std`.
- Added support for `DataFrame.rank` and `Series.rank` with `pct=True`.
- Added support for `pd.date_range`.
- Added support for the `fill_value` parameter in binary operations.
- Added support for `Dataframe.duplicated` and `Series.duplicated`.
- Added support for `Dataframe.drop_duplicates` and `Series.drop_duplicates`.
- Added support for binary operations between `DataFrame` and `Series` (and vice-versa).
- Added support for binary operations between a `Series` or `DataFrame` object and a list-like object for `axis=0`.

### Behavior Changes
- Deprecated support for Python 3.8. A future release will upgrade the `pandas` version to 2.1.4, which no longer supports Python 3.8. Users should upgrade Python to 3.9 or later.

### Improvements
- Added cleanup logic at interpreter shutdown to close all active sessions.
- Improved performance for `DataFrame.apply` for `axis=1` by relying on Snowflake vectorized UDFs instead of vectorized UDTFs together with dynamic pivot.

### Bug Fixes
- Fixed bug for `loc` when the index is unordered and the key is a slice with reversed order.
- Fixed bug for `pd.get_dummies` when input has been sorted, or just read from Snowflake.

## 1.12.0a1 (2024-02-02)

### Improvements
- Enabled telemetry for several private methods, e.g., `__getitem__` and `__setitem__`.
- Removed `to_numeric` length check.
- Added parameter type validation for aggregation, includes numeric_only, skipna and min_count.
- Changed `to_pandas` to return decimal numbers as `float64` instead of `object`  based on Snowpark 1.12 release.

### Bug Fixes
- Fixed bug where `loc` get on multiindex prefix matching.
- Removed the `modin.pandas.Session` reference to the Snowpark Session class.
- Removed unnecessary coalescing of join keys for left, right and inner join/merge.

### New Features
- Added support for `DataFrame.diff` and `Series.diff`.
- Added support for `DataFrame.groupby.shift` and `Series.groupby.shift`
- Added support for `DataFrame.quantile` and `Series.quantile`
- Added support for `min`, `max`, `count`, and `sum` aggregations with `axis=1`.
- Added support for `DataFrame.resample` and `Series.resample` for aggregations: `median`, `sum`, `std`, `var`, `count`.
- Added support for binary operations with `pd.DateOffset` where offset is treated as a timedelta.
- Added support for `DataFrame.fillna` where `value` is a dataframe or `Series.fillna` where `value` is a series or dict.
- Added support for `DataFrame.isin`.
- Added support for `pd.get_dummies` for DataFrames and Series if params `dummy_na`, `drop_first` and `dtype` take default values.
- Added support for `groupby` with `sum`, `DataFrame.sum`, and `Series.sum` for string-typed data.
- Added support for `DataFrame.select_dtypes`.
- Added support for partial string indexing for `DatetimeIndex`.
- Added support for `DataFrame.iterrows` and `DataFrame.itertuples`.
- Added support for `DataFrame.sample` and `Series.sample`.
- Added support for `DataFrame.shift` and `Series.shift` with `axis=0,1`, `fill_value` and `periods`.
- Added support for `DataFrame.rank` and `Series.rank`.
- Added support for `DataFrame.describe` and `Series.describe`.
- Added support for `DataFrame.replace` and `Series.replace`.

### Bug Fixes
- Fixed bug when `apply` has been called multiple times.
- Fixed bug where `loc` with slice key on a single row dataframe or series.
- Fixed bug where `series.reset_index` triggers eager evaluation.

## 1.11.1a1 (2023-12-21)

### Improvements
- Improved performance of `transpose` by removing unnecessary count queries.
- Raised NotImplementedError where setting cell with list like values.
- Reduced the number of queries for `iloc` get with scalar row key
- Improved performance of `insert` by removing count query.
- Improved performance of displaying Dataframe/Series in notebook. As part of this improvement we also removed support for html representation for DataFrames.
- Enabled SQL simplifier.
- Started warning users about all fallbacks to pandas via stored procedures.

### Bug Fixes
- Fixed bug when `setitem`/`loc` on empty frame returns wrong result.
- Fixed bug where telemetry message can be duplicated.

## 1.10.0a1 (2023-12-13)

### New Features
- Added support for setting the Snowpark session for Snowpark pandas DataFrame/Series, via `snowflake.snowpark.modin.pandas.session`.
- Added support for `ngroups` on `groupby` object.
- Added support for `Series.set_axis()` and `DataFrame.set_axis()`.
- Added support for `Series.dt.month`, `Series.dt.year`, `Series.dt.day` and `Series.dt.quarter`.
- Added support for `DataFrame.transform` with string and callable parameters.
- Added support for `DataFrame.abs`, `Series.abs`, `DataFrame.__neg__` and `Series.__neg__`.
- Added support for `df.resample` and `ser.resample`. Supported resample bins are: `T`, `S`, `H`, and `D`. Supported aggregations are: `max`, `min`, and `mean`.
- Added support for `pd.read_parquet` using Snowflake `COPY INTO` SQL command.
- Added support for `pd.read_json` using Snowflake `COPY INTO` SQL command.
- Added support for `DataFrame.value_counts` and `Series.value_counts`.
- Added support for `DataFrame.all`, `Series.all`, `Dataframe.any` and `Series.any` for integer
- Added support for `Series.mask()` and `DataFrame.mask()`.
- Added support for `ffill` on `df.resample`.
- Added support for `method` parameter of `DataFrame.fillna()` and `Series.fillna()`.

### Improvements
- Updated with changes from snowpark-python 1.8.0 release.
- Rewrote and improved `.iloc` get using single query with lazy evaluation.
- Improved warning messages from `.to_datetime`.
- Improved `.to_datetime` to avoid unnecessary eager evaluation.
- Improved performance for fallback execution, i.e., running unsupported pandas APIs using stored procedures.
- Rewrote and improved `.loc` get using single query with lazy evaluation.
- Rewrote and improved `.loc` set using single query with lazy evaluation.
- Changed the implementation of `pd.read_csv` to use Snowflake `COPY INTO` SQL command instead of locally executing pandas `read_csv`.
- Improved performance of groupby by removing unnecessary count queries.
- Raise NotImplementedError for pivot_table when no index configured.
- memory_usage() will not return an error, but it will return '0' for all values.
- Rewrote and improved `__getitem__` using single query with lazy evaluation.
- Rewrote and improved `__setitem__` using single query with lazy evaluation.
- Improved performance of aggregate functions by reducing query count.

### Bug Fixes
- Fixed a bug where binary operations between series with duplicate index values produces wrong result.
- Fixed a bug for `fillna` where the fill value is not supposed to be applied to index columns, and also stay consistent with Snowflake type system without explicit casting to variant.
- Fixed a bug where non-homogenous columns or indices were not converted correctly in `to_pandas`.

### Changes
- Error out when unsupported aggregation function is used.

## 1.7.0a4 (2023-10-10)
- Improved warning messages from `.to_datetime`

### New Features
- Added support for `DataFrame.to_dict` and `series.to_dict`.
- Added support for `DataFrame.take` and `series.take`.
- Added support for `pd.Series.isin`

### Improvements
- Rewrote and improved `.iloc` get with series key using single join query with lazy evaluation.
- Updated docstring for `DataFrame.sort_values` and `Series.sort_value` APIs.
- Updated docstring for `DataFrame.reset_index` and `Series.reset_index` APIs.
- Removed unnecessary client side check and fallback for aggregation.

### Bug Fixes
- Fixed a bug where `.loc` and `.iloc` handle column indexers

## 1.7.0a3 (2023-10-04)

### New Features
- Added support for `Series.dt.date`, `Series.dt.hour`, `Series.dt.minute` and `Series.dt.second`.

### Bug Fixes
- Fixed a bug where `DataFrame.dropna` used the original row position as new row positions after rows were dropped.
- Fixed a bug where `.loc` uses a string as the column key.
- Fixed a bug where `.iloc` pulls series key's index to client.
- Fixed a bug where `DataFrame.join` calls `to_pandas()` unexpected.
- Fixed a bug where some unsupported APIs didn't raise NotImplementedError.
- Fixed a bug where binary operation `pow`, `rpow`, `__and__`, `__rand__`, `__or__`, `__ror__`, `__xor__`, and `__rxor__` calls frontend `default_to_pandas`.
- Fixed a bug where creating DataFrame from shared database fails.

## 1.7.0a2 (2023-09-20)

### New Features
- Added support for `pd.read_csv` by reading csv files on the client then uploading data to Snowflake.
- Added support for binary arithmetic and comparison operators between series.
- Added support for `pd.unique`.

### Improvements
- Improved performance for `head`, `tail`, `_repr_html_`, `loc`, `iloc`, `__getitem__` and `__setitem__`, `__repr__`.
- Improved API documents for Snowpark pandas IO methods.
- Improved error messages when using Snowpark pandas API with multiple Snowpark sessions.
- Improved type conversion performance (from string to datetime).

### Bug Fixes
- Fixed a bug where an extra temp table was incorrectly created while using `pd.read_snowflake` to read a regular Snowflake table.
- Fixed a bug where `df.pivot_table` failed when the original dataframe is created from large local data.
- Fixed a bug when creating a Snowpark pandas DataFrame/Series from local numpy data that is not json-serializable.
- Fixed a bug where `df.apply`, `series.apply` and `df.applymap` incorrectly convert SQL nulls to JSON nulls in Snowflake Variant data.
- Fixed a bug where aggregation functions with `groupby` did not work on decimal columns.
- Fixed a bug where the output `_repr_html_` and `__repr__` did not match pandas behavior.

## 1.7.0a1 (2023-09-15)

Start of Private Preview
