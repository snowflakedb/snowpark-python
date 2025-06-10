=============================
Series
=============================

.. currentmodule:: modin.pandas
.. rubric:: :doc:`All supported Series APIs <supported/series_supported>`

.. rubric:: Constructor

.. autosummary::
    :toctree: pandas_api/

    Series

.. rubric:: Attributes

.. autosummary::
    :toctree: pandas_api/

    Series.index
    Series.axes
    Series.array
    Series.dtype
    Series.dtypes
    Series.duplicated
    Series.equals
    Series.empty
    Series.hasnans
    Series.is_monotonic_increasing
    Series.is_monotonic_decreasing
    Series.name
    Series.ndim
    Series.shape
    Series.size
    Series.T
    Series.values


.. rubric:: Snowflake Specific

.. autosummary::
    :toctree: pandas_api/

    Series.to_snowflake
    Series.to_snowpark
    Series.cache_result
    Series.create_or_replace_view
    Series.create_or_replace_dynamic_table
    Series.to_view
    Series.to_dynamic_table
    Series.to_iceberg
    Series.get_backend
    Series.set_backend
    Series.move_to
    Series.pin_backend
    Series.unpin_backend

.. rubric:: Conversion

.. autosummary::
    :toctree: pandas_api/

    Series.astype
    Series.convert_dtypes
    Series.copy
    Series.to_dict
    Series.to_list
    Series.to_numpy
    Series.to_pandas
    Series.__array__


.. rubric:: Indexing, iteration

.. autosummary::
    :toctree: pandas_api/

    Series.iloc
    Series.items
    Series.loc
    Series.__iter__
    Series.keys



.. rubric:: Binary operator functions

.. autosummary::
    :toctree: pandas_api/

    Series.add
    Series.sub
    Series.mul
    Series.div
    Series.truediv
    Series.floordiv
    Series.mod
    Series.pow
    Series.radd
    Series.rsub
    Series.rmul
    Series.rdiv
    Series.rtruediv
    Series.rfloordiv
    Series.rmod
    Series.rpow
    Series.round
    Series.lt
    Series.gt
    Series.le
    Series.ge
    Series.ne
    Series.eq

.. rubric:: Function application, GroupBy & window

.. autosummary::
    :toctree: pandas_api/

    Series.apply
    Series.agg
    Series.aggregate
    Series.transform
    Series.map
    Series.groupby
    Series.rolling





.. rubric:: Computations / descriptive stats

.. autosummary::
    :toctree: pandas_api/

    Series.abs
    Series.all
    Series.any
    Series.count
    Series.cummax
    Series.cummin
    Series.cumsum
    Series.describe
    Series.diff
    Series.max
    Series.mean
    Series.median
    Series.min
    Series.pct_change
    Series.quantile
    Series.rank
    Series.skew
    Series.std
    Series.sum
    Series.var
    Series.unique
    Series.nunique
    Series.is_unique
    Series.value_counts


.. rubric:: Reindexing / selection / label manipulation

.. autosummary::
    :toctree: pandas_api/

    Series.case_when
    Series.drop
    Series.drop_duplicates
    Series.get
    Series.head
    Series.idxmax
    Series.idxmin
    Series.isin
    Series.last
    Series.rename
    Series.rename_axis
    Series.reset_index
    Series.sample
    Series.set_axis
    Series.take
    Series.tail
    Series.where
    Series.mask
    Series.add_prefix
    Series.add_suffix



.. rubric:: Missing data handling

.. autosummary::
    :toctree: pandas_api/

    Series.backfill
    Series.bfill
    Series.dropna
    Series.ffill
    Series.fillna
    Series.isna
    Series.isnull
    Series.notna
    Series.notnull
    Series.pad
    Series.replace

.. rubric:: Reshaping, sorting

.. autosummary::
    :toctree: pandas_api/

    Series.sort_values
    Series.sort_index
    Series.unstack
    Series.nlargest
    Series.nsmallest
    Series.squeeze

.. rubric:: Combining / comparing / joining / merging

.. autosummary::
    :toctree: pandas_api/

    Series.compare
    Series.update

.. rubric:: Time Series-related

.. autosummary::
    :toctree: pandas_api/

    Series.shift
    Series.first_valid_index
    Series.last_valid_index
    Series.resample


.. rubric:: Accessors

.. autosummary::
    :toctree: pandas_api/
    :template: autosummary/modin_accessor.rst

    Series.str
    Series.dt


.. rubric:: Datetime accessor properties

:doc:`All supported Series dt APIs <supported/series_dt_supported>`

.. autosummary::
    :toctree: pandas_api/
    :template: autosummary/modin_accessor_attribute.rst

    Series.dt.date
    Series.dt.time
    Series.dt.year
    Series.dt.month
    Series.dt.day
    Series.dt.hour
    Series.dt.minute
    Series.dt.second
    Series.dt.microsecond
    Series.dt.nanosecond
    Series.dt.dayofweek
    Series.dt.day_of_week
    Series.dt.weekday
    Series.dt.dayofyear
    Series.dt.day_of_year
    Series.dt.days_in_month
    Series.dt.daysinmonth
    Series.dt.quarter
    Series.dt.isocalendar
    Series.dt.month_name
    Series.dt.day_name
    Series.dt.is_month_start
    Series.dt.is_month_end
    Series.dt.is_quarter_start
    Series.dt.is_quarter_end
    Series.dt.is_year_start
    Series.dt.is_year_end
    Series.dt.is_leap_year
    Series.dt.floor
    Series.dt.ceil
    Series.dt.round
    Series.dt.normalize
    Series.dt.days
    Series.dt.seconds
    Series.dt.microseconds
    Series.dt.nanoseconds
    Series.dt.tz_convert
    Series.dt.tz_localize


.. rubric:: String accessor methods

:doc:`All supported Series str APIs <supported/series_str_supported>`

.. autosummary::
    :toctree: pandas_api/
    :template: autosummary/modin_accessor_method.rst

    Series.str.capitalize
    Series.str.casefold
    Series.str.center
    Series.str.contains
    Series.str.count
    Series.str.endswith
    Series.str.get
    Series.str.isdigit
    Series.str.islower
    Series.str.istitle
    Series.str.isupper
    Series.str.len
    Series.str.lower
    Series.str.lstrip
    Series.str.match
    Series.str.replace
    Series.str.rstrip
    Series.str.slice
    Series.str.split
    Series.str.startswith
    Series.str.strip
    Series.str.translate
    Series.str.upper

.. rubric:: Serialization / IO / conversion

.. autosummary::
    :toctree: pandas_api/

    Series.to_csv
    Series.to_string
