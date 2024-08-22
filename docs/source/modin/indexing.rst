=============================
Index objects
=============================

Index
-----
.. currentmodule:: snowflake.snowpark.modin.pandas
.. rubric:: :doc:`All supported Index APIs <supported/index_supported>`

.. rubric:: Constructor

.. autosummary::
    :toctree: pandas_api/

    Index

.. rubric:: Properties

.. autosummary::
    :toctree: pandas_api/

    Index.values
    Index.is_monotonic_increasing
    Index.is_monotonic_decreasing
    Index.is_unique
    Index.has_duplicates
    Index.hasnans
    Index.dtype
    Index.shape
    Index.name
    Index.names
    Index.ndim
    Index.size
    Index.empty
    Index.T
    Index.nlevels

.. rubric:: Snowflake Specific

.. autosummary::
    :toctree: pandas_api/

    Index.to_pandas

.. rubric:: Modifying and computations

.. autosummary::
    :toctree: pandas_api/

    Index.all
    Index.any
    Index.argmin
    Index.argmax
    Index.copy
    Index.delete
    Index.drop
    Index.drop_duplicates
    Index.duplicated
    Index.equals
    Index.identical
    Index.insert
    Index.is_boolean
    Index.is_floating
    Index.is_integer
    Index.is_numeric
    Index.is_object
    Index.item
    Index.min
    Index.max
    Index.reindex
    Index.rename
    Index.unique
    Index.nunique
    Index.value_counts
    Index.array
    Index.str

.. rubric:: Compatibility with MultiIndex

.. autosummary::
    :toctree: pandas_api/

    Index.set_names

.. rubric:: Missing values

.. autosummary::
    :toctree: pandas_api/

    Index.fillna
    Index.dropna
    Index.isna
    Index.notna


.. rubric:: Conversion

.. autosummary::
    :toctree: pandas_api/

    Index.astype
    Index.item
    Index.to_list
    Index.tolist
    Index.to_series
    Index.to_frame

.. rubric:: Sorting

.. autosummary::
    :toctree: pandas_api/

    Index.sort_values

.. rubric:: Combining / joining / set operations

.. autosummary::
    :toctree: pandas_api/

    Index.append
    Index.join
    Index.intersection
    Index.union
    Index.difference

.. rubric:: Selecting

.. autosummary::
    :toctree: pandas_api/

    Index.get_indexer_for
    Index.get_level_values
    Index.isin
    Index.slice_indexer

.. _api.datetimeindex:

DatetimeIndex
-------------

.. autosummary::
   :toctree: pandas_api/

   DatetimeIndex

.. rubric:: `DatetimeIndex` Time/date components

.. autosummary::
    :toctree: pandas_api/

    DatetimeIndex.year
    DatetimeIndex.month
    DatetimeIndex.day
    DatetimeIndex.hour
    DatetimeIndex.minute
    DatetimeIndex.second
    DatetimeIndex.microsecond
    DatetimeIndex.nanosecond
    DatetimeIndex.date
    DatetimeIndex.time
    DatetimeIndex.timetz
    DatetimeIndex.dayofyear
    DatetimeIndex.day_of_year
    DatetimeIndex.dayofweek
    DatetimeIndex.day_of_week
    DatetimeIndex.weekday
    DatetimeIndex.quarter
    DatetimeIndex.tz
    DatetimeIndex.freq
    DatetimeIndex.freqstr
    DatetimeIndex.is_month_start
    DatetimeIndex.is_month_end
    DatetimeIndex.is_quarter_start
    DatetimeIndex.is_quarter_end
    DatetimeIndex.is_year_start
    DatetimeIndex.is_year_end
    DatetimeIndex.is_leap_year
    DatetimeIndex.inferred_freq

.. rubric:: `DatetimeIndex` Selecting

.. autosummary::
    :toctree: pandas_api/

    DatetimeIndex.indexer_at_time
    DatetimeIndex.indexer_between_time

.. rubric:: `DatetimeIndex` Time-specific operations

.. autosummary::
    :toctree: pandas_api/

    DatetimeIndex.normalize
    DatetimeIndex.strftime
    DatetimeIndex.snap
    DatetimeIndex.tz_convert
    DatetimeIndex.tz_localize
    DatetimeIndex.round
    DatetimeIndex.floor
    DatetimeIndex.ceil
    DatetimeIndex.month_name
    DatetimeIndex.day_name

.. rubric:: `DatetimeIndex` Conversion

.. autosummary::
    :toctree: pandas_api/

    DatetimeIndex.as_unit
    DatetimeIndex.to_period
    DatetimeIndex.to_pydatetime
    DatetimeIndex.to_series
    DatetimeIndex.to_frame

.. rubric:: `DatetimeIndex` Methods

.. autosummary::
    :toctree: pandas_api/

    DatetimeIndex.mean
    DatetimeIndex.std
