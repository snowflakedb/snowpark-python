=============================
GroupBy
=============================

.. currentmodule:: snowflake.snowpark.modin.pandas.groupby
.. rubric:: :doc:`All supported groupby APIs <supported/groupby_supported>`

.. rubric:: Indexing, iteration

.. autosummary::
    :toctree: pandas_api/

    DataFrameGroupBy.__iter__
    SeriesGroupBy.__iter__
    DataFrameGroupBy.get_group
    DataFrameGroupBy.groups
    SeriesGroupBy.groups
    DataFrameGroupBy.indices
    SeriesGroupBy.indices

.. rubric:: Function application

.. autosummary::
    :toctree: pandas_api/

    DataFrameGroupBy.apply
    DataFrameGroupBy.agg
    SeriesGroupBy.agg
    DataFrameGroupBy.aggregate
    SeriesGroupBy.aggregate
    DataFrameGroupBy.transform

.. rubric:: `DataFrameGroupBy` computations / descriptive stats

.. autosummary::
    :toctree: pandas_api/

    DataFrameGroupBy.count
    DataFrameGroupBy.cumcount
    DataFrameGroupBy.cummax
    DataFrameGroupBy.cummin
    DataFrameGroupBy.cumsum
    DataFrameGroupBy.head
    DataFrameGroupBy.idxmax
    DataFrameGroupBy.idxmin
    DataFrameGroupBy.max
    DataFrameGroupBy.mean
    DataFrameGroupBy.median
    DataFrameGroupBy.min
    DataFrameGroupBy.nunique
    DataFrameGroupBy.quantile
    DataFrameGroupBy.rank
    DataFrameGroupBy.shift
    DataFrameGroupBy.size
    DataFrameGroupBy.std
    DataFrameGroupBy.sum
    DataFrameGroupBy.tail
    DataFrameGroupBy.var

.. rubric:: `SeriesGroupBy` computations / descriptive stats

.. autosummary::
    :toctree: pandas_api/

    SeriesGroupBy.count
    SeriesGroupBy.cumcount
    SeriesGroupBy.cummax
    SeriesGroupBy.cummin
    SeriesGroupBy.cumsum
    SeriesGroupBy.head
    SeriesGroupBy.idxmax
    SeriesGroupBy.idxmin
    SeriesGroupBy.max
    SeriesGroupBy.mean
    SeriesGroupBy.median
    SeriesGroupBy.min
    SeriesGroupBy.nunique
    SeriesGroupBy.quantile
    SeriesGroupBy.rank
    SeriesGroupBy.shift
    SeriesGroupBy.size
    SeriesGroupBy.std
    SeriesGroupBy.sum
    SeriesGroupBy.tail
    SeriesGroupBy.var
