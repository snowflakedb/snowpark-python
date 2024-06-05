=============================
Series
=============================

.. currentmodule:: snowflake.snowpark.modin.pandas
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
    Series.empty
    Series.hasnans
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
    Series.squeeze

.. rubric:: Combining / comparing / joining / merging

.. autosummary::
    :toctree: pandas_api/

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

    Series.str
    Series.dt

.. rubric:: :doc:`All supported Series str APIs <supported/series_str_supported>`
.. rubric:: :doc:`All supported Series dt APIs <supported/series_dt_supported>`
