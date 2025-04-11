=============================
DataFrame
=============================

.. currentmodule:: modin.pandas
.. rubric:: :doc:`All supported DataFrame APIs <supported/dataframe_supported>`

.. rubric:: Constructor

.. autosummary::
    :toctree: pandas_api/

    DataFrame

.. rubric:: Attributes

.. autosummary::
    :toctree: pandas_api/

    DataFrame.index
    DataFrame.columns
    DataFrame.dtypes
    DataFrame.info
    DataFrame.select_dtypes
    DataFrame.values
    DataFrame.axes
    DataFrame.ndim
    DataFrame.size
    DataFrame.shape
    DataFrame.empty

.. rubric:: Snowflake Specific

.. autosummary::
    :toctree: pandas_api/

    DataFrame.to_pandas
    DataFrame.to_snowflake
    DataFrame.to_snowpark
    DataFrame.cache_result
    DataFrame.create_or_replace_view
    DataFrame.create_or_replace_dynamic_table
    DataFrame.to_view
    DataFrame.to_dynamic_table

.. rubric:: Conversion

.. autosummary::
    :toctree: pandas_api/

    DataFrame.astype
    DataFrame.convert_dtypes
    DataFrame.copy

.. rubric:: Indexing, iteration

.. autosummary::
    :toctree: pandas_api/

    DataFrame.assign
    DataFrame.head
    DataFrame.loc
    DataFrame.iloc
    DataFrame.insert
    DataFrame.__iter__
    DataFrame.keys
    DataFrame.iterrows
    DataFrame.items
    DataFrame.itertuples
    DataFrame.tail
    DataFrame.isin
    DataFrame.where
    DataFrame.mask

.. rubric:: Binary operator functions

.. autosummary::
    :toctree: pandas_api/

    DataFrame.add
    DataFrame.sub
    DataFrame.mul
    DataFrame.div
    DataFrame.truediv
    DataFrame.floordiv
    DataFrame.mod
    DataFrame.pow
    DataFrame.radd
    DataFrame.rsub
    DataFrame.rmul
    DataFrame.rdiv
    DataFrame.rtruediv
    DataFrame.rfloordiv
    DataFrame.rmod
    DataFrame.rpow
    DataFrame.lt
    DataFrame.gt
    DataFrame.le
    DataFrame.ge
    DataFrame.ne
    DataFrame.eq

.. rubric:: Function application, GroupBy & window

.. autosummary::
    :toctree: pandas_api/

    DataFrame.apply
    DataFrame.applymap
    DataFrame.agg
    DataFrame.aggregate
    DataFrame.transform
    DataFrame.groupby
    DataFrame.rolling

.. rubric:: Computations / descriptive stats

.. autosummary::
    :toctree: pandas_api/

    DataFrame.abs
    DataFrame.all
    DataFrame.any
    DataFrame.count
    DataFrame.cummax
    DataFrame.cummin
    DataFrame.cumsum
    DataFrame.describe
    DataFrame.diff
    DataFrame.max
    DataFrame.mean
    DataFrame.median
    DataFrame.min
    DataFrame.pct_change
    DataFrame.quantile
    DataFrame.rank
    DataFrame.round
    DataFrame.skew
    DataFrame.sum
    DataFrame.std
    DataFrame.var
    DataFrame.nunique
    DataFrame.value_counts


.. rubric:: Reindexing / selection / label manipulation

.. autosummary::
    :toctree: pandas_api/

    DataFrame.add_prefix
    DataFrame.add_suffix
    DataFrame.drop
    DataFrame.drop_duplicates
    DataFrame.duplicated
    DataFrame.equals
    DataFrame.first
    DataFrame.get
    DataFrame.head
    DataFrame.idxmax
    DataFrame.idxmin
    DataFrame.last
    DataFrame.rename
    DataFrame.rename_axis
    DataFrame.reset_index
    DataFrame.sample
    DataFrame.set_axis
    DataFrame.set_index
    DataFrame.tail
    DataFrame.take

.. rubric:: Missing data handling

.. autosummary::
    :toctree: pandas_api/

    DataFrame.backfill
    DataFrame.bfill
    DataFrame.dropna
    DataFrame.ffill
    DataFrame.fillna
    DataFrame.isna
    DataFrame.isnull
    DataFrame.notna
    DataFrame.notnull
    DataFrame.pad
    DataFrame.replace

.. rubric:: Reshaping, sorting, transposing

.. autosummary::
    :toctree: pandas_api/

    DataFrame.melt
    DataFrame.nlargest
    DataFrame.nsmallest
    DataFrame.pivot
    DataFrame.pivot_table
    DataFrame.sort_index
    DataFrame.nlargest
    DataFrame.nsmallest
    DataFrame.melt
    DataFrame.sort_values
    DataFrame.squeeze
    DataFrame.stack
    DataFrame.T
    DataFrame.transpose
    DataFrame.unstack

.. rubric:: Combining / comparing / joining / merging

.. autosummary::
    :toctree: pandas_api/

    DataFrame.compare
    DataFrame.join
    DataFrame.merge
    DataFrame.update

.. rubric:: Time Series-related

.. autosummary::
    :toctree: pandas_api/

    DataFrame.shift
    DataFrame.first_valid_index
    DataFrame.last_valid_index
    DataFrame.resample

.. rubric:: Serialization / IO / conversion

.. autosummary::
    :toctree: pandas_api/

    DataFrame.to_csv