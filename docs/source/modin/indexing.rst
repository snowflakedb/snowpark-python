=============================
Index
=============================

.. currentmodule:: snowflake.snowpark.modin.plugin.index
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
    Index.inferred_type
    Index.shape
    Index.name
    Index.names
    Index.nbytes
    Index.ndim
    Index.size
    Index.empty
    Index.T
    Index.memory_usage
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
    Index.factorize
    Index.identical
    Index.insert
    Index.is_
    Index.is_boolean
    Index.is_categorical
    Index.is_floating
    Index.is_integer
    Index.is_interval
    Index.is_numeric
    Index.is_object
    Index.min
    Index.max
    Index.reindex
    Index.rename
    Index.repeat
    Index.where
    Index.take
    Index.putmask
    Index.unique
    Index.nunique
    Index.value_counts
    Index.array
    Index.str

.. rubric:: Compatibility with MultiIndex

.. autosummary::
    :toctree: pandas_api/

    Index.set_names
    Index.droplevel

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
    Index.map
    Index.ravel
    Index.to_list
    Index.tolist
    Index.to_series
    Index.to_frame
    Index.view

.. rubric:: Sorting

.. autosummary::
    :toctree: pandas_api/

    Index.argsort
    Index.searchsorted
    Index.sort_values

.. rubric:: Time-specific operations

.. autosummary::
    :toctree: pandas_api/

    Index.shift

.. rubric:: Combining / joining / set operations

.. autosummary::
    :toctree: pandas_api/

    Index.append
    Index.join
    Index.intersection
    Index.union
    Index.difference
    Index.symmetric_difference

.. rubric:: Selecting

.. autosummary::
    :toctree: pandas_api/

    Index.asof
    Index.asof_locs
    Index.get_indexer
    Index.get_indexer_for
    Index.get_indexer_non_unique
    Index.get_level_values
    Index.get_loc
    Index.get_slice_bound
    Index.isin
    Index.slice_indexer
    Index.slice_locs

