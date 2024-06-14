=============================
Index
=============================

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
    Index.is_interval
    Index.is_numeric
    Index.is_object
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

