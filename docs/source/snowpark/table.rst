

==========
Table
==========


.. currentmodule:: snowflake.snowpark



.. rubric:: Classes

.. autosummary::
    :toctree: api/

    DeleteResult
    MergeResult
    Table
    UpdateResult
    WhenMatchedClause
    WhenNotMatchedClause



.. rubric:: Methods

.. autosummary::
    :toctree: api/

    ~Table.delete
    ~Table.drop_table
    ~Table.merge
    ~Table.sample
    ~Table.update
    ~WhenMatchedClause.delete
    ~WhenMatchedClause.update
    ~WhenNotMatchedClause.insert



.. rubric:: Attributes

.. autosummary::
    :toctree: api/

    ~DeleteResult.rows_deleted
    ~MergeResult.rows_deleted
    ~MergeResult.rows_inserted
    ~MergeResult.rows_updated
    ~Table.is_cached
    ~Table.table_name
    ~UpdateResult.multi_joined_rows_updated
    ~UpdateResult.rows_updated



