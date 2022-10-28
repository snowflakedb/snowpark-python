==============
Table
==============

.. currentmodule:: snowflake.snowpark

.. rubric:: Classes

.. autosummary::
    :toctree: api/

    Table

.. rubric:: Methods

.. autosummary::
    :toctree: api/

    Table.sample
    Table.update
    Table.delete
    Table.merge
    Table.drop_table
    UpdateResult.count
    UpdateResult.index
    DeleteResult.count
    DeleteResult.index
    MergeResult.count
    MergeResult.index
    WhenMatchedClause.delete
    WhenMatchedClause.update
    WhenNotMatchedClause.insert
   
   




.. rubric:: Attributes

.. autosummary::
    :toctree: api/
    
    Table.is_cached
    Table.table_name
    UpdateResult.multi_joined_rows_updated
    UpdateResult.rows_updated
    DeleteResult.rows_deleted
    MergeResult.rows_deleted
    MergeResult.rows_inserted
    MergeResult.rows_updated