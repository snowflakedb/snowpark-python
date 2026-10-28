=================================
Custom Data Sources
=================================


.. automodule:: snowflake.snowpark.datasource
  :noindex:

.. currentmodule:: snowflake.snowpark.datasource




.. rubric:: Classes

.. autosummary::
    :toctree: api/

    DataSource
    DataSourceReader
    DataSourceRegistration
    InputPartition



.. rubric:: Methods

.. autosummary::
    :toctree: api/

    ~DataSource.name
    ~DataSource.schema
    ~DataSource.reader
    ~DataSourceReader.partitions
    ~DataSourceReader.read
    ~DataSourceRegistration.register
    ~DataSourceRegistration.unregister
    ~DataSourceRegistration.registered_names
