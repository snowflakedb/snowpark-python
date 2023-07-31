=========================
Input/Output
=========================

.. currentmodule:: snowflake.snowpark

.. rubric:: Classes

.. autosummary::
    :toctree: api/

    DataFrameReader
    DataFrameWriter
    FileOperation
    PutResult
    GetResult

.. rubric:: Methods

.. autosummary::
    :toctree: api/

    DataFrameReader.avro
    DataFrameReader.csv
    DataFrameReader.format
    DataFrameReader.load
    DataFrameReader.json
    DataFrameReader.option
    DataFrameReader.options
    DataFrameReader.orc
    DataFrameReader.parquet
    DataFrameReader.schema
    DataFrameReader.table
    DataFrameReader.with_metadata
    DataFrameReader.xml
    DataFrameWriter.copy_into_location
    DataFrameWriter.csv
    DataFrameWriter.format
    DataFrameWriter.json
    DataFrameWriter.mode
    DataFrameWriter.option
    DataFrameWriter.options
    DataFrameWriter.parquet
    DataFrameWriter.partition_by
    DataFrameWriter.save
    DataFrameWriter.saveAsTable
    DataFrameWriter.save_as_table
    FileOperation.get
    FileOperation.get_stream
    FileOperation.put
    FileOperation.put_stream
    PutResult.count
    PutResult.index
    GetResult.count
    GetResult.index

.. rubric:: Attributes

.. autosummary::
    :toctree: api/

    PutResult.message
    PutResult.source
    PutResult.source_compression
    PutResult.source_size
    PutResult.status
    PutResult.target
    PutResult.target_compression
    PutResult.target_size
    GetResult.file
    GetResult.message
    GetResult.size
    GetResult.status

