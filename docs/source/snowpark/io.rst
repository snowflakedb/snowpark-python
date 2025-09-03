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
    ListResult

.. rubric:: Methods

.. autosummary::
    :toctree: api/

    DataFrameReader.avro
    DataFrameReader.csv
    DataFrameReader.dbapi
    DataFrameReader.file
    DataFrameReader.format
    DataFrameReader.json
    DataFrameReader.load
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
    DataFrameWriter.save
    DataFrameWriter.saveAsTable
    DataFrameWriter.save_as_table
    DataFrameWriter.insertInto
    DataFrameWriter.insert_into
    FileOperation.get
    FileOperation.get_stream
    FileOperation.put
    FileOperation.put_stream
    FileOperation.list
    FileOperation.remove
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
    ListResult.name
    ListResult.size
    ListResult.md5
    ListResult.sha1
    ListResult.last_modified

