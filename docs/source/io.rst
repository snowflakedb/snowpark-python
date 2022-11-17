
=================
Input/Output
=================

.. currentmodule:: snowflake.snowpark

.. rubric:: Classes

.. autosummary::
    :recursive:
    :toctree: api/

    DataFrameReader
	DataFrameWriter
	FileOperation
	PutResult
	GetResult


.. rubric:: Methods

.. autosummary::
    :toctree: api/

    ~DataFrameReader.__init__
	~DataFrameReader.avro
	~DataFrameReader.csv
	~DataFrameReader.json
	~DataFrameReader.option
	~DataFrameReader.options
	~DataFrameReader.orc
	~DataFrameReader.parquet
	~DataFrameReader.schema
	~DataFrameReader.table
	~DataFrameReader.xml
	~DataFrameWriter.__init__
	~DataFrameWriter.copy_into_location
	~DataFrameWriter.mode
	~DataFrameWriter.saveAsTable
	~DataFrameWriter.save_as_table
	~FileOperation.__init__
	~FileOperation.get
	~FileOperation.get_stream
	~FileOperation.put
	~FileOperation.put_stream
	~PutResult.__init__
	~PutResult.count
	~PutResult.index
	~GetResult.__init__
	~GetResult.count
	~GetResult.index
    


.. rubric:: Attributes

.. autosummary::
    :toctree: api/

    ~PutResult.message
	~PutResult.source
	~PutResult.source_compression
	~PutResult.source_size
	~PutResult.status
	~PutResult.target
	~PutResult.target_compression
	~PutResult.target_size
	~GetResult.file
	~GetResult.message
	~GetResult.size
	~GetResult.status
    


