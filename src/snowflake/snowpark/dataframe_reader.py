#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#

from typing import Dict

import snowflake.snowpark
from snowflake.snowpark._internal.analyzer.analyzer_utils import (
    create_file_format_statement,
    drop_file_format_if_exists_statement,
    infer_schema_statement,
    quote_name_without_upper_casing,
)
from snowflake.snowpark._internal.analyzer.expression import Attribute
from snowflake.snowpark._internal.error_message import SnowparkClientExceptionMessages
from snowflake.snowpark._internal.type_utils import convert_sf_to_sp_type
from snowflake.snowpark._internal.utils import (
    COPY_OPTIONS,
    INFER_SCHEMA_FORMAT_TYPES,
    TempObjectType,
    random_name_for_temp_object,
)
from snowflake.snowpark.dataframe import DataFrame
from snowflake.snowpark.types import StructType, VariantType


class DataFrameReader:
    """Provides methods to load data in various supported formats from a Snowflake
    stage to a :class:`DataFrame`. The paths provided to the DataFrameReader must refer
    to Snowflake stages.

    To use this object:

    1. Access an instance of a :class:`DataFrameReader` by using the
    :attr:`Session.read` property.

    2. Specify any `format-specific options <https://docs.snowflake.com/en/sql-reference/sql/create-file-format.html#format-type-options-formattypeoptions>`_ and `copy options <https://docs.snowflake.com/en/sql-reference/sql/copy-into-table.html#copy-options-copyoptions>`_
    by calling the :func:`option` or :func:`options` method. These methods return a
    DataFrameReader that is configured with these options. (Note that although
    specifying copy options can make error handling more robust during the reading
    process, it may have an effect on performance.)

    3. Specify the schema of the data that you plan to load by constructing a
    :class:`types.StructType` object and passing it to the :func:`schema` method. This method
    returns a :class:`DataFrameReader` that is configured to read data that uses the
    specified schema.

    4. Specify the format of the data by calling the method named after the format
    (e.g. :func:`csv`, :func:`json`, etc.). These methods return a :class:`DataFrame`
    that is configured to load data in the specified format.

    5. Call a :class:`DataFrame` method that performs an action (e.g.
    :func:`DataFrame.collect`) to load the data from the file.

    The following examples demonstrate how to use a DataFrameReader.

    Example 1:
        Loading the first two columns of a CSV file and skipping the first header line::

            from snowflake.snowpark.types import *
            file_path = "@mystage1"
            # Define the schema for the data in the CSV file.
            user_schema = StructType([StructField("a", IntegerType()), StructField("b", StringType())])
            # Create a DataFrame that is configured to load data from the CSV file.
            df = session.read.option("skip_header", 1).schema(user_schema).csv(file_path)
            # Load the data into the DataFrame and return an Array of Rows containing the results.
            results = df.collect()


    Example 2:
        Loading a gzip compressed json file::

            file_path = "@mystage2/data.json.gz"
            # Create a DataFrame that is configured to load data from the gzipped JSON file.
            json_df = session.read.option("compression", "gzip").json(file_path)
            # Load the data into the DataFrame and return an Array of Rows containing the results.
            results = json_df.collect()

      In addition, if you want to load only a subset of files from the stage, you can use the
      `pattern <https://docs.snowflake.com/en/sql-reference/sql/copy-into-table.html#loading-using-pattern-matching>`_
      option to specify a regular expression that matches the files that you want to load.

    Example 3:
        Loading only the CSV files from a stage location::

            from snowflake.snowpark.types import *
            # Define the schema for the data in the CSV files.
            user_schema = StructType([StructField("a", IntegerType()), StructField("b", StringType())])
            # Create a DataFrame that is configured to load data from the CSV files in the stage.
            csv_df = session.read.option("pattern", ".*[.]csv").schema(user_schema).csv("@stage_location")
            # Load the data into the DataFrame and return an Array of Rows containing the results.
            results = csv_df.collect()
    """

    def __init__(self, session: "snowflake.snowpark.session.Session"):
        self._session = session
        self._user_schema = None
        self._cur_options = {}
        self._file_path = None
        self._file_type = None

    def table(self, name: str) -> DataFrame:
        """Returns a :class:`DataFrame` that is set up to load data from the specified
        table.

        For the ``name`` argument, you can specify an unqualified name (if the table
        is in the current database and schema) or a fully qualified name
        (``db.schema.name``).

        Note that the data is not loaded in the DataFrame until you call a method that
        performs an action (e.g. :func:`DataFrame.collect`,
        :func:`DataFrame.count`, etc.).

        Args:
            name: Name of the table to use.
        """
        return self._session.table(name)

    def schema(self, schema: StructType) -> "DataFrameReader":
        """Returns a :class:`DataFrameReader` instance with the specified schema
        configuration for the data to be read.

        To define the schema for the data that you want to read, use a
        :class:`types.StructType` object.

        Args:
            schema: Schema configuration for the data to be read.
        """
        self._user_schema = schema
        return self

    def csv(self, path: str) -> DataFrame:
        """Returns a :class:`DataFrame` that is set up to load data from the specified CSV
        file.

        This method only supports reading data from files in Snowflake stages.

        Note that the data is not loaded in the DataFrame until you call a method that
        performs an action (e.g. :func:`DataFrame.collect`, :func:`DataFrame.count`,
        etc.).

        Example::

            file_path = "@mystage1/myfile.csv"
            # Create a DataFrame that uses a DataFrameReader to load data from a file
            # in a stage.
            df = session.read.schema(user_schema).csv(fileInAStage).filter(col("a") < 2)
            # Load the data into the DataFrame and return an Array of Rows containing the results.
            results = df.collect()

        Args:
            path: The path to the CSV file (including the stage name).
        """
        if not self._user_schema:
            raise SnowparkClientExceptionMessages.DF_MUST_PROVIDE_SCHEMA_FOR_READING_FILE()

        self._file_path = path
        self._file_type = "csv"
        df = DataFrame(
            self._session,
            self._session._plan_builder.read_file(
                path,
                self._file_type,
                self._cur_options,
                self._session.get_fully_qualified_current_schema(),
                self._user_schema._to_attributes(),
            ),
        )
        df._reader = self
        return df

    def json(self, path: str) -> DataFrame:
        r"""Returns a :class:`DataFrame` that is set up to load data from the
        specified JSON file.

        This method only supports reading data from files in Snowflake stages.

        Note that the data is not loaded in the DataFrame until you call a method that
        performs an action (e.g. :func:`DataFrame.collect`, :func:`DataFrame.count`,
        etc.).

        Example::

          # Create a DataFrame that uses a DataFrameReader to load data from a file in a stage.
          df = session.read.json(path).where(col("$1:num") > 1)
          # Load the data into the DataFrame and return an Array of Rows containing the results.
          results = df.collect()

        Args:
            path: The path to the JSON file (including the stage name).

        """
        return self._read_semi_structured_file(path, "JSON")

    def avro(self, path: str) -> DataFrame:
        """Returns a :class:`DataFrame` that is set up to load data from the
        specified Avro file.

        This method only supports reading data from files in Snowflake stages.

        Note that the data is not loaded in the DataFrame until you call a method that
        performs an action (e.g. :func:`DataFrame.collect`, :func:`DataFrame.count`,
        etc.).

        Args:
            path: The path to the Avro file (including the stage name).

        """
        return self._read_semi_structured_file(path, "AVRO")

    def parquet(self, path: str) -> DataFrame:
        """Returns a :class:`DataFrame` that is set up to load data from the specified
        Parquet file.

        This method only supports reading data from files in Snowflake stages.

        Note that the data is not loaded in the DataFrame until you call a method that
        performs an action (e.g. :func:`DataFrame.collect`, :func:`DataFrame.count`,
        etc.).

        Example::

            # Create a DataFrame that uses a DataFrameReader to load data from a file in
            # a stage.
            df = session.read.parquet(path).where(col("$1:num") > 1)
            # Load the data into the DataFrame and return an Array of Rows containing
            # the results.
            results = df.collect()

        Args:
            path: The path to the Parquet file (including the stage name).

        """
        return self._read_semi_structured_file(path, "PARQUET")

    def orc(self, path: str) -> DataFrame:
        """Returns a :class:`DataFrame` that is set up to load data from the specified
        ORC file.

        This method only supports reading data from files in Snowflake stages.

        Note that the data is not loaded in the DataFrame until you call a method that performs
        an action (e.g. :func:`DataFrame.collect`, :func:`DataFrame.count`, etc.).

        Example::

            # Create a DataFrame that uses a DataFrameReader to load data from a file in a stage.
            df = session.read.orc(path).where(col("$1:num") > 1)
            # Load the data into the DataFrame and return an Array of Rows containing the results.
            results = df.collect()

        Args:
            path: The path to the ORC file (including the stage name).

        """
        return self._read_semi_structured_file(path, "ORC")

    def xml(self, path: str) -> DataFrame:
        """Returns a :class:`DataFrame` that is set up to load data from the specified
        XML file.

        This method only supports reading data from files in Snowflake stages.

        Note that the data is not loaded in the DataFrame until you call a method that
        performs an action (e.g. :func:`DataFrame.collect`, :func:`DataFrame.count`,
        etc.).

        Example::

            # Create a DataFrame that uses a DataFrameReader to load data from a file in a stage.
            df = session.read.xml(path).where(col("xmlget(\\$1, 'num', 0):\"$\"") > 1)
            # Load the data into the DataFrame and return an Array of Rows containing the results.
            results = df.collect()

        Args:
            path: The path to the XML file (including the stage name).
        """
        return self._read_semi_structured_file(path, "XML")

    def option(self, key: str, value) -> "DataFrameReader":
        """Sets the specified option in the DataFrameReader.

        Use this method to configure any
        `format-specific options <https://docs.snowflake.com/en/sql-reference/sql/create-file-format.html#format-type-options-formattypeoptions>`_
        and
        `copy options <https://docs.snowflake.com/en/sql-reference/sql/copy-into-table.html#copy-options-copyoptions>`_.
        (Note that although specifying copy options can make error handling more robust during the
        reading process, it may have an effect on performance.)

        Example 1:
            Loading a LZO compressed Parquet file::

                # Create a DataFrame that uses a DataFrameReader to load data from a file in a stage.
                df = session.read.option("compression", "lzo").parquet(file_path)
                # Load the data into the DataFrame and return an Array of Rows containing the results.
                results = df.collect()

        Example 2:
            Loading an uncompressed JSON file::

                # Create a DataFrame that uses a DataFrameReader to load data from a file in a stage.
                df = session.read.option("compression", "none").json(file_path)
                # Load the data into the DataFrame and return an Array of Rows containing the results.
                results = df.collect()

        Example 3:
            Loading the first two columns of a colon-delimited CSV file in which the
            first line is the header::

              from snowflake.snowpark.types import *
              # Define the schema for the data in the CSV files.
              user_schema = StructType([StructField("a", IntegerType()), StructField("b", StringType())])
              # Create a DataFrame that is configured to load data from the CSV file.
              csv_df = session.read.option("field_delimiter", ":").option("skip_header", 1).schema(user_schema).csv(file_path)
              # Load the data into the DataFrame and return an Array of Rows containing the results.
              results = csv_df.collect()

        In addition, if you want to load only a subset of files from the stage, you can
        use the `pattern <https://docs.snowflake.com/en/sql-reference/sql/copy-into-table.html#loading-using-pattern-matching>`_
        option to specify a regular expression that matches the files that you want to
        load.

        Example 4:
            Loading only the CSV files from a stage location::

                from snowflake.snowpark.types import *
                # Define the schema for the data in the CSV files.
                user_schema = StructType([StructField("a", IntegerType()),StructField("b", StringType())])
                # Create a DataFrame that is configured to load data from the CSV files in the stage.
                csv_df = session.read.option("pattern", ".*[.]csv").schema(user_schema).csv("@stage_location")
                # Load the data into the DataFrame and return an Array of Rows containing the results.
                results = csv_df.collect()

        By default, we will automatically determine the schema for parquet, avro and orc
        files and allow you to select from the individual columns. If you would like to
        disable this, you can set the `INFER_SCHEMA <https://docs.snowflake.com/en/sql-reference/functions/infer_schema.html>`_
        option to False. If you disable this schema detection, then everything is loaded into a column called, $1.

        Example 5:
            Disabling schema detection::

            >>> # Create a DataFrameReader that doesn't infer the schema for avro, parquet, or orc
            >>> default_schema_reader = session.read.option("INFER_SCHEMA", False)

        Args:
            key: Name of the option (e.g. ``compression``, ``skip_header``, etc.).
            value: Value of the option.
        """
        self._cur_options[key.upper()] = value
        return self

    def options(self, configs: Dict) -> "DataFrameReader":
        """Sets multiple specified options in the DataFrameReader.

        Use this method to configure any
        `format-specific options <https://docs.snowflake.com/en/sql-reference/sql/create-file-format.html#format-type-options-formattypeoptions>`_
        and
        `copy options <https://docs.snowflake.com/en/sql-reference/sql/copy-into-table.html#copy-options-copyoptions>`_.
        (Note that although specifying copy options can make error handling more robust during the
        reading process, it may have an effect on performance.)

        In addition, if you want to load only a subset of files from the stage, you can use the
        `pattern <https://docs.snowflake.com/en/sql-reference/sql/copy-into-table.html#loading-using-pattern-matching>`_
        option to specify a regular expression that matches the files that you want to load.

        Example:
            Loading a LZO compressed Parquet file and removing any white space from the
            fields::

                # Create a DataFrame that uses a DataFrameReader to load data from a file in a stage.
                df = session.read.option({"compression": "lzo", "trim_space": true}).parquet(file_path)
                # Load the data into the DataFrame and return an Array of Rows containing the results.
                results = df.collect()

        Args:
            configs: Dictionary of the names of options (e.g. ``compression``,
                ``skip_header``, etc.) and their corresponding values.
        """
        for k, v in configs.items():
            self.option(k, v)
        return self

    def _read_semi_structured_file(self, path: str, format: str) -> DataFrame:
        if self._user_schema:
            raise ValueError(f"Read {format} does not support user schema")
        self._file_path = path
        self._file_type = format

        format_type_options = {}
        for k, v in self._cur_options.items():
            if k not in ("PATTERN", "INFER_SCHEMA") and k not in COPY_OPTIONS:
                format_type_options[k] = v

        infer_schema = (
            self._cur_options.get("INFER_SCHEMA", True)
            if format in INFER_SCHEMA_FORMAT_TYPES
            else False
        )

        schema = [Attribute('"$1"', VariantType())]
        transformations = None
        schema_to_cast = None
        if infer_schema:
            temp_file_format_name = (
                self._session.get_fully_qualified_current_schema()
                + "."
                + random_name_for_temp_object(TempObjectType.FILE_FORMAT)
            )
            create_file_format_query = create_file_format_statement(
                temp_file_format_name,
                format,
                format_type_options,
                temp=True,
                if_not_exist=True,
            )
            drop_file_format_if_exists_query = drop_file_format_if_exists_statement(
                temp_file_format_name
            )
            infer_schema_query = infer_schema_statement(path, temp_file_format_name)
            try:
                self._session._conn.run_query(
                    create_file_format_query, is_ddl_on_temp_object=True
                )
                results = self._session._conn.run_query(infer_schema_query)["data"]
                new_schema = []
                schema_to_cast = []
                transformations = []
                for r in results:
                    # Columns for r [column_name, type, nullable, expression, filenames]
                    name = quote_name_without_upper_casing(r[0])
                    # Parse the type returned by infer_schema command to
                    # pass to determine datatype for schema
                    data_type_parts = r[1].split("(")
                    parts_length = len(data_type_parts)
                    if parts_length == 1:
                        data_type = r[1]
                        precision = 0
                        scale = 0
                    else:
                        data_type = data_type_parts[0]
                        precision = int(data_type_parts[1].split(",")[0])
                        scale = int(data_type_parts[1].split(",")[1][:-1])
                    new_schema.append(
                        Attribute(
                            name,
                            convert_sf_to_sp_type(data_type, precision, scale),
                            r[2],
                        )
                    )
                    schema_to_cast.append((r[3], r[0]))
                    transformations.append(r[3])
                schema = new_schema
            finally:
                # Clean up the file format we created
                self._session._conn.run_query(
                    drop_file_format_if_exists_query, is_ddl_on_temp_object=True
                )

        df = DataFrame(
            self._session,
            self._session._plan_builder.read_file(
                path,
                format,
                self._cur_options,
                self._session.get_fully_qualified_current_schema(),
                schema,
                schema_to_cast=schema_to_cast,
                transformations=transformations,
            ),
        )
        df._reader = self
        return df
