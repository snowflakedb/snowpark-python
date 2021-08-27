#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All right reserved.
#

from enum import Enum
from typing import Dict, Iterable, Union

from snowflake.snowpark.internal.analyzer.snowflake_plan import SnowflakeCreateTable
from snowflake.snowpark.internal.utils import Utils, _SaveMode


class DataFrameWriter:
    """Provides methods for writing data from a DataFrame to supported output destinations.

    To use this object:

    1. Create an instance of a :class:`DataFrameWriter` by accessing the :attr:`DataFrame.write` property.
    2. Specify the save mode by calling :meth:`mode`, which returns the same `DataFrameWriter` that is configured to
        save data using the specified mode.
        The default mode is "error".
    3. Call the :meth:`saveAsTable` method to save the data to the specified destination.

    Examples::

        df.write.mode("overwrite").saveAsTable("T")


    """

    def __init__(self, data_frame: "DataFrame"):
        self.__data_frame = data_frame
        self.__save_mode = _SaveMode.ERROR  # TODO: spark default value is error. Scala snowpark is append.

    def mode(self, save_mode: str) -> "DataFrameWriter":
        """Set the save mode of this `DataFrameWriter`.

        Args:
            save_mode: One of the following strings.

                "append": Append data of this DataFrame to existing data.

                "overwrite": Overwrite existing data.

                "error" or "errorifexists": Throw an exception if data already exists.

                "ignore": Ignore this operation if data already exists.

                Default value is "error".

        Returns:
            The `DataFrameWriter` itself.
        """
        self.__save_mode = Utils.str_to_enum(save_mode.lower(), _SaveMode, "`save_mode`")
        return self

    def saveAsTable(self, table_name: Union[str, Iterable[str]], mode: str = None) -> None:
        """Writes the data to the specified table in a Snowflake database.

        Args:
            table_name: This can be a str that has the table name or fully-qualified object identifier,
                which includes database, schema, and table name.
                It can also be a list of strs that have the database, schema, and table name.
            mode: The save mode used to write the data. This param will override :meth:mode if both are used.

        Returns:
            None
        """
        # Snowpark scala doesn't have mode as a param but pyspark has it.
        # They both have mode()
        full_table_name = (
            table_name if isinstance(table_name, str) else ".".join(table_name)
        )
        # TODO: Should we validate this in the client or allow the server to throw the error?
        Utils.validate_object_name(full_table_name)
        save_mode = Utils.str_to_enum(mode.lower(), _SaveMode, "`mode`") if mode else self.__save_mode
        create_table_logic_plan = SnowflakeCreateTable(
            full_table_name, save_mode, self.__data_frame._DataFrame__plan
        )
        session = self.__data_frame.session
        snowflake_plan = session.analyzer.resolve(create_table_logic_plan)
        session.conn.execute(snowflake_plan)
