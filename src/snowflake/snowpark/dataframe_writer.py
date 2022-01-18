#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#

from enum import Enum
from typing import Dict, Iterable, Optional, Union

import snowflake.snowpark  # for forward references of type hints
from snowflake.snowpark._internal.analyzer.snowflake_plan import SnowflakeCreateTable
from snowflake.snowpark._internal.utils import Utils, _SaveMode


class DataFrameWriter:
    """Provides methods for writing data from a :class:`DataFrame` to supported output destinations.

    To use this object:

    1. Create an instance of a :class:`DataFrameWriter` by accessing the :attr:`DataFrame.write` property.
    2. Specify the save mode by calling :meth:`mode`, which returns the same
       :class:`DataFrameWriter` that is configured to save data using the specified mode.
       The default mode is "errorifexists".
    3. Call the :meth:`saveAsTable` method to save the data to the specified destination.

    Example::

        df.write.mode("overwrite").saveAsTable("table1")


    """

    def __init__(self, dataframe: "snowflake.snowpark.DataFrame"):
        self.__dataframe = dataframe
        self.__save_mode = _SaveMode.APPEND  # spark default value is error.

    def mode(self, save_mode: str) -> "DataFrameWriter":
        """Set the save mode of this :class:`DataFrameWriter`.

        Args:
            save_mode: One of the following strings.

                "append": Append data of this DataFrame to existing data.

                "overwrite": Overwrite existing data.

                "errorifexists": Throw an exception if data already exists.

                "ignore": Ignore this operation if data already exists.

                Default value is "errorifexists".

        Returns:
            The :class:`DataFrameWriter` itself.
        """
        self.__save_mode = Utils.str_to_enum(
            save_mode.lower(), _SaveMode, "`save_mode`"
        )
        return self

    def saveAsTable(
        self,
        table_name: Union[str, Iterable[str]],
        *,
        mode: Optional[str] = None,
        create_temp_table: bool = False
    ) -> None:
        """Writes the data to the specified table in a Snowflake database.

        Args:
            table_name: A string or list of strings that specify the table name or fully-qualified object identifier
                (database name, schema name, and table name).
            mode: One of the following values. When it's ``None``, the save mode set by calling ``df.write.mode(save_mode)`` is used.

                "append": Append data of this DataFrame to existing data.

                "overwrite": Overwrite existing data.

                "errorifexists": Throw an exception if data already exists.

                "ignore": Ignore this operation if data already exists.

            create_temp_table: The to-be-created table will be temporary if this is set to ``True``. Default is ``False``.

        Example::

            df.write.mode("overwrite").saveAsTable("table1")
            df.write.saveAsTable("table2", mode="overwrite", create_temp_table=True)

        """
        # Snowpark scala doesn't have mode as a param but pyspark has it.
        # They both have mode()
        save_mode = (
            Utils.str_to_enum(mode.lower(), _SaveMode, "'mode'")
            if mode
            else self.__save_mode
        )
        full_table_name = (
            table_name if isinstance(table_name, str) else ".".join(table_name)
        )
        Utils.validate_object_name(full_table_name)
        create_table_logic_plan = SnowflakeCreateTable(
            full_table_name,
            save_mode,
            self.__dataframe._plan,
            create_temp_table,
        )
        session = self.__dataframe.session
        snowflake_plan = session._analyzer.resolve(create_table_logic_plan)
        session._conn.execute(snowflake_plan)
