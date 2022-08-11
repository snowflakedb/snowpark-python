#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#
from enum import Enum
from typing import Dict, Iterator, List, Union

import snowflake.snowpark
from snowflake.connector import SnowflakeConnection
from snowflake.connector.options import pandas
from snowflake.snowpark._internal.utils import (
    check_is_pandas_dataframe_in_to_pandas,
    result_set_to_iter,
    result_set_to_rows,
)
from snowflake.snowpark.row import Row


class _AsyncDataType(Enum):
    PANDAS = "pandas"
    ROW = "row"
    ITERATOR = "iterator"
    PANDAS_BATCH = "pandas_batch"
    COUNT = "count"
    NONE_TYPE = "no_type"
    UPDATE = "update"
    DELETE = "delete"
    MERGE = "merge"


class AsyncJob:
    def __init__(
        self,
        query_id: str,
        query: str,
        conn: SnowflakeConnection,
        server_connection: "snowflake.snowpark._internal.server_connection.ServerConnection",
        data_type: _AsyncDataType = _AsyncDataType.ROW,
        parameters: Dict[str, any] = None,
    ) -> None:
        self.query_id = query_id
        self.query = query
        self._conn = conn
        self._cursor = self._conn.cursor()
        self._data_type = data_type
        self._parameters = parameters
        self._result_meta = None
        self._server_connection = server_connection
        self._inserted = False
        self._updated = False
        self._deleted = False
        self._plan = None

    def is_done(self) -> bool:
        # return a bool value to indicate whether the query is finished
        status = self._conn.get_query_status(self.query_id)
        is_running = self._conn.is_still_running(status)

        return not is_running

    def cancel(self) -> None:
        # stop and cancel current query id
        self._cursor.execute(f"select SYSTEM$CANCEL_QUERY('{self.query_id}')")

    def _table_result(
        self, result_data
    ) -> Union[
        "snowflake.snowpark.MergeResult",
        "snowflake.snowpark.UpdateResult",
        "snowflake.snowpark.DeleteResult",
    ]:
        if self._data_type == _AsyncDataType.UPDATE:
            return snowflake.snowpark.UpdateResult(
                int(result_data[0][0]), int(result_data[0][1])
            )
        elif self._data_type == _AsyncDataType.DELETE:
            return snowflake.snowpark.DeleteResult(int(result_data[0][0]))
        else:
            idx = 0
            rows_inserted, rows_updated, rows_deleted = 0, 0, 0
            if self._inserted:
                rows_inserted = int(result_data[0][idx])
                idx += 1
            if self._updated:
                rows_updated = int(result_data[0][idx])
                idx += 1
            if self._deleted:
                rows_deleted = int(result_data[0][idx])
            return snowflake.snowpark.MergeResult(
                rows_inserted, rows_updated, rows_deleted
            )

    def result(self) -> Union[List[Row], "pandas.DataFrame", Iterator[Row], int, None]:
        # return result of the query
        self._cursor.get_results_from_sfqid(self.query_id)
        if self._data_type == _AsyncDataType.NONE_TYPE:
            self._cursor.fetchall()
            result = None
        elif self._data_type == _AsyncDataType.PANDAS:
            result = self._server_connection._to_data_or_iter(
                self._cursor, to_pandas=True, to_iter=False
            )["data"]
            check_is_pandas_dataframe_in_to_pandas(result)
        elif self._data_type == _AsyncDataType.PANDAS_BATCH:
            result = self._server_connection._to_data_or_iter(
                self._cursor, to_pandas=True, to_iter=True
            )["data"]
        else:
            result_data = self._cursor.fetchall()
            self._result_meta = self._cursor.description
            if self._data_type == _AsyncDataType.ROW:
                result = result_set_to_rows(result_data, self._result_meta)
            elif self._data_type == _AsyncDataType.ITERATOR:
                result = result_set_to_iter(result_data, self._result_meta)
            elif self._data_type == _AsyncDataType.COUNT:
                result = result_data[0][0]
            elif self._data_type in [
                _AsyncDataType.UPDATE,
                _AsyncDataType.DELETE,
                _AsyncDataType.MERGE,
            ]:
                result = self._table_result(result_data)
            else:
                raise ValueError(f"{self._data_type} is not a supported data type")
        for action in self._plan.post_actions:
            self._server_connection.run_query(
                action.sql,
                is_ddl_on_temp_object=action.is_ddl_on_temp_object,
                **self._parameters,
            )
        return result
