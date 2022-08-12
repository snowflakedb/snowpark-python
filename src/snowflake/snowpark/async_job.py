#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#
from enum import Enum
from typing import Iterator, List, Union

import snowflake.snowpark
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
    """AsyncJob is strongly related to dataframe functions, to use AsyncJob, you need to create a dataframe first
    Provides a way to track an asynchronous query in Snowflake. A :class:`DataFrame` object can be evaluated asynchronously and an :class:`AsyncJob` object will be returned. With this instance, you can do:
    - retrieve results;
    - check the query status (still running or done);
    - cancel the running query;
    - retrieve the query ID and perform other operations on this query ID manually.
    We demonstrate how to evaluate a :class:`DataFrame` asynchronously and use returned :class:`AsyncJob` object:


    First, we create a dataframe:
        >>> from snowflake.snowpark import Session
        >>> from snowflake.snowpark.functions import when_matched, when_not_matched
        >>> df = session.create_dataframe([[float(4), 3, 5], [2.0, -4, 7], [3.0, 5, 6],[4.0,6,8]], schema=["a", "b", "c"])

    Example 1
        :meth:`DataFrame.collect` can be performed asynchronously::

            >>> async_job = df.collect_nowait()
            >>> async_job.result()
            [Row(A=4.0, B=3, C=5), Row(A=2.0, B=-4, C=7), Row(A=3.0, B=5, C=6), Row(A=4.0, B=6, C=8)]

        You can also do::
            >>> async_job = df.collect(block=False)
            >>> async_job.result()
            [Row(A=4.0, B=3, C=5), Row(A=2.0, B=-4, C=7), Row(A=3.0, B=5, C=6), Row(A=4.0, B=6, C=8)]

    Example 2
        :meth:`DataFrame.to_pandas` can be performed asynchronously::

            >>> async_job = df.to_pandas(block=False)
            >>> async_job.result()
                 A  B  C
            0  4.0  3  5
            1  2.0 -4  7
            2  3.0  5  6
            3  4.0  6  8

    Example 3
        :meth:`DataFrame.first` can be performed asynchronously::

            >>> async_job = df.first(block=False)
            >>> async_job.result()
            [Row(A=4.0, B=3, C=5)]

    Example 4
        :meth:`DataFrame.count` can be performed asynchronously::

            >>> async_job = df.count(block=False)
            >>> async_job.result()
            4

    Example 5
        Save a dataframe to table or copy it into a stage file can also be performed asynchronously::

            >>> table_name = "name"
            >>> async_job = df.write.save_as_table(table_name, block=False)
            >>> # copy into a stage file
            >>> remote_location = f"{session.get_session_stage()}/name.csv"
            >>> async_job = df.write.copy_into_location(remote_location, block=False)
            >>> async_job.result()
            [Row(rows_unloaded=4, input_bytes=27, output_bytes=47)]

    Example 7
        :meth:`Table.merge`, :meth:`Table.update`, :meth:`Table.delete` can also be performed asynchronously::

            >>> target_df = session.create_dataframe([(10, "old"), (10, "too_old"), (11, "old")], schema=["key", "value"])
            >>> target_df.write.save_as_table("my_table", mode="overwrite", table_type="temporary")
            >>> target = session.table("my_table")
            >>> source = session.create_dataframe([(10, "new"), (12, "new"), (13, "old")], schema=["key", "value"])
            >>> target.merge(source,target["key"] == source["key"],[when_matched().update({"value": source["value"]}),when_not_matched().insert({"key": source["key"]})],block=False)
            >>> target.result()
            MergeResult(rows_inserted=2, rows_updated=2, rows_deleted=0)

    Example 8
        Cancel the running query associated with the dataframe::

            >>> df = session.sql("select SYSTEM$WAIT(3)")
            >>> async_job = df.collect_nowait()
            >>> async_job.cancel()

    Note:
        - If a dataframe is associated with multiple queries,
            + if you use `:meth:Session.create_dataframe` to create a dataframe from a large amount of local data and evaluate this dataframe asynchronously, data will still be loaded into Snowflake synchronously, and only fetching data from Snowflake again will be performed asynchronously.
            + otherwise, multiple queries will be wrapped into a `Snowflake Anonymous Block <https://docs.snowflake.com/en/developer-guide/snowflake-scripting/blocks.html#using-an-anonymous-block>`_ and executed asynchronously as one query.
        - Temporary objects (e.g., tables) might be created when evaluating dataframes and they will be dropped automatically after all queries finish when calling a synchronous API. When you evaluating dataframes asynchronously, temporary objects will only be dropped after calling :meth:`result`.
    """

    def __init__(
        self,
        query_id: str,
        query: str,
        server_connection: "snowflake.snowpark._internal.server_connection.ServerConnection",
        data_type: _AsyncDataType = _AsyncDataType.ROW,
        **kwargs,
    ) -> None:
        #: The query ID of the executed query
        self.query_id = query_id
        #: The SQL text of of the executed query
        self.query = query
        self._conn = server_connection._conn
        self._cursor = self._conn.cursor()
        self._data_type = data_type
        self._parameters = kwargs
        self._result_meta = None
        self._server_connection = server_connection
        self._inserted = False
        self._updated = False
        self._deleted = False
        self._plan = None

    def is_done(self) -> bool:
        """Check the status of query associated with this instance, return a bool value indicate whether the query is
        finished.

        """
        # return a bool value to indicate whether the query is finished
        status = self._conn.get_query_status(self.query_id)
        is_running = self._conn.is_still_running(status)

        return not is_running

    def cancel(self) -> None:
        """Cancel the query associated with this instance."""
        # stop and cancel current query id
        self._cursor.execute(f"select SYSTEM$CANCEL_QUERY('{self.query_id}')")

    def _table_result(
        self, result_data: Union[List[tuple], List[dict]]
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
        """Block and wait until the query associated with this instance is finished and return query results, this
        functions acts like execute query in a synchronous way. Return data type is decided by the function that create
        this instance

        """
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
                raise ValueError(f"{self._data_type} is not supported")
        for action in self._plan.post_actions:
            self._server_connection.run_query(
                action.sql,
                is_ddl_on_temp_object=action.is_ddl_on_temp_object,
                **self._parameters,
            )
        return result
