#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#
from enum import Enum
from logging import getLogger
from typing import Iterator, List, Literal, Optional, Union

import snowflake.snowpark
from snowflake.connector.options import pandas
from snowflake.snowpark._internal.analyzer.analyzer_utils import result_scan_statement
from snowflake.snowpark._internal.analyzer.snowflake_plan import Query
from snowflake.snowpark._internal.utils import (
    check_is_pandas_dataframe_in_to_pandas,
    experimental,
    result_set_to_iter,
    result_set_to_rows,
)
from snowflake.snowpark.exceptions import SnowparkSQLException
from snowflake.snowpark.functions import col
from snowflake.snowpark.row import Row

_logger = getLogger(__name__)


class _AsyncResultType(Enum):
    ROW = "row"
    ITERATOR = "row_iterator"
    PANDAS = "pandas"
    PANDAS_BATCH = "pandas_batches"
    COUNT = "count"
    NO_RESULT = "no_result"
    UPDATE = "update"
    DELETE = "delete"
    MERGE = "merge"


class AsyncJob:
    """
    Provides a way to track an asynchronous query in Snowflake. A :class:`DataFrame` object can be
    evaluated asynchronously and an :class:`AsyncJob` object will be returned. With this instance,
    you can:

        - retrieve results;
        - check the query status (still running or done);
        - cancel the running query;
        - retrieve the query ID and perform other operations on this query ID manually.

    :class:`AsyncJob` can be created by :meth:`Session.create_async_job` or action methods in
    :class:`DataFrame` and other classes. All methods in :class:`DataFrame` with a suffix of
    ``_nowait`` execute asynchronously and create an :class:`AsyncJob` instance. They are also
    equivalent to corresponding functions in :class:`DataFrame` and other classes that set
    ``block=False``. Therefore, to use it, you need to create a dataframe first. Here we demonstrate
    how to do that:

    First, we create a dataframe:
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
            >>> async_job.result()[0]['rows_unloaded']
            4

    Example 7
        :meth:`Table.merge`, :meth:`Table.update`, :meth:`Table.delete` can also be performed asynchronously::

            >>> target_df = session.create_dataframe([(10, "old"), (10, "too_old"), (11, "old")], schema=["key", "value"])
            >>> target_df.write.save_as_table("my_table", mode="overwrite", table_type="temporary")
            >>> target = session.table("my_table")
            >>> source = session.create_dataframe([(10, "new"), (12, "new"), (13, "old")], schema=["key", "value"])
            >>> async_job = target.merge(source,target["key"] == source["key"],[when_matched().update({"value": source["value"]}),when_not_matched().insert({"key": source["key"]})],block=False)
            >>> async_job.result()
            MergeResult(rows_inserted=2, rows_updated=2, rows_deleted=0)

    Example 8
        Cancel the running query associated with the dataframe::

            >>> df = session.sql("select SYSTEM$WAIT(3)")
            >>> async_job = df.collect_nowait()
            >>> async_job.cancel()

    Example 9
        Executing two queries asynchronously is faster than executing two queries one by one::

            >>> from time import time
            >>> df1 = session.sql("select SYSTEM$WAIT(3)")
            >>> df2 = session.sql("select SYSTEM$WAIT(3)")
            >>> start = time()
            >>> sync_res1 = df1.collect()
            >>> sync_res2 = df2.collect()
            >>> time1 = time() - start
            >>> start = time()
            >>> async_job1 = df1.collect_nowait()
            >>> async_job2 = df2.collect_nowait()
            >>> async_res1 = async_job1.result()
            >>> async_res2 = async_job2.result()
            >>> time2 = time() - start
            >>> time2 < time1
            True

    Example 10
        Creating an :class:`AsyncJob` from an existing query ID, retrieving results and converting it back to a :class:`DataFrame`:

            >>> from snowflake.snowpark.functions import col
            >>> query_id = session.sql("select 1 as A, 2 as B, 3 as C").collect_nowait().query_id
            >>> async_job = session.create_async_job(query_id)
            >>> async_job.query
            'select 1 as A, 2 as B, 3 as C'
            >>> async_job.result()
            [Row(A=1, B=2, C=3)]
            >>> async_job.result(result_type="pandas")
               A  B  C
            0  1  2  3
            >>> df = async_job.to_df()
            >>> df.select(col("A").as_("D"), "B").collect()
            [Row(D=1, B=2)]

    Note:
        - This feature is experimental since 0.10.0. Methods in this class are subject to change in
          future releases.
        - If a dataframe is associated with multiple queries:

            + if you use :meth:`Session.create_dataframe` to create a dataframe from a large amount
              of local data and evaluate this dataframe asynchronously, data will still be loaded
              into Snowflake synchronously, and only fetching data from Snowflake again will be
              performed asynchronously.
            + otherwise, multiple queries will be wrapped into a
              `Snowflake Anonymous Block <https://docs.snowflake.com/en/developer-guide/snowflake-scripting/blocks.html#using-an-anonymous-block>`_
              and executed asynchronously as one query.
        - Temporary objects (e.g., tables) might be created when evaluating dataframes and they will
          be dropped automatically after all queries finish when calling a synchronous API. When you
          evaluate dataframes asynchronously, temporary objects will only be dropped after calling
          :meth:`result`.
        - This feature is currently not supported in Snowflake Python stored procedures.
    """

    def __init__(
        self,
        query_id: str,
        query: Optional[str],
        session: "snowflake.snowpark.session.Session",
        result_type: _AsyncResultType = _AsyncResultType.ROW,
        post_actions: Optional[List[Query]] = None,
        **kwargs,
    ) -> None:
        self.query_id: str = query_id  #: The query ID of the executed query
        self._query = query
        self._can_query_be_retrieved = True
        self._session = session
        self._cursor = session._conn._conn.cursor()
        self._result_type = result_type
        self._post_actions = post_actions if post_actions else []
        self._parameters = kwargs
        self._result_meta = None
        self._inserted = False
        self._updated = False
        self._deleted = False

    @property
    @experimental(version="0.10.0")
    def query(self) -> Optional[str]:
        """
        The SQL text of of the executed query. Returns ``None`` if it cannot be retrieved from Snowflake.
        """
        if not self._can_query_be_retrieved:
            return None
        else:
            error_message = f"query cannot be retrieved from query ID {self.query_id}"
            if not self._query:
                try:
                    result = (
                        self._session.table_function("information_schema.query_history")
                        .where(col("query_id") == self.query_id)
                        .select("query_text")
                        ._internal_collect_with_tag_no_telemetry()
                    )
                except SnowparkSQLException as ex:
                    _logger.debug(f"{error_message}: {ex}")
                    self._can_query_be_retrieved = False
                    return None
                else:
                    if len(result) == 0:
                        _logger.debug(f"{error_message}: result is empty")
                        self._can_query_be_retrieved = False
                        return None
                    else:
                        self._query = str(result[0][0])

            return self._query

    @experimental(version="0.12.0")
    def to_df(self) -> "snowflake.snowpark.dataframe.DataFrame":
        """
        Returns a :class:`DataFrame` built from the result of this asynchronous job.
        """
        return self._session.sql(result_scan_statement(self.query_id))

    @experimental(version="0.10.0")
    def is_done(self) -> bool:
        """
        Checks the status of the query associated with this instance and returns a bool value
        indicating whether the query has finished.
        """
        status = self._session._conn._conn.get_query_status(self.query_id)
        is_running = self._session._conn._conn.is_still_running(status)
        return not is_running

    @experimental(version="0.10.0")
    def cancel(self) -> None:
        """Cancels the query associated with this instance."""
        # stop and cancel current query id
        self._cursor.execute(f"select SYSTEM$CANCEL_QUERY('{self.query_id}')")

    def _table_result(
        self,
        result_data: Union[List[tuple], List[dict]],
        result_type: _AsyncResultType,
    ) -> Union[
        "snowflake.snowpark.MergeResult",
        "snowflake.snowpark.UpdateResult",
        "snowflake.snowpark.DeleteResult",
    ]:
        if result_type == _AsyncResultType.UPDATE:
            return snowflake.snowpark.UpdateResult(
                int(result_data[0][0]), int(result_data[0][1])
            )
        elif result_type == _AsyncResultType.DELETE:
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

    @experimental(version="0.10.0")
    def result(
        self,
        result_type: Optional[
            Literal["row", "row_iterator", "pandas", "pandas_batches", "no_result"]
        ] = None,
    ) -> Union[
        List[Row],
        Iterator[Row],
        "pandas.DataFrame",
        Iterator["pandas.DataFrame"],
        int,
        "snowflake.snowpark.MergeResult",
        "snowflake.snowpark.UpdateResult",
        "snowflake.snowpark.DeleteResult",
        None,
    ]:
        """
        Blocks and waits until the query associated with this instance finishes, then returns query
        results. This acts like executing query in a synchronous way. The data type of returned
        query results is determined by how you create this :class:`AsyncJob` instance. For example,
        if this instance is returned by :meth:`DataFrame.collect_nowait`, you will get a list of
        :class:`Row` s from this method.

        Args:
            result_type: (Experimental) specifies the data type of returned query results. Currently
                it only supports the following return data types:

                - "row": returns a list of :class:`Row` objects, which is the same as the return
                  type of :meth:`DataFrame.collect`.
                - "row_iterator": returns an iterator of :class:`Row` objects, which is the same as
                  the return type of :meth:`DataFrame.to_local_iterator`.
                - "row": returns a ``pandas.DataFrame``, which is the same as the return type of
                  :meth:`DataFrame.to_pandas`.
                - "pandas_batches": returns an iterator of ``pandas.DataFrame`` s, which is the same
                  as the return type of :meth:`DataFrame.to_pandas_batches`.
                - "no_result": returns ``None``. You can use this option when you intend to execute
                  the query but don't care about query results (the client will not fetch results
                  either).

                When you create an :class:`AsyncJob` by :meth:`Session.create_async_job` and
                retrieve results with this method, ``result_type`` should be specified to determine
                the result data type. Otherwise, it will return a list of :class:`Row` objects by default.
                When you create an :class:`AsyncJob` by action methods in :class:`DataFrame` and
                other classes, ``result_type`` is optional and it will return results with
                corresponding type. If you still provide a value for it, this value will overwrite
                the original result data type.
        """
        result_type = (
            _AsyncResultType(result_type.lower()) if result_type else self._result_type
        )
        self._cursor.get_results_from_sfqid(self.query_id)
        if result_type == _AsyncResultType.NO_RESULT:
            result = None
        elif result_type == _AsyncResultType.PANDAS:
            result = self._session._conn._to_data_or_iter(
                self._cursor, to_pandas=True, to_iter=False
            )["data"]
            check_is_pandas_dataframe_in_to_pandas(result)
        elif result_type == _AsyncResultType.PANDAS_BATCH:
            result = self._session._conn._to_data_or_iter(
                self._cursor, to_pandas=True, to_iter=True
            )["data"]
        else:
            result_data = self._cursor.fetchall()
            self._result_meta = self._cursor.description
            if result_type == _AsyncResultType.ROW:
                result = result_set_to_rows(result_data, self._result_meta)
            elif result_type == _AsyncResultType.ITERATOR:
                result = result_set_to_iter(result_data, self._result_meta)
            elif result_type == _AsyncResultType.COUNT:
                result = result_data[0][0]
            elif result_type in [
                _AsyncResultType.UPDATE,
                _AsyncResultType.DELETE,
                _AsyncResultType.MERGE,
            ]:
                result = self._table_result(result_data, result_type)
            else:
                raise ValueError(f"{result_type} is not supported")
        for action in self._post_actions:
            self._session._conn.run_query(
                action.sql,
                is_ddl_on_temp_object=action.is_ddl_on_temp_object,
                **self._parameters,
            )
        return result
