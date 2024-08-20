#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#
import logging
import weakref
from collections import defaultdict
from queue import Empty, Queue
from threading import Event, Thread
from typing import TYPE_CHECKING, Dict, Optional

from snowflake.snowpark._internal.analyzer.snowflake_plan_node import SnowflakeTable

if TYPE_CHECKING:
    from snowflake.snowpark.session import Session  # pragma: no cover


DROP_TABLE_STATEMENT_PARAM_NAME = "auto_clean_up_temp_table"


class TempTableAutoCleaner:
    """
    Automatically cleans up unused temporary tables created in the current session
    when it is no longer referenced (i.e., its `SnowflakeTable` object gets garbage collected).

    Temporary tables are typically used for intermediate computations (e.g., df.cache_result) and
    are not needed when they are no longer referenced. Removing these tables helps maintain a
    clean working environment and reduce storage cost for a long-running session.
    """

    def __init__(self, session: "Session") -> None:
        self.session = session
        # this dict maintains key-value pair from Snowpark-generated temp table fully-qualified name
        # to its reference count for later temp table management
        # this dict will still be maintained even if the cleaner is stopped (`stop()` is called)
        self.ref_count_map: Dict[str, int] = defaultdict(int)
        # unused temp table will be put into the queue for cleanup
        self.queue: Queue = Queue()
        # thread for removing temp tables (running DROP TABLE sql)
        self.cleanup_thread: Optional[Thread] = None
        # An event managing a flag that indicates whether the cleaner is started
        self.stop_event = Event()

    def add(self, table: SnowflakeTable) -> None:
        self.ref_count_map[table.name] += 1
        # the finalizer will be triggered when it gets garbage collected
        # and this table will be dropped finally
        _ = weakref.finalize(table, self._delete_ref_count, table.name)

    def _delete_ref_count(self, name: str) -> None:
        """
        Decrements the reference count of a temporary table,
        and if the count reaches zero, puts this table in the queue for cleanup.
        """
        self.ref_count_map[name] -= 1
        if self.ref_count_map[name] == 0:
            self.ref_count_map.pop(name)
            # clean up
            self.queue.put(name)
        elif self.ref_count_map[name] < 0:
            logging.debug(
                f"Unexpected reference count {self.ref_count_map[name]} for table {name}"
            )

    def process_cleanup(self) -> None:
        while not self.stop_event.is_set():
            try:
                # it's non-blocking after timeout and become interruptable with stop_event
                # it will raise an `Empty` exception if queue is empty after timeout,
                # then we catch this exception and avoid breaking loop
                table_name = self.queue.get(timeout=1)
                self.drop_table(table_name)
            except Empty:
                continue

    def drop_table(self, name: str) -> None:
        common_log_text = f"temp table {name} in session {self.session.session_id}"
        logging.debug(f"Cleanup Thread: Ready to drop {common_log_text}")
        try:
            # TODO SNOW-1556553: Remove this workaround once multi-threading of Snowpark session is supported
            with self.session._conn._conn.cursor() as cursor:
                cursor.execute(
                    f"drop table if exists {name} /* internal query to drop unused temp table */",
                    _statement_params={DROP_TABLE_STATEMENT_PARAM_NAME: name},
                )
            logging.debug(f"Cleanup Thread: Successfully dropped {common_log_text}")
        except Exception as ex:
            logging.warning(
                f"Cleanup Thread: Failed to drop {common_log_text}, exception: {ex}"
            )  # pragma: no cover

    def is_alive(self) -> bool:
        return self.cleanup_thread is not None and self.cleanup_thread.is_alive()

    def start(self) -> None:
        self.stop_event.clear()
        if not self.is_alive():
            self.cleanup_thread = Thread(target=self.process_cleanup)
            self.cleanup_thread.start()

    def stop(self) -> None:
        """
        The cleaner will stop immediately and leave unfinished temp tables in the queue.
        """
        self.stop_event.set()
        if self.is_alive():
            self.cleanup_thread.join()
