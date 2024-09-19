#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#
import logging
import weakref
from collections import defaultdict
from typing import TYPE_CHECKING, Dict

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

    def add(self, table: SnowflakeTable) -> None:
        self.ref_count_map[table.name] += 1
        # the finalizer will be triggered when it gets garbage collected
        # and this table will be dropped finally
        _ = weakref.finalize(table, self._delete_ref_count, table.name)

    def _delete_ref_count(self, name: str) -> None:  # pragma: no cover
        """
        Decrements the reference count of a temporary table,
        and if the count reaches zero, puts this table in the queue for cleanup.
        """
        self.ref_count_map[name] -= 1
        if self.ref_count_map[name] == 0:
            if self.session.auto_clean_up_temp_table_enabled:
                self.drop_table(name)
        elif self.ref_count_map[name] < 0:
            logging.debug(
                f"Unexpected reference count {self.ref_count_map[name]} for table {name}"
            )

    def drop_table(self, name: str) -> None:  # pragma: no cover
        common_log_text = f"temp table {name} in session {self.session.session_id}"
        logging.debug(f"Ready to drop {common_log_text}")
        query_id = None
        try:
            async_job = self.session.sql(
                f"drop table if exists {name} /* internal query to drop unused temp table */",
            )._internal_collect_with_tag_no_telemetry(
                block=False, statement_params={DROP_TABLE_STATEMENT_PARAM_NAME: name}
            )
            query_id = async_job.query_id
            logging.debug(f"Dropping {common_log_text} with query id {query_id}")
        except Exception as ex:  # pragma: no cover
            warning_message = f"Failed to drop {common_log_text}, exception: {ex}"
            logging.warning(warning_message)
            if query_id is None:
                # If no query_id is available, it means the query haven't been accepted by gs,
                # and it won't occur in our job_etl_view, send a separate telemetry for recording.
                self.session._conn._telemetry_client.send_temp_table_cleanup_abnormal_exception_telemetry(
                    self.session.session_id,
                    name,
                    str(ex),
                )

    def stop(self) -> None:
        """
        Stops the cleaner (no-op) and sends the telemetry.
        """
        self.session._conn._telemetry_client.send_temp_table_cleanup_telemetry(
            self.session.session_id,
            temp_table_cleaner_enabled=self.session.auto_clean_up_temp_table_enabled,
            num_temp_tables_cleaned=self.num_temp_tables_cleaned,
            num_temp_tables_created=self.num_temp_tables_created,
        )

    @property
    def num_temp_tables_created(self) -> int:
        return len(self.ref_count_map)

    @property
    def num_temp_tables_cleaned(self) -> int:
        # TODO SNOW-1662536: we may need a separate counter for the number of tables cleaned when parameter is enabled
        return sum(v == 0 for v in self.ref_count_map.values())
