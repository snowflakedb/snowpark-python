#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

"""
Helper methods for moving data between Snowflake and Ray. move_from_ray_helper is defined to
raise an error if ray is not installed.
"""

import modin.pandas as pd
from modin.config import context as config_context
from modin.core.storage_formats import BaseQueryCompiler, PandasQueryCompiler  # type: ignore
import numpy as np
import pandas as native_pd

import sys
import os
import logging
from typing import TYPE_CHECKING, Any

from snowflake.snowpark._internal.utils import (
    random_name_for_temp_object,
    is_interactive,
)
from snowflake.snowpark.modin.plugin._internal.utils import (
    generate_snowflake_quoted_identifiers_helper,
    extract_pandas_label_from_snowflake_quoted_identifier,
    fill_none_in_index_labels,
    unquote_name_if_quoted,
    TempObjectType,
    ROW_POSITION_COLUMN_LABEL,
)
from snowflake.snowpark.modin.plugin._internal.frame import (
    InternalFrame,
)
from snowflake.snowpark.session import Session

if TYPE_CHECKING:
    from snowflake.snowpark.modin.plugin.compiler.snowflake_query_compiler import (
        SnowflakeQueryCompiler,
    )

try:  # pragma: no cover
    import ray  # type: ignore[import]
    from ray.util.actor_pool import ActorPool  # type: ignore[import]

    @ray.remote
    class SnowflakeWriterActor:
        """
        Ray remote actor responsible for writing data from Ray to Snowflake.
        """

        def __init__(
            self, table_name: str, connection_creds: dict[str, Any]
        ) -> None:  # pragma: no cover
            self.table_name = table_name
            try:
                self.session = Session.builder.configs(connection_creds).getOrCreate()
            except Exception as e:
                logging.error(
                    "Could not get or create Snowpark session. Ensure you have a "
                    "~/.snowflake/connections.toml file with the correct credentials. "
                    f"{e}"
                )
                raise RuntimeError("Could not get or create Snowpark session.") from e

        def write(self, batch: native_pd.DataFrame) -> None:  # pragma: no cover
            self.session.write_pandas(
                df=batch,
                table_name=self.table_name,
                table_type="",
                parallel=4,
                auto_create_table=True,
                overwrite=False,
            )

    def move_from_ray_helper(ray_qc: BaseQueryCompiler, *, max_sessions: int):
        """
        Move the data from Ray to Snowflake by writing to a Snowflake table. Preserves
        the row position order from the original dataframe.

        Args:
            ray_qc: The Ray-backed query compiler.
            max_sessions: The maximum number of sessions to use for writes.

        Returns:
            A new SnowflakeQueryCompiler with the data
        """
        from snowflake.snowpark.modin.plugin.compiler.snowflake_query_compiler import (
            SnowflakeQueryCompiler,
        )

        def get_connection_creds() -> dict[str, Any]:
            """
            Get the connection credentials from the notebook environment or an empty dict
            otherwise to use default connection parameters.

            Multiple sessions are necessary because Ray spawns separate processes, and the
            existing session object cannot be pickled/passed around.
            """
            if "snowbook" in sys.modules:
                try:
                    with open("/snowflake/session/token") as token_file:
                        return {
                            "host": os.getenv("SNOWFLAKE_HOST"),
                            "account": os.getenv("SNOWFLAKE_ACCOUNT"),
                            "token": token_file.read(),
                            "authenticator": "oauth",
                            "protocol": "https",
                            "database": os.getenv("SNOWFLAKE_DATABASE"),
                            "schema": os.getenv("SNOWFLAKE_SCHEMA"),
                            "port": os.getenv("SNOWFLAKE_PORT"),
                            "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE", ""),
                        }
                except Exception:
                    logging.error(
                        "Could not read session token from notebook environment. "
                        "Attempting to use default connection parameters."
                    )
                    return {}
            return {}

        connection_creds = get_connection_creds()
        table_name = random_name_for_temp_object(TempObjectType.TABLE)
        pool = ActorPool(
            [
                SnowflakeWriterActor.remote(table_name, connection_creds)  # type: ignore[attr-defined]
                for _ in range(max_sessions)
            ]
        )
        ray_df = pd.DataFrame(query_compiler=ray_qc)

        original_column_labels = ray_df.columns.tolist()
        original_column_index_names = ray_df.columns.names
        data_column_snowflake_quoted_identifiers = (
            generate_snowflake_quoted_identifiers_helper(
                pandas_labels=original_column_labels, excluded=[]
            )
        )
        ray_df.columns = [
            extract_pandas_label_from_snowflake_quoted_identifier(identifier)
            for identifier in data_column_snowflake_quoted_identifiers
        ]
        original_index_pandas_labels = ray_df.index.names
        index_snowflake_quoted_identifiers = (
            generate_snowflake_quoted_identifiers_helper(
                pandas_labels=fill_none_in_index_labels(original_index_pandas_labels),
                excluded=data_column_snowflake_quoted_identifiers,
                wrap_double_underscore=True,
            )
        )
        current_df_data_column_snowflake_quoted_identifiers = (
            index_snowflake_quoted_identifiers
            + data_column_snowflake_quoted_identifiers
        )
        index_names = [
            extract_pandas_label_from_snowflake_quoted_identifier(identifier)
            for identifier in index_snowflake_quoted_identifiers
        ]
        ray_df.reset_index(
            inplace=True,
            allow_duplicates=True,
            names=index_names,
        )
        row_position_snowflake_quoted_identifier = (
            generate_snowflake_quoted_identifiers_helper(
                pandas_labels=[ROW_POSITION_COLUMN_LABEL],
                excluded=current_df_data_column_snowflake_quoted_identifiers,
                wrap_double_underscore=True,
            )[0]
        )
        row_position_pandas_label = (
            extract_pandas_label_from_snowflake_quoted_identifier(
                row_position_snowflake_quoted_identifier
            )
        )

        ray_df[row_position_pandas_label] = np.arange(len(ray_df))

        current_df_data_column_snowflake_quoted_identifiers.append(
            row_position_snowflake_quoted_identifier
        )

        with config_context(Backend="Ray"):
            ray_ds = pd.io.to_ray(ray_df)
        # Wait for all actors to finish writing
        list(
            pool.map_unordered(
                lambda actor, v: actor.write.remote(v),
                ray_ds.iter_batches(batch_size=None, batch_format="pandas"),
            )
        )

        with config_context(Backend="Snowflake"):
            snowpark_pandas_df = pd.read_snowflake(
                table_name, index_col=index_names, enforce_ordering=True
            )

        snowpark_pandas_df.sort_values(by=row_position_pandas_label, inplace=True)
        snowpark_pandas_df.drop(row_position_pandas_label, axis=1, inplace=True)
        # Drop the permanent table and reference only the snapshot
        pd.session.sql(f"DROP TABLE IF EXISTS {table_name}").collect()

        return SnowflakeQueryCompiler(
            InternalFrame.create(
                ordered_dataframe=snowpark_pandas_df._query_compiler._modin_frame.ordered_dataframe,
                data_column_pandas_labels=original_column_labels,
                data_column_pandas_index_names=original_column_index_names,
                data_column_snowflake_quoted_identifiers=data_column_snowflake_quoted_identifiers,
                index_column_pandas_labels=original_index_pandas_labels,
                index_column_snowflake_quoted_identifiers=index_snowflake_quoted_identifiers,
                data_column_types=None,
                index_column_types=None,
            )
        )

    def move_to_ray_helper(
        qc: "SnowflakeQueryCompiler",
    ) -> PandasQueryCompiler:  # pragma: no cover
        """
        Move the QueryCompiler to a Ray backend using the `ml.data.DataConnector`.

        Returns:
            A new Ray-backed PandasQueryCompiler
        """
        try:
            from snowflake.ml.data.data_connector import DataConnector  # type: ignore[import]
        except ImportError as e:  # pragma: no cover
            raise ImportError(
                "DataConnector is required for efficient transfer to Ray. "
                "Please `pip install snowflake-ml`."
            ) from e

        cached_qc = qc.cache_result()
        original_index_labels = qc.get_index_names()
        original_column_labels = qc.columns.tolist()
        snowflake_index_labels = [
            label if is_interactive() else unquote_name_if_quoted(label)
            for label in cached_qc._modin_frame.index_column_snowflake_quoted_identifiers
        ]
        snowflake_column_labels = [
            label if is_interactive() else unquote_name_if_quoted(label)
            for label in cached_qc._modin_frame.data_column_snowflake_quoted_identifiers
        ]
        snowpark_df = (
            cached_qc._modin_frame.ordered_dataframe.to_projected_snowpark_dataframe(
                sort=True
            )
        )
        # Drop all metadata (ordering, row position, row count) columns
        snowpark_df = snowpark_df.drop(
            snowpark_df.columns[
                len(snowflake_index_labels) + len(snowflake_column_labels) :
            ],
        )

        data_connector = DataConnector.from_dataframe(snowpark_df)
        ray_ds = data_connector.to_ray_dataset()
        with config_context(Backend="Ray"):
            ray_df = pd.io.from_ray(ray_ds)

        ray_df.set_index(
            snowflake_index_labels,
            drop=True,
            inplace=True,
        )
        ray_df.index.set_names(original_index_labels, inplace=True)
        ray_df.columns = original_column_labels

        return ray_df._query_compiler

except ImportError:

    RAY_REQUIRED_MESSAGE = (
        "Ray is required for this operation. Please `pip install modin[ray]`."
    )

    def move_from_ray_helper(ray_qc: BaseQueryCompiler, max_sessions: int):
        raise ImportError(RAY_REQUIRED_MESSAGE)  # pragma: no cover

    def move_to_ray_helper(qc: "SnowflakeQueryCompiler"):
        raise ImportError(RAY_REQUIRED_MESSAGE)  # pragma: no cover
