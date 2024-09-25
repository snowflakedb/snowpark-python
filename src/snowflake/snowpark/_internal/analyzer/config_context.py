#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

from typing import Any


class ConfigContext:
    """Class to help reading a snapshot of configuration attributes from a session object.

    On instantiation, this object stores the configuration from the session object
    and returns the stored configuration attributes when requested.
    """

    def __init__(self, session) -> None:
        self.session = session
        self.configs = {
            "_query_compilation_stage_enabled",
            "cte_optimization_enabled",
            "eliminate_numeric_sql_value_cast_enabled",
            "large_query_breakdown_complexity_bounds",
            "large_query_breakdown_enabled",
        }
        self._create_snapshot()

    def __getattr__(self, name: str) -> Any:
        if name in self.configs:
            return getattr(self.session, name)
        raise AttributeError(f"ConfigContext has no attribute {name}")

    def _create_snapshot(self) -> "ConfigContext":
        """Reads the configuration attributes from the session object and stores them
        in the context object.
        """
        for name in self.configs:
            setattr(self, name, getattr(self.session, name))
        return self
