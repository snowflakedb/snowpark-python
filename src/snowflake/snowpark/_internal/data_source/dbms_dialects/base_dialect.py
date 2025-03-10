#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

from abc import ABC, abstractmethod
from snowflake.snowpark.types import StructType
import logging

logger = logging.getLogger(__name__)


class BaseDialect(ABC):
    @abstractmethod
    def generate_select_query(self, table_or_query: str, schema: StructType) -> str:
        pass
