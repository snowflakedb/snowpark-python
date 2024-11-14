#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

"""This package contains all Snowpark pandas exceptions."""
from enum import Enum

from snowflake.snowpark.exceptions import SnowparkClientException


class SnowparkPandasErrorCode(Enum):
    # Snowpark pandas specific error starts with error code 3XXX
    GENERAL_SQL_EXCEPTION = 3000


# TODO (SNOW-840704): Wrap snowpark SQL exception with more details
#   Snowpark pandas specific error starts with error code 3XXX
class SnowparkPandasException(SnowparkClientException):
    """
    Base Snowpark pandas exception class.
    """

    pass
