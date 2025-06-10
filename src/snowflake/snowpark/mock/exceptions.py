#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

from typing import Optional

from snowflake.snowpark.exceptions import SnowparkSQLException


class SnowparkLocalTestingException(SnowparkSQLException):
    """Exception for errors related to the local testing execution of the Mock Snowflake plan."""

    @classmethod
    def raise_from_error(
        cls, error: BaseException, error_message: Optional[str] = None
    ):
        if isinstance(error, (SnowparkLocalTestingException, NotImplementedError)):
            raise error

        raise cls(
            message=error_message
            or f'[Local Testing] Encountered exception "{type(error).__name__}" with'
            f' message "{str(error)}" during execution, please check '
            f"the error traceback for detailed information."
        ) from error
