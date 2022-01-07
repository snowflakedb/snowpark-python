#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#
"""This package contains all Snowpark client-side exceptions."""
from typing import Optional


class SnowparkClientException(Exception):
    """Base Snowpark exception class"""

    def __init__(self, message: str, error_code: Optional[str] = None):
        self.message = message
        self.error_code = error_code
        self.telemetry_message = message

    # TODO: SNOW-363951 handle telemetry


class _SnowparkInternalException(SnowparkClientException):
    """Exception for internal errors. For internal use only.

    Includes all error codes in 10XX (where XX is 0-9).
    """

    pass


class SnowparkDataframeException(SnowparkClientException):
    """Exception for dataframe related errors.

    Includes all error codes in range 11XX (where XX is 0-9).

    This exception is specifically raised for error codes: 1104, 1107, 1108, 1109.
    """

    pass


class SnowparkPlanException(SnowparkClientException):
    """Exception for plan analysis errors.

    Includes all error codes in range 12XX (where XX is 0-9).

    This exception is specifically raised for error codes: 1200, 1201, 1202, 1205.
    """

    pass


class SnowparkSQLException(SnowparkClientException):
    """Exception for errors related to the executed SQL statement that was generated
    from the Snowflake plan.

    Includes all error codes in range 13XX (where XX is 0-9).

    This exception is specifically raised for error codes: 1300.
    """

    pass


class SnowparkServerException(SnowparkClientException):
    """Exception for miscellaneous related errors.

    Includes all error codes in range 14XX (where XX is 0-9).
    """

    pass


class SnowparkGeneralException(SnowparkClientException):
    """Exception for general exceptions.

    Includes all error codes in range 15XX (where XX is 0-9).
    """

    pass


class SnowparkColumnException(SnowparkDataframeException):
    """Exception for column related errors during dataframe operations.

    Includes error codes: 1100, 1101, 1102, 1105.
    """

    pass


class SnowparkJoinException(SnowparkDataframeException):
    """Exception for join related errors during dataframe operations.

    Includes error codes: 1103, 1110, 1111, 1112.
    """

    pass


class SnowparkDataframeReaderException(SnowparkDataframeException):
    """Exception for dataframe reader errors.

    Includes error codes: 1106.
    """

    pass


class SnowparkPandasException(SnowparkDataframeException):
    """Exception for pandas related errors.

    Includes error codes: 1106.
    """

    pass


class SnowparkCreateViewException(SnowparkPlanException):
    """Exception for errors while trying to create a view.

    Includes error codes: 1203, 1204, 1205, 1206.
    """

    pass


class SnowparkSQLAmbiguousJoinException(SnowparkSQLException):
    """Exception for ambiguous joins that are created from the
    translated SQL statement.

    Includes error codes: 1303.
    """

    pass


class SnowparkSQLInvalidIdException(SnowparkSQLException):
    """Exception for having an invalid ID (usually a missing ID)
    that are created from the translated SQL statement.

    Includes error codes: 1302.
    """

    pass


class SnowparkSQLUnexpectedAliasException(SnowparkSQLException):
    """Exception for having an unexpected alias that are created
    from the translated SQL statement.

    Includes error codes: 1301.
    """

    pass


class SnowparkSessionException(SnowparkServerException):
    """Exception for any session related errors.

    Includes error codes: 1402, 1403, 1404, 1405.
    """

    pass


class SnowparkMissingDbOrSchemaException(SnowparkServerException):
    """Exception for when a schema or database is missing in the session connection.
    These are needed to run queries.

    Includes error codes: 1400.
    """

    pass


class SnowparkQueryCancelledException(SnowparkServerException):
    """Exception for when we are trying to interact with a cancelled query.

    Includes error codes: 1401.
    """

    pass


class SnowparkFetchDataException(SnowparkServerException):
    """Exception for when we are trying to fetch data from Snowflake.

    Includes error codes: 1406.
    """

    pass


class SnowparkUploadUdfFileException(SnowparkServerException):
    """Exception for when we are trying to upload UDF files to the server.

    Includes error codes: 1407.
    """

    pass


class SnowparkInvalidObjectNameException(SnowparkGeneralException):
    """Exception for inputting an invalid object name. Checked locally.

    This exception is specifically raised for error codes: 1500.
    """

    pass
