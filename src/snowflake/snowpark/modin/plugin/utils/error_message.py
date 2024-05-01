#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

# Licensed to Modin Development Team under one or more contributor license agreements.
# See the NOTICE file distributed with this work for additional information regarding
# copyright ownership.  The Modin Development Team licenses this file to you under the
# Apache License, Version 2.0 (the "License"); you may not use this file except in
# compliance with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under
# the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF
# ANY KIND, either express or implied. See the License for the specific language
# governing permissions and limitations under the License.

# Code in this file may constitute partial or total reimplementation, or modification of
# existing code originally distributed by the Modin project, under the Apache License,
# Version 2.0.
from logging import getLogger
from typing import NoReturn

logger = getLogger(__name__)


class ErrorMessage:
    # Only print full ``default to pandas`` warning one time.
    printed_default_to_pandas = False
    printed_warnings: set[int] = set()  # Set of hashes of printed warnings

    @classmethod
    def not_implemented(cls, message: str = "") -> NoReturn:  # pragma: no cover
        if message == "":
            message = "This functionality is not yet available in Snowpark pandas API"
        logger.debug(f"NotImplementedError: {message}")
        raise NotImplementedError(message)

    @staticmethod
    def method_not_implemented_error(
        name: str, class_: str
    ) -> None:  # pragma: no cover
        """
        Invokes ``ErrorMessage.not_implemented()`` with specified method name and class.

        Parameters
        ----------
        name: str
            The method that is not implemented.
        class_: str
            The class of Snowpark pandas function associated with the method.
        """
        message = f"{name} is not yet implemented for {class_}"
        ErrorMessage.not_implemented(message)

    # TODO SNOW-840704: using Snowpark pandas exception class for the internal error
    @classmethod
    def internal_error(
        cls, failure_condition: bool, extra_log: str = ""
    ) -> None:  # pragma: no cover
        if failure_condition:
            raise Exception(f"Internal Error: {extra_log}")

    @classmethod
    def catch_bugs_and_request_email(
        cls, failure_condition: bool, extra_log: str = ""
    ) -> None:  # pragma: no cover
        if failure_condition:
            logger.info(f"Modin Error: Internal Error: {extra_log}")
            raise Exception(
                "Internal Error. "
                + "Please visit https://github.com/modin-project/modin/issues "
                + "to file an issue with the traceback and the command that "
                + "caused this error. If you can't file a GitHub issue, "
                + f"please email bug_reports@modin.org.\n{extra_log}"
            )
