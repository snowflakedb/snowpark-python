#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

from typing import Dict
import snowflake.snowpark
import requests


class RetryWithTokenRefreshAdapter(requests.adapters.HTTPAdapter):
    def __init__(
        self,
        session_instance: "snowflake.snowpark.Session",
        header: Dict,
        max_retries: int = 3,
    ) -> None:
        super().__init__()
        self.snowpark_session = session_instance
        self.max_retries = max_retries
        self.header = header
        self.retryable_status_code = [401]

    def send(self, request, **kwargs):
        """Send request with retry logic and token refresh on failure"""
        for attempt in range(self.max_retries + 1):
            try:
                request.headers.update(self.header)

                response = super().send(request, **kwargs)

                # If successful, return the response
                if (
                    response.status_code in self.retryable_status_code
                    and attempt < self.max_retries
                ):
                    self.header = (
                        self.snowpark_session._get_external_telemetry_auth_token()
                    )
                    continue
                else:
                    return response

            except (requests.exceptions.RequestException, Exception) as e:
                if attempt < self.max_retries:
                    self.header = (
                        self.snowpark_session._get_external_telemetry_auth_token()
                    )
                    continue
                else:
                    # Re-raise the exception if we've exhausted retries
                    raise e
