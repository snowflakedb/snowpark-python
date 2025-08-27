#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
from snowflake.snowpark.modin.plugin._internal.telemetry import ModinTelemetrySender

# exported to allow for mocking of the telemetry in a consistent way
__all__ = ["ModinTelemetrySender"]
