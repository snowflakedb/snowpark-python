#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

"""
The module uses the out-of-band telemetry service defined snowflake connector to collect telemetry information
"""

from snowflake.connector.telemetry_oob import (
    TelemetryField,
    TelemetryLogEvent,
    TelemetryService,
    generate_telemetry_data_dict,
)

ENABLE_TELEMETRY = True


def local_test_not_implemented_error(
    not_implemented_method_name=None,
    not_implemented_expression_name=None,
    extra_info=None,
):
    """
    send telemetry information of not implemented feature to OOB telemetry service

    """
    global enable_telemetry
    if not enable_telemetry:
        return

    dict_data = {TelemetryField.KEY_SOURCE.value: "PythonSnowparkLocalTesting"}
    if not_implemented_method_name:
        dict_data["not_implemented_method_name"] = not_implemented_method_name
    if not_implemented_expression_name:
        dict_data["not_implemented_expression_name"] = not_implemented_expression_name
    if extra_info:
        dict_data["extra_info"] = extra_info

    TelemetryService.get_instance().add(
        TelemetryLogEvent(
            name="PythonSnowparkLocalTestingNotImplementedError",
            tags={
                TelemetryField.KEY_OOB_EVENT_TYPE.value: "PythonSnowparkLocalTestingNotImplementedError"
            },
            urgent=False,
            value=generate_telemetry_data_dict(
                from_dict=dict_data,
                is_oob_telemetry=True,
            ),
        )
    )
    TelemetryService.get_instance().flush()
