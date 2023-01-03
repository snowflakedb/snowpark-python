#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#

import logging
import pickle
from unittest import mock

import pytest

from snowflake.snowpark import Session
from snowflake.snowpark._internal.udf_utils import (
    cleanup_failed_permanent_registration,
    generate_python_code,
    get_error_message_abbr,
    pickle_function,
)
from snowflake.snowpark._internal.utils import TempObjectType


def test_get_error_message_abbr_exception():
    with pytest.raises(ValueError, match="Expect FUNCTION of PROCEDURE"):
        get_error_message_abbr(TempObjectType.VIEW)


def test_cleanup_failed_permanent_registration_exception(
    mock_server_connection, caplog
):
    fake_session = Session(mock_server_connection)
    with mock.patch.object(
        fake_session, "_run_query", side_effect=Exception("fake exception")
    ):
        with caplog.at_level(logging.WARNING):
            cleanup_failed_permanent_registration(
                fake_session, "fake_upload_file_stage_location", "fake_stage_location"
            )
    assert "Failed to clean uploaded file" in caplog.text


def test_pickle_function_exception():
    with mock.patch("cloudpickle.dumps", side_effect=TypeError("fake error")):
        with pytest.raises(TypeError, match="fake error"):
            pickle_function(lambda: None)

    with mock.patch(
        "cloudpickle.dumps", side_effect=pickle.PicklingError("fake error")
    ):
        with pytest.raises(
            pickle.PicklingError, match="fake error.*save the unpicklable object"
        ):
            pickle_function(lambda: None)


def test_generate_python_code_exception():
    with mock.patch(
        "snowflake.snowpark._internal.code_generation.generate_source_code",
        side_effect=Exception("fake error"),
    ):
        generated_code, _ = generate_python_code(
            func=lambda: None,
            arg_names=[],
            object_type=TempObjectType.FUNCTION,
            is_pandas_udf=False,
            is_dataframe_input=False,
            source_code_display=True,
        )
        assert "Source code comment could not be generated" in generated_code
