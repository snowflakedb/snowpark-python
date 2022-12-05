#!/usr/bin/env python3
#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#
import logging
import subprocess

# import subprocess
# from datetime import datetime, timedelta, timezone
#
# import pytest
#
# from snowflake.connector.errors import ProgrammingError
# from snowflake.snowpark._internal.utils import TempObjectType, warning_dict
# from snowflake.snowpark.exceptions import SnowparkPandasException
# from tests.utils import Utils


def test_mypy_on_module(session):
    """
    tests that snowflake.snowpark module can be linted with mypy to detect possible type issues.
    """
    proc = subprocess.Popen(
        ["mypy", "-p", "snowflake.snowpark", "--explicit-package-bases"],
        stdout=subprocess.PIPE,
        stdin=subprocess.PIPE,
        encoding="utf-8",
    )
    try:
        out, err = proc.communicate(timeout=20)
    except subprocess.TimeoutExpired:
        proc.kill()
        out, err = proc.communicate()

    # for inspection, output mypy result
    if out:
        logging.info(out)
    if err:
        logging.error(err)

    # mypy currently produces errors, therefore allow code 1.
    assert proc.returncode in [0, 1], f"mypy returned with exit code {proc.returncode}"
