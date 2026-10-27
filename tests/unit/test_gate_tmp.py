#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import sys


def test_gate_tmp_fails_on_macos():
    assert sys.platform != "darwin"
