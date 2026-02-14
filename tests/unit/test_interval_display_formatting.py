#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import pytest

from snowflake.snowpark._internal.type_utils import (
    format_year_month_interval_for_display,
)
from snowflake.snowpark.types import YearMonthIntervalType


@pytest.mark.parametrize("cell", ["+5-00", "+5", "-5-00", "-5"])
def test_format_year_month_interval_for_display_accepts_single_field_year(cell):
    # Connector behavior change: single-field intervals may come back as "+5" / "-5"
    # instead of "+5-00" / "-5-00". We should accept both.
    formatted = format_year_month_interval_for_display(
        cell, YearMonthIntervalType.YEAR, YearMonthIntervalType.YEAR
    )
    assert formatted in {"INTERVAL '5' YEAR", "INTERVAL '-5' YEAR"}


@pytest.mark.parametrize("cell", ["+5-00", "+5"])
def test_format_year_month_interval_for_display_year_to_month_defaults_month_to_zero(
    cell,
):
    assert (
        format_year_month_interval_for_display(
            cell, YearMonthIntervalType.YEAR, YearMonthIntervalType.MONTH
        )
        == "INTERVAL '5-0' YEAR TO MONTH"
    )


@pytest.mark.parametrize("cell", ["+5", "-5"])
def test_format_year_month_interval_for_display_month_only_single_field(cell):
    expected = "INTERVAL '5' MONTH" if cell == "+5" else "INTERVAL '-5' MONTH"
    assert (
        format_year_month_interval_for_display(
            cell, YearMonthIntervalType.MONTH, YearMonthIntervalType.MONTH
        )
        == expected
    )
