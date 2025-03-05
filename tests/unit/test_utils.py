#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

from snowflake.snowpark._internal.utils import (
    str_contains_alphabet,
    get_sorted_key_for_version,
)


def test_str_contains_alphabet():
    assert str_contains_alphabet("1a1") is True
    assert str_contains_alphabet("abc") is True
    assert str_contains_alphabet("123") is False
    assert str_contains_alphabet("12b34") is True
    assert str_contains_alphabet(".") is False
    assert str_contains_alphabet("1.2.3") is False


def test_get_sorted_key_for_version():
    assert get_sorted_key_for_version("1.11.1a1") == (1, 11, -1)
    assert get_sorted_key_for_version("1.2.3") == (1, 2, 3)
    assert get_sorted_key_for_version("2.0.0") == (2, 0, 0)
    assert get_sorted_key_for_version("3.4a.5") == (3, -1, 5)
    assert get_sorted_key_for_version("10.20.30") == (10, 20, 30)
    assert get_sorted_key_for_version("4.5.6b7") == (4, 5, -1)
    assert get_sorted_key_for_version("7.8.9c") == (7, 8, -1)
