#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
from collections import defaultdict

import pytest
import uuid
import copy
from snowflake.snowpark._internal.utils import (
    ExprAliasUpdateDict,
    str_contains_alphabet,
    get_sorted_key_for_version,
)


def test_expr_alias_update_dict():
    expr_dict = ExprAliasUpdateDict()
    key1, key2, key3 = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()

    # Test setting and getting values
    expr_dict[key1] = "alias1"
    assert (
        expr_dict[key1] == "alias1"
        and expr_dict.was_updated_due_to_inheritance(key1) is False
    )
    expr_dict[key2] = ("alias2", True)
    assert (
        expr_dict[key2] == "alias2"
        and expr_dict.was_updated_due_to_inheritance(key2) is True
    )
    expr_dict[key3] = ("alias3", False)
    assert (
        expr_dict[key3] == "alias3"
        and expr_dict.was_updated_due_to_inheritance(key3) is False
    )

    # Test invalid value
    with pytest.raises(ValueError, match=r"Value must be a tuple of \(str, bool\)"):
        expr_dict[key1] = ("alias3", "not a bool")

    # Test get with default
    new_key = uuid.uuid4()
    assert expr_dict.get(new_key, "default_alias") == "default_alias"
    assert expr_dict.was_updated_due_to_inheritance(new_key) is False

    # Test items and values
    expr_dict[key1] = "alias4"
    items = dict(expr_dict.items())
    assert items[key1] == "alias4"
    assert set(expr_dict.values()) == {"alias3", "alias4", "alias2"}

    # Test update
    dict2 = ExprAliasUpdateDict()
    key3 = uuid.uuid4()
    dict2[key3] = ("alias5", True)
    expr_dict.update(dict2)
    assert expr_dict[key3] == "alias5"
    assert expr_dict.was_updated_due_to_inheritance(key3) is True

    # Test copy and deepcopy
    copied_dict = expr_dict.copy()
    deep_copied_dict = copy.deepcopy(expr_dict)
    assert (
        copied_dict[key3] == "alias5"
        and copied_dict.was_updated_due_to_inheritance(key3) is True
    )
    assert (
        deep_copied_dict[key3] == "alias5"
        and deep_copied_dict.was_updated_due_to_inheritance(key3) is True
    )
    assert copied_dict is not expr_dict
    assert deep_copied_dict is not expr_dict

    # Test defaultdict update behavior
    df_alias_dict1, df_alias_dict2 = defaultdict(ExprAliasUpdateDict), defaultdict(
        ExprAliasUpdateDict
    )
    df_alias_1, df_alias_2 = "df_1", "df_2"
    col_key_1, col_key_2 = "col_1", "col_2"

    df_alias_dict1[df_alias_1][col_key_1] = ("alias1", True)
    df_alias_dict1[df_alias_1][col_key_2] = ("alias2", True)

    # only 1 df alias with 2 col aliases in the df_alias_dict1
    assert len(df_alias_dict1) == 1 and len(df_alias_dict1[df_alias_1]) == 2

    df_alias_dict2[df_alias_2][col_key_1] = ("alias3", False)
    df_alias_dict2[df_alias_1][col_key_2] = ("alias4", False)

    df_alias_dict1.update(df_alias_dict2)

    # after update, there should be 2 df alias replacing the previous one
    assert (
        len(df_alias_dict1) == 2
        and len(df_alias_dict1[df_alias_1]) == 1
        and len(df_alias_dict1[df_alias_2]) == 1
    )
    assert (
        df_alias_dict1[df_alias_1][col_key_2] == "alias4"
        and df_alias_dict1[df_alias_1].was_updated_due_to_inheritance(col_key_2)
        is False
        and df_alias_dict1[df_alias_2][col_key_1] == "alias3"
        and df_alias_dict1[df_alias_2].was_updated_due_to_inheritance(col_key_1)
        is False
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
