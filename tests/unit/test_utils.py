#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import pytest
import uuid
import copy
from snowflake.snowpark._internal.utils import (
    ExprAliasUpdateDict,
)  # Replace 'your_module' with the actual module name


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
