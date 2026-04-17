#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import pytest

from snowflake.snowpark._internal.analyzer.expression import (
    Attribute,
    Expression,
    Literal,
)
from snowflake.snowpark._internal.analyzer.metadata_utils import (
    _extract_inferable_attribute_names,
)
from snowflake.snowpark._internal.analyzer.unary_expression import Alias, Cast
from snowflake.snowpark.types import (
    ArrayType,
    DataType,
    DoubleType,
    IntegerType,
    MapType,
    StringType,
    StructType,
)


def test_none_input():
    assert _extract_inferable_attribute_names(None) == (None, None)


def test_plain_attribute_passthrough():
    attrs = [Attribute('"A"', IntegerType()), Attribute('"B"', StringType())]
    expected, resolved = _extract_inferable_attribute_names(attrs)
    assert len(expected) == 2
    assert len(resolved) == 2
    assert resolved[0].name == '"A"'
    assert resolved[0].datatype == IntegerType()
    assert resolved[1].name == '"B"'
    assert resolved[1].datatype == StringType()


def test_alias_attribute_resolved_from_parent():
    """Alias(Attribute) resolves type from from_attributes by name."""
    child = Attribute('"A"', DataType())
    projection = [Alias(child, '"X"')]
    from_attributes = [Attribute('"A"', IntegerType(), nullable=False)]

    expected, resolved = _extract_inferable_attribute_names(projection, from_attributes)
    assert resolved is not None
    assert len(resolved) == 1
    assert resolved[0].name == '"X"'
    assert resolved[0].datatype == IntegerType()


def test_alias_attribute_no_match_in_parent():
    """Alias(Attribute) with no matching name in from_attributes falls through
    to the Alias(Literal/Attribute)+datatype branch. With DataType() (dummy),
    it resolves but with a non-concrete type that downstream code rejects."""
    child = Attribute('"MISSING"', DataType())
    projection = [Alias(child, '"X"')]
    from_attributes = [Attribute('"A"', IntegerType())]

    expected, resolved = _extract_inferable_attribute_names(projection, from_attributes)
    assert resolved is not None
    assert len(resolved) == 1
    assert resolved[0].name == '"X"'
    assert type(resolved[0].datatype) is DataType


def test_alias_cast_scalar_type():
    """Alias(Cast(to=scalar_type)) resolves to the Cast target type."""
    inner = Attribute('"A"', DataType())
    cast = Cast(inner, IntegerType())
    projection = [Alias(cast, '"X"')]

    expected, resolved = _extract_inferable_attribute_names(projection)
    assert resolved is not None
    assert len(resolved) == 1
    assert resolved[0].name == '"X"'
    assert resolved[0].datatype == IntegerType()


@pytest.mark.parametrize(
    "structured_type",
    [
        ArrayType(IntegerType()),
        MapType(StringType(), IntegerType()),
        StructType(),
    ],
    ids=["ArrayType", "MapType", "StructType"],
)
def test_alias_cast_structured_type_skipped(structured_type):
    """Alias(Cast(to=structured_type)) is not inferred -- falls back to describe."""
    inner = Attribute('"A"', DataType())
    cast = Cast(inner, structured_type)
    projection = [Alias(cast, '"X"')]

    expected, resolved = _extract_inferable_attribute_names(projection)
    assert (expected, resolved) == (None, None)


def test_alias_cast_dummy_datatype_skipped():
    """Alias(Cast(to=DataType())) is not inferred -- base DataType is not concrete."""
    inner = Attribute('"A"', DataType())
    cast = Cast(inner, DataType())
    projection = [Alias(cast, '"X"')]

    expected, resolved = _extract_inferable_attribute_names(projection)
    assert (expected, resolved) == (None, None)


def test_alias_literal_with_known_type():
    """Alias(Literal) with concrete type resolves (existing behavior)."""
    lit = Literal(42, IntegerType())
    projection = [Alias(lit, '"X"')]
    projection[0].datatype = IntegerType()

    expected, resolved = _extract_inferable_attribute_names(projection)
    assert resolved is not None
    assert resolved[0].name == '"X"'
    assert resolved[0].datatype == IntegerType()


def test_unresolvable_expression():
    """Alias with unknown child type -> (None, None)."""

    class UnknownExpr(Expression):
        pass

    projection = [Alias(UnknownExpr(), '"X"')]

    expected, resolved = _extract_inferable_attribute_names(projection)
    assert (expected, resolved) == (None, None)


def test_mixed_resolvable_and_unresolvable():
    """If any expression is unresolvable, entire result is (None, None)."""
    good = Attribute('"A"', IntegerType())

    class UnknownExpr(Expression):
        pass

    bad = Alias(UnknownExpr(), '"B"')
    projection = [good, bad]

    expected, resolved = _extract_inferable_attribute_names(projection)
    assert (expected, resolved) == (None, None)


def test_multiple_cast_types():
    """Multiple Alias(Cast) with different scalar types all resolve."""
    projection = [
        Alias(Cast(Attribute('"A"', DataType()), IntegerType()), '"X"'),
        Alias(Cast(Attribute('"B"', DataType()), StringType()), '"Y"'),
        Alias(Cast(Attribute('"C"', DataType()), DoubleType()), '"Z"'),
    ]

    expected, resolved = _extract_inferable_attribute_names(projection)
    assert resolved is not None
    assert len(resolved) == 3
    assert resolved[0].datatype == IntegerType()
    assert resolved[1].datatype == StringType()
    assert resolved[2].datatype == DoubleType()


def test_duplicate_from_attributes_raises_assertion():
    """from_attributes with duplicate names should trigger AssertionError."""
    projection = [Attribute('"A"', IntegerType())]
    from_attributes = [
        Attribute('"X"', IntegerType()),
        Attribute('"X"', StringType()),
    ]

    with pytest.raises(AssertionError, match="Unexpected duplicate column names"):
        _extract_inferable_attribute_names(projection, from_attributes)
