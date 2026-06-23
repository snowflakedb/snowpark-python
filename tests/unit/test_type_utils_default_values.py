#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

"""Regression tests for evaluating UDF default values reconstructed from source.

When registering a UDF from a source file, default parameter values are statically
reconstructed into strings by ``retrieve_func_defaults_from_source`` (including
``ast.Call``/``ast.Attribute``/``ast.Name`` nodes) and then evaluated by
``python_value_str_to_object``. That evaluation used to call a bare ``eval()``,
which would evaluate arbitrary expressions instead of just the documented default
values, so a source file such as::

    def foo(x: int = __import__('os').system('echo hi')) -> int: ...

would run that expression while reading the default during registration.

``safe_eval_default_value`` replaces ``eval`` with an allowlist evaluator that
supports only literals, containers, and a fixed set of constructors
(``datetime``/``decimal``/``bytes``/``bytearray``/...). These tests guard two
properties simultaneously:

1. unsupported expressions are rejected (never evaluated), and
2. the documented constructor defaults keep working (a naive ``ast.literal_eval``
   replacement would silently break these).
"""

import datetime
import decimal

import pytest

from snowflake.snowpark._internal.type_utils import (
    python_value_str_to_object,
    retrieve_func_defaults_from_source,
    safe_eval_default_value,
)
from snowflake.snowpark.types import (
    BinaryType,
    DateType,
    DecimalType,
    IntegerType,
    TimestampType,
    TimeType,
)

UNSUPPORTED_EXPRESSIONS = [
    "__import__('os').system('echo hi')",
    "os.system('id')",
    "open('/etc/passwd').read()",
    "eval('1')",
    "exec('x=1')",
    "().__class__.__base__",
    "().__class__",
    "globals()",
    "[].__class__",
]


class TestSafeEvalDefaultValue:
    @pytest.mark.parametrize("expr", UNSUPPORTED_EXPRESSIONS)
    def test_unsupported_expression_is_rejected(self, expr):
        """Unsupported expressions must raise instead of being evaluated."""
        with pytest.raises(TypeError):
            safe_eval_default_value(expr)

    def test_unsupported_default_value_not_evaluated(self, tmp_path):
        """An unsupported default value expression must NOT be evaluated."""
        marker_file = tmp_path / "side_effect.txt"
        expr = f"__import__('os').system('touch {marker_file}')"

        with pytest.raises(TypeError):
            python_value_str_to_object(expr, IntegerType())

        assert not marker_file.exists(), (
            "An unsupported default value expression was evaluated: the marker file "
            "was created, proving the expression ran instead of being rejected."
        )

    def test_unsupported_default_in_full_register_flow(self, tmp_path):
        """End-to-end: an unsupported default in a source file must not be evaluated."""
        marker_file = tmp_path / "side_effect_e2e.txt"

        source = f"""
def sample_udf(x: int = __import__('os').system('touch {marker_file}')) -> int:
    return x
"""
        # retrieve_func_defaults_from_source parses the AST and reconstructs the
        # default value as a string like "__import__('os').system('touch ...')"
        defaults = retrieve_func_defaults_from_source(
            "fake.py", "sample_udf", _source=source
        )

        assert defaults is not None, "Function should be found in source"

        for default_str in defaults:
            if default_str is not None:
                with pytest.raises(TypeError):
                    python_value_str_to_object(default_str, IntegerType())

        assert not marker_file.exists(), (
            "An unsupported default value expression was evaluated via the "
            "register_from_file flow: the marker file was created."
        )

    def test_safe_literal_defaults_still_work(self):
        """Legitimate literal defaults must continue to parse correctly."""
        assert python_value_str_to_object("42", IntegerType()) == 42
        assert python_value_str_to_object("-1", IntegerType()) == -1
        assert python_value_str_to_object("None", IntegerType()) is None

    def test_legitimate_constructor_defaults_still_work(self):
        """Constructor-based defaults must keep working.

        These are exactly the cases a naive ``ast.literal_eval`` replacement would
        silently break, so this is the key non-regression guard for the fix.
        """
        assert python_value_str_to_object(
            "decimal.Decimal('3.14')", DecimalType()
        ) == decimal.Decimal("3.14")
        assert python_value_str_to_object(
            "bytearray('one', 'utf-8')", BinaryType()
        ) == bytearray("one", "utf-8")
        assert python_value_str_to_object(
            "datetime.date(2024, 4, 1)", DateType()
        ) == datetime.date(2024, 4, 1)
        assert python_value_str_to_object(
            "datetime.time(12, 0, second=20, tzinfo=datetime.timezone.utc)", TimeType()
        ) == datetime.time(12, 0, second=20, tzinfo=datetime.timezone.utc)
        assert python_value_str_to_object(
            "datetime.datetime(2024, 4, 1, 12, 0, 20)", TimestampType()
        ) == datetime.datetime(2024, 4, 1, 12, 0, 20)

    @pytest.mark.parametrize(
        "expr,expected",
        [
            # Literals / containers (int, float, bool, str, list, dict).
            ("5", 5),
            ("1.5", 1.5),
            ("True", True),
            ("'abc'", "abc"),
            ("[1, 2, 3]", [1, 2, 3]),
            ("{'a': 1}", {"a": 1}),
            ("b'one'", b"one"),
            # Builtin value constructors (explicit-constructor spelling). Only the
            # builtins that map to a Snowflake type are allowed.
            ("int('5')", 5),
            ("float('1.5')", 1.5),
            ("bool(1)", True),
            ("str(3)", "3"),
            ("list((1, 2))", [1, 2]),
            ("tuple([1, 2])", (1, 2)),
            ("dict(a=1)", {"a": 1}),
            # Non-literal value constructors permitted by the documented type mapping.
            ("bytes(3)", bytes(3)),
            ("bytearray('one', 'utf-8')", bytearray("one", "utf-8")),
            ("decimal.Decimal('3.14')", decimal.Decimal("3.14")),
            ("datetime.date(2024, 4, 1)", datetime.date(2024, 4, 1)),
            (
                "datetime.time(12, 0, second=20, tzinfo=datetime.timezone.utc)",
                datetime.time(12, 0, second=20, tzinfo=datetime.timezone.utc),
            ),
            (
                "datetime.datetime(2024, 4, 1, 12, 0, 20)",
                datetime.datetime(2024, 4, 1, 12, 0, 20),
            ),
            # Timezone-aware constructors (``datetime.timezone.utc`` as an argument).
            (
                "datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc)",
                datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc),
            ),
            # Nested constructor calls (a ``timedelta`` inside a ``timezone``).
            (
                "datetime.timezone(datetime.timedelta(hours=5))",
                datetime.timezone(datetime.timedelta(hours=5)),
            ),
            # Unary minus and negative constructor arguments.
            ("-5", -5),
            ("datetime.timedelta(days=-1)", datetime.timedelta(days=-1)),
            # Containers nesting allowed constructor calls.
            (
                "[datetime.date(2024, 1, 1), datetime.date(2024, 1, 2)]",
                [datetime.date(2024, 1, 1), datetime.date(2024, 1, 2)],
            ),
            (
                "{'d': datetime.date(2024, 1, 1)}",
                {"d": datetime.date(2024, 1, 1)},
            ),
        ],
    )
    def test_documented_value_forms_allowed(self, expr, expected):
        """Every documented value form (literals + constructors) evaluates."""
        assert safe_eval_default_value(expr) == expected

    @pytest.mark.parametrize(
        "expr,expected",
        [
            ("datetime.timezone.utc", datetime.timezone.utc),
            ("datetime.date.max", datetime.date.max),
            ("datetime.date.min", datetime.date.min),
            ("datetime.datetime.min", datetime.datetime.min),
            ("datetime.datetime.max", datetime.datetime.max),
        ],
    )
    def test_supported_type_instance_references_allowed(self, expr, expected):
        """Bare references to instances of supported types are permitted."""
        assert safe_eval_default_value(expr) == expected

    @pytest.mark.parametrize(
        "expr,expected",
        [
            # Deterministic methods on supported datetime/decimal types.
            ("datetime.datetime.fromtimestamp(0)", datetime.datetime.fromtimestamp(0)),
            (
                "datetime.date(2024, 1, 1).replace(year=2025)",
                datetime.date(2025, 1, 1),
            ),
            ("decimal.Decimal('1').sqrt()", decimal.Decimal("1")),
            ("decimal.Decimal(10).sqrt()", decimal.Decimal(10).sqrt()),
            (
                "datetime.timedelta(days=1).total_seconds()",
                datetime.timedelta(days=1).total_seconds(),
            ),
            # Method on a supported-type instance reached via a bare reference.
            (
                "datetime.timezone.utc.utcoffset(None)",
                datetime.timezone.utc.utcoffset(None),
            ),
        ],
    )
    def test_supported_type_method_calls_allowed(self, expr, expected):
        """Non-dunder methods on datetime/decimal types are permitted."""
        assert safe_eval_default_value(expr) == expected

    @pytest.mark.parametrize(
        "expr,expected_type",
        [
            ("datetime.datetime.now()", datetime.datetime),
            ("datetime.date.today()", datetime.date),
            # Factory method with a timezone argument.
            ("datetime.datetime.now(datetime.timezone.utc)", datetime.datetime),
            ("datetime.datetime.now(tz=datetime.timezone.utc)", datetime.datetime),
            # Chained: method on the result of a factory method.
            ("datetime.datetime.now().date()", datetime.date),
        ],
    )
    def test_supported_type_factory_methods_allowed(self, expr, expected_type):
        """Factory/classmethods (non-deterministic value) on supported types work."""
        assert isinstance(safe_eval_default_value(expr), expected_type)

    @pytest.mark.parametrize(
        "expr",
        [
            # Module-qualified calls into modules that are not part of the documented
            # default-value type mapping. Only the documented types are supported, so
            # the whole module namespace is rejected rather than enumerated.
            'pandas.Timestamp("2024-01-01")',
            "numpy.float64(1.5)",
            'pandas.read_pickle("x")',
            'numpy.load("x")',
            "sys.maxsize",  # arbitrary module attribute
            # Dunder access/calls on a supported-type result are still rejected.
            "datetime.datetime.now().__class__",
            "datetime.datetime.now().__class__()",
            "datetime.datetime.fromtimestamp(0).__reduce__()",
            # Bare references to a module or class (only *instances* are allowed).
            "datetime",
            "datetime.datetime",
            "datetime.timezone",
            "decimal.Decimal",
            # A method referenced but not called is not an instance of a supported type.
            "datetime.datetime.now",
            # Unsupported expression node kinds. These never reach
            # ``safe_eval_default_value`` in practice either: ``parse_default_value``
            # in ``retrieve_func_defaults_from_source`` only reconstructs
            # Constant/Name/Attribute/Call/Tuple/List/Dict, so the original code
            # rejected them at reconstruction too (i.e. they are not regressions).
            "lambda: 1",  # lambda
            "1 if True else 2",  # conditional expression (IfExp)
            "60 * 60",  # arithmetic (BinOp) is not constant-folded
            "[x for x in (1, 2)]",  # list comprehension (ListComp, not a List literal)
            # Valid Python values that don't map to a Snowflake type.
            "complex(1, 2)",
            "set([1, 2])",
            "frozenset([1])",
        ],
    )
    def test_non_canonical_forms_rejected(self, expr):
        """Forms outside the documented type mapping are rejected, not evaluated."""
        with pytest.raises(TypeError):
            safe_eval_default_value(expr)
