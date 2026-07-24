#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import inspect
from typing import Union

import pytest

from snowflake.snowpark import Column
from snowflake.snowpark._internal.analyzer.table_function import (
    NamedArgumentsTableFunction,
    PosArgumentsTableFunction,
)
from snowflake.snowpark.functions import (
    approx_percentile,
    approx_percentile_accumulate,
    approx_percentile_combine,
    approx_percentile_estimate,
    col,
    corr,
    covar_pop,
    covar_samp,
    get_ignore_case,
    get_path,
    lit,
    object_keys,
    sha2,
    typeof,
    xmlget,
)
from snowflake.snowpark.table_function import _create_table_function_expression
from snowflake.snowpark.functions import (
    _python_obj_to_sql_literal,
    ai_extract,
)
from snowflake.snowpark._internal.analyzer.analyzer_utils import function_expression


@pytest.mark.parametrize(
    "func",
    [
        xmlget,
        typeof,
        get_ignore_case,
        object_keys,
        get_path,
        approx_percentile,
        approx_percentile_accumulate,
        approx_percentile_estimate,
        approx_percentile_combine,
        corr,
        covar_pop,
        covar_samp,
    ],
)
def test_funcs_negative(func):
    signature = inspect.signature(func)
    params = list(signature.parameters.values())
    for i in range(
        len(params)
    ):  # Pass an integer (invalid parameter type) as the i-th parameter for every iteration.
        param_values = [1] * len(params)
        for j in range(len(params)):
            if i != j and params[j].annotation == Union[Column, str]:
                param_values[j] = lit(1)  # pass a value of type Column
        if (
            params[j].annotation == Union[Column, str]
        ):  # it should be Column or str, but given 1
            with pytest.raises(TypeError) as ex_info:
                func(*param_values)
            assert (
                f"'{func.__name__.upper()}' expected Column or str, got: {int}"
                in str(ex_info)
            )


def test_create_table_function_expression_args():
    function_expression = _create_table_function_expression(
        "func_name", lit("v1"), lit("v2")
    )
    assert isinstance(function_expression, PosArgumentsTableFunction)
    assert function_expression.func_name == "func_name"
    assert [arg.value for arg in function_expression.args] == [
        "v1",
        "v2",
    ]  # arg is Literal


def test_create_table_function_expression_named_args():
    function_expression = _create_table_function_expression(
        "func_name", arg_a=lit("v1"), arg_b=lit("v2")
    )
    assert isinstance(function_expression, NamedArgumentsTableFunction)
    assert function_expression.func_name == "func_name"
    assert [(key, arg.value) for key, arg in function_expression.args.items()] == [
        ("arg_a", "v1"),
        ("arg_b", "v2"),
    ]


def test_create_table_function_expression_named_wrong_params():
    with pytest.raises(ValueError) as ve:
        _create_table_function_expression("func_name", lit("v1"), argb=lit("v2"))
    assert (
        "A table function shouldn't have both args and named args." == ve.value.args[0]
    )


def test_create_table_function_expression_named_wrong_table_name():
    with pytest.raises(TypeError) as ve:
        _create_table_function_expression(1)
    assert (
        "'func' should be a function name in str, a list of strs that have all or a part of the fully qualified name, or a TableFunctionCall instance."
        == ve.value.args[0]
    )


def test_sha2_negative():
    num_bits = 2
    with pytest.raises(
        ValueError, match=f"num_bits {num_bits} is not in the permitted values"
    ):
        sha2(col("a"), num_bits)


def test_functions_alias():
    import snowflake.snowpark.functions as functions

    assert functions.substr == functions.substring
    assert functions.count_distinct == functions.countDistinct
    assert functions.collect_set == functions.array_unique_agg
    assert functions.to_char == functions.to_varchar
    assert functions.function == functions.builtin
    assert functions.call_function == functions.call_builtin
    assert functions.expr == functions.sql_expr
    assert functions.monotonically_increasing_id == functions.seq8
    assert functions.from_unixtime == functions.to_timestamp


def test_sql_expr_is_constant():
    from snowflake.snowpark._internal.analyzer.expression import (
        COLUMN_DEPENDENCY_ALL,
        COLUMN_DEPENDENCY_EMPTY,
        UnresolvedAttribute,
    )
    from snowflake.snowpark.functions import sql_expr

    col_expr = sql_expr("a + 1", _emit_ast=False)
    assert isinstance(col_expr._expression, UnresolvedAttribute)
    assert not col_expr._expression.is_constant
    assert col_expr._expression.dependent_column_names() == COLUMN_DEPENDENCY_ALL

    const_expr = sql_expr("{'k': 1}", _emit_ast=False, is_constant=True)
    assert isinstance(const_expr._expression, UnresolvedAttribute)
    assert const_expr._expression.is_constant
    assert const_expr._expression.dependent_column_names() == COLUMN_DEPENDENCY_EMPTY


def _ast_pos_and_named(col):
    """Return (pos_arg_kinds, named_arg_names) from a Column's ApplyExpr AST."""
    ae = col._ast.apply_expr
    pos = [a.WhichOneof("variant") for a in ae.pos_args]
    named = [e._1 for e in ae.named_args]
    return pos, named


def test_ai_functions_ast_optional_args_use_named_args():
    """Optional AI args skipped in the middle must be encoded as named_args.

    Encoding them as trailing positionals would bind to the wrong parameter on
    AST replay (e.g. return_error_details landing in file / categories / config).
    """
    from snowflake.snowpark.functions import (
        ai_classify,
        ai_count_tokens,
        ai_extract,
        ai_filter,
        ai_multi_embed,
        ai_parse_document,
        ai_redact,
        ai_sentiment,
        ai_transcribe,
        to_file,
    )

    pos, named = _ast_pos_and_named(
        ai_filter("is it true?", return_error_details=True)
    )
    assert pos == ["string_val"]
    assert named == ["return_error_details"]

    pos, named = _ast_pos_and_named(
        ai_classify(
            "x",
            ["a", "b"],
            return_error_details=True,
            task_description="desc",
        )
    )
    assert pos == ["string_val", "list_val"]
    assert named == ["return_error_details", "task_description"]

    f = to_file("@s/f.pdf")
    pos, named = _ast_pos_and_named(
        ai_parse_document(f, return_error_details=True, mode="LAYOUT")
    )
    assert len(pos) == 1
    assert named == ["mode", "return_error_details"]

    pos, named = _ast_pos_and_named(
        ai_transcribe(f, return_error_details=True, timestamp_granularity="word")
    )
    assert len(pos) == 1
    assert named == ["return_error_details", "timestamp_granularity"]

    pos, named = _ast_pos_and_named(
        ai_count_tokens("ai_complete", "hello", model="llama3.1-70b")
    )
    assert pos == ["string_val", "string_val"]
    assert named == ["model"]

    pos, named = _ast_pos_and_named(
        ai_extract("text", {"a": "q"}, config={"scale_factor": 2.0})
    )
    assert pos == ["string_val", "seq_map_val"]
    assert named == ["config"]

    pos, named = _ast_pos_and_named(ai_sentiment("text", return_error_details=True))
    assert pos == ["string_val"]
    assert named == ["return_error_details"]

    pos, named = _ast_pos_and_named(ai_redact("text", mode="detect"))
    assert pos == ["string_val"]
    assert named == ["mode"]

    pos, named = _ast_pos_and_named(
        ai_multi_embed("twelvelabs-marengo-embed-3-0", "hello", start_sec=1.0)
    )
    assert pos == ["string_val", "string_val"]
    assert named == ["start_sec"]


def _render_ai_extract_sql(response_format):
    """Render the full generated SQL fragment for an ``ai_extract`` call.

    Reproduces what ``Analyzer`` emits into the SELECT projection: each child
    expression's raw SQL is concatenated by ``function_expression``. This lets
    us assert at the unit level that ``response_format`` string content is
    escaped and kept inside the SQL string literal so the generated SQL is valid.
    """
    column = ai_extract("INPUT_TEXT", response_format, _emit_ast=False)
    expr = column._expression
    children = [
        child.name if hasattr(child, "name") else "<input>" for child in expr.children
    ]
    return function_expression(expr.name, children, False)


# ---------------------------------------------------------------------------
# Special-character escaping tests for the AI function object/array literals.
# ---------------------------------------------------------------------------


def test_python_obj_to_sql_literal_escapes_single_quotes():
    # An apostrophe in a question must be doubled so it stays inside the literal.
    out = _python_obj_to_sql_literal({"name": "What is the employee's last name?"})
    assert out == "{'name': 'What is the employee''s last name?'}"
    # Every single quote is paired: the literal stays balanced.
    assert out.count("'") % 2 == 0


def test_python_obj_to_sql_literal_escapes_backslash():
    # Snowflake treats backslash as an escape char inside string literals, so it
    # must be doubled to stay inside the literal.
    out = _python_obj_to_sql_literal({"a": "b\\c"})
    assert out == "{'a': 'b\\\\c'}"
    # A trailing backslash must not escape the closing quote.
    out2 = _python_obj_to_sql_literal(["ends_with_backslash\\"])
    assert out2 == "['ends_with_backslash\\\\']"


def test_python_obj_to_sql_literal_scalar_types():
    assert _python_obj_to_sql_literal(None) == "null"
    assert _python_obj_to_sql_literal(True) == "true"
    assert _python_obj_to_sql_literal(False) == "false"
    assert _python_obj_to_sql_literal(100) == "100"
    assert _python_obj_to_sql_literal(0.7) == "0.7"
    # bool must not be serialized as an int
    assert _python_obj_to_sql_literal({"flag": True}) == "{'flag': true}"


def test_python_obj_to_sql_literal_nested_structures():
    assert (
        _python_obj_to_sql_literal([["name", "q1"], ["addr", "q2"]])
        == "[['name', 'q1'], ['addr', 'q2']]"
    )
    assert (
        _python_obj_to_sql_literal({"temperature": 0.7, "max_tokens": 100})
        == "{'temperature': 0.7, 'max_tokens': 100}"
    )


def test_python_obj_to_sql_literal_rejects_unsupported_type():
    with pytest.raises(TypeError):
        _python_obj_to_sql_literal({"k": object()})


def test_python_obj_to_sql_literal_non_finite_floats_match_json():
    # Non-finite floats are serialized the same way json.dumps does -- NaN /
    # Infinity / -Infinity -- and left for Snowflake to reject server-side,
    # rather than raising client-side. NaN hits the `value != value` clause;
    # the infinities hit the two others.
    assert _python_obj_to_sql_literal(float("nan")) == "NaN"
    assert _python_obj_to_sql_literal(float("inf")) == "Infinity"
    assert _python_obj_to_sql_literal(float("-inf")) == "-Infinity"
    # ...including when nested in a dict/list.
    assert (
        _python_obj_to_sql_literal({"temperature": float("inf")})
        == "{'temperature': Infinity}"
    )
    assert _python_obj_to_sql_literal([1.0, float("nan")]) == "[1.0, NaN]"


def test_ai_extract_apostrophe_question_valid_sql():
    sql = _render_ai_extract_sql({"name": "What is the employee's last name?"})
    # The whole call is a single ai_extract(...) with exactly one closing paren.
    assert sql.startswith("ai_extract(")
    assert sql.endswith(")")
    assert sql == (
        "ai_extract(<input>, {'name': 'What is the employee''s last name?'})"
    )
    # The literal portion is balanced: every single quote is paired.
    assert sql.count("'") % 2 == 0


def test_ai_extract_dict_value_special_characters_escaped():
    # A question value with apostrophes, parentheses, commas, a trailing "--"
    # and a double quote -- all must be escaped and kept inside one literal.
    response_format = {"q": "what's the user's name? (v2) -- note \"x\""}
    sql = _render_ai_extract_sql(response_format)
    assert sql == (
        "ai_extract(<input>, {'q': 'what''s the user''s name? (v2) -- note \"x\"'})"
    )
    # Each apostrophe in the value is doubled inside the literal.
    assert "what''s the user''s name?" in sql
    # Exactly one ai_extract(...) call, balanced single quotes, one output value.
    assert sql.startswith("ai_extract(")
    assert sql.endswith(")")
    assert sql.count("'") % 2 == 0


def test_ai_extract_list_value_special_characters_escaped():
    # A list value containing single quotes, a backslash, parentheses, a comma
    # and a trailing "--" -- all escaped and kept inside one string literal.
    response_format = ["a'b\\c) , (d --"]
    sql = _render_ai_extract_sql(response_format)
    assert sql == "ai_extract(<input>, ['a''b\\\\c) , (d --'])"
    # The call begins with exactly one ai_extract( and ends with one paren.
    assert sql.startswith("ai_extract(")
    assert sql.endswith(")")
    # Single quotes are doubled and the backslash is doubled inside the literal.
    assert "a''b\\\\c" in sql
    assert sql.count("'") % 2 == 0
