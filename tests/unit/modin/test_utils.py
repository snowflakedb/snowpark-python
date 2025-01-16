#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import re
from unittest import mock
from unittest.mock import Mock, patch

import numpy as np
import pandas as native_pd
import pytest
from modin.pandas import DataFrame, Series

from snowflake.snowpark._internal.analyzer.analyzer_utils import DOUBLE_QUOTE
from snowflake.snowpark._internal.type_utils import VALID_PYTHON_TYPES_FOR_LITERAL_VALUE
from snowflake.snowpark.dataframe import DataFrame as SnowparkDataFrame
from snowflake.snowpark.functions import col
from snowflake.snowpark.modin.plugin._internal.ordered_dataframe import (
    DataFrameReference,
    OrderedDataFrame,
    OrderingColumn,
)
from snowflake.snowpark.modin.plugin._internal.utils import (
    _MAX_IDENTIFIER_LENGTH,
    INDEX_LABEL,
    LEVEL_LABEL,
    append_columns,
    assert_duplicate_free,
    check_snowpark_pandas_object_in_arg,
    convert_numpy_pandas_scalar_to_snowpark_literal,
    convert_snowflake_string_constant_to_python_string,
    extract_all_duplicates,
    extract_non_pandas_label_from_object_construct_map,
    extract_pandas_label_from_object_construct_map,
    extract_pandas_label_from_object_construct_snowflake_quoted_identifier,
    extract_pandas_label_from_snowflake_quoted_identifier,
    fill_missing_levels_for_pandas_label,
    fill_none_in_index_labels,
    fillna_label_to_value_map,
    from_pandas_label,
    generate_new_labels,
    generate_snowflake_quoted_identifiers_helper,
    get_mapping_from_left_to_right_columns_by_label,
    is_all_label_components_none,
    is_json_serializable_pandas_labels,
    is_repr_truncated,
    is_valid_snowflake_quoted_identifier,
    label_prefix_match,
    parse_snowflake_object_construct_identifier_to_map,
    serialize_pandas_labels,
    to_pandas_label,
    try_convert_to_simple_slice,
    unquote_name_if_quoted,
)
from snowflake.snowpark.modin.plugin.extensions.indexing_overrides import (
    is_boolean_array,
)
from snowflake.snowpark.types import (
    ColumnIdentifier,
    IntegerType,
    StructField,
    StructType,
)


def check_identifier_equality_or_match_pattern(
    identifier: str, expected_pattern: str, is_equal: bool
) -> None:
    if is_equal:
        assert identifier == expected_pattern
    else:
        assert re.match(expected_pattern, identifier)


@pytest.mark.parametrize("identifier", ['"A"', '"""A"""', '"A""B"', '""""'])
def test_is_valid_snowflake_quoted_identifier_true(identifier: str) -> None:
    assert is_valid_snowflake_quoted_identifier(identifier)


@pytest.mark.parametrize(
    "identifier", ["", '"', '""', '"A', '"A"B"', '"""', '""A""', "abc"]
)
def test_is_valid_snowflake_quoted_identifier_false(identifier):
    assert not is_valid_snowflake_quoted_identifier(identifier)


@pytest.mark.parametrize(
    "labels, expected_identifier_patterns",
    [
        (
            ["A", '"A"', 12, 42, None, (2, 3)],
            ['"A"', '"""A"""', '"12"', '"42"', '"None"', r'"\(2, 3\)"'],
        ),
        (["A", "A"], ['"A"', r'"A_[0-9a-z]+"']),
        (["(2, 3)", (2, 3)], [r'"\(2, 3\)"', r'"\(2, 3\)_[0-9a-z]+"']),
        (
            ["(2, 3)", (2, 3), (2, 3)],
            [r'"\(2, 3\)"', r'"\(2, 3\)_[0-9a-z]+"', r'"\(2, 3\)_[0-9a-z]+"'],
        ),
    ],
)
def test_generate_snowflake_quoted_identifiers_excluded_empty(
    labels,
    expected_identifier_patterns,
):
    identifiers = generate_snowflake_quoted_identifiers_helper(
        pandas_labels=labels, excluded=[]
    )
    for identifier, pattern in zip(identifiers, expected_identifier_patterns):
        assert re.match(pattern, identifier)


@pytest.mark.parametrize(
    "labels_to_expected_identifiers",
    [
        (
            ["A", '"A"', 12, 42, None, (2, 3)],
            ['"A"', '"""A"""', '"12"', '"42"', '"None"', '"(2, 3)"'],
        ),
        (["A", "A"], ['"A"', '"A"']),
        (["(2, 3)", (2, 3)], ['"(2, 3)"', '"(2, 3)"']),
        (["(2, 3)", (2, 3), (2, 3)], ['"(2, 3)"', '"(2, 3)"', '"(2, 3)"']),
    ],
)
def test_generate_snowflake_quoted_identifiers_excluded_none(
    labels_to_expected_identifiers,
):
    (labels, expected_identifiers) = labels_to_expected_identifiers
    assert (
        generate_snowflake_quoted_identifiers_helper(pandas_labels=labels)
        == expected_identifiers
    )


@pytest.mark.parametrize(
    "excluded_identifiers, has_suffix",
    [
        (['"A"'], True),
        (['"A"', '"A_0"'], True),
        (['"B"', '"C"'], False),
        (['"A"', '"B"', '"C"'], True),
        (['"__A__"', '"B"', '"C"'], False),
    ],
)
def test_generate_snowflake_quoted_identifier_excluded_not_empty(
    excluded_identifiers,
    has_suffix,
):
    identifier = generate_snowflake_quoted_identifiers_helper(
        pandas_labels=["A"], excluded=excluded_identifiers
    )[0]
    if has_suffix:
        assert re.match(r'"A_[0-9a-z]+"', identifier)
    else:
        assert identifier == '"A"'


@pytest.mark.parametrize(
    "excluded_identifiers, has_suffix",
    [
        (['"A"', '"_A_"'], False),
        (['"__A__"', '"A"'], True),
        (['"__a__"', '"A"'], False),
    ],
)
def test_generate_snowflake_quoted_identifier_wrap_underscore(
    excluded_identifiers, has_suffix
):
    identifier = generate_snowflake_quoted_identifiers_helper(
        pandas_labels=["A"], excluded=excluded_identifiers, wrap_double_underscore=True
    )[0]
    if has_suffix:
        assert re.match(r'"__A_[0-9a-z]+__"', identifier)
    else:
        assert identifier == '"__A__"'


def test_generate_snowflake_quoted_identifier_invalid_excluded_negative():
    # verify unquoted identifiers result in error.
    with pytest.raises(
        ValueError,
        match="'excluded' must have quoted identifiers. Found unquoted identifier='A'",
    ):
        generate_snowflake_quoted_identifiers_helper(
            excluded=["A"], pandas_labels=["A"]
        )


def test_generated_names_have_max_length():
    label = "A"
    all_identifiers = ['"A"']
    all_labels = [label]
    for _i in range(0, 100):
        label = generate_new_labels(pandas_labels=[label], excluded=all_labels)[0]
        all_labels.append(label)
        identifier = generate_snowflake_quoted_identifiers_helper(
            pandas_labels=[label], excluded=all_identifiers
        )[0]
        all_identifiers.append(identifier)
    assert [all(len(id) <= _MAX_IDENTIFIER_LENGTH for id in all_identifiers)]
    assert [all(len(id) <= _MAX_IDENTIFIER_LENGTH for id in all_labels)]


@patch(
    "snowflake.snowpark.modin.plugin._internal.utils.generate_column_identifier_random"
)
def test_generate_snowflake_quoted_identifier_random_suffix_conflict(
    mock_generate_column_identifier_random,
):
    mock_generate_column_identifier_random.return_value = "suffix"
    with pytest.raises(
        ValueError,
        match="Failed to generate quoted identifier for pandas label",
    ):
        generate_snowflake_quoted_identifiers_helper(
            pandas_labels=["A"], excluded=['"A"', '"A_suffix"']
        )


@pytest.mark.parametrize(
    "test_data, serialized_result",
    [
        ([None], ["None"]),
        ([(None,)], ["[null]"]),
        (["abc"], ["abc"]),
        ([("abc",)], ['["abc"]']),
        (["abc", "def", "ghi"], ["abc", "def", "ghi"]),
        ([("abc", "def", "ghi")], ['["abc", "def", "ghi"]']),
        ([("abc",), ("def",), ("ghi",)], ['["abc"]', '["def"]', '["ghi"]']),
        ([("abc", "def"), ("ghi", "jkl")], ['["abc", "def"]', '["ghi", "jkl"]']),
        ([1, True, 3.14519], ["1", "True", "3.14519"]),
        ([(1, True, 3.14519)], ["[1, true, 3.14519]"]),
        (
            [
                (
                    "abc",
                    1,
                ),
                (True, 3.14519),
                (None, "foo"),
            ],
            ['["abc", 1]', "[true, 3.14519]", '[null, "foo"]'],
        ),
    ],
)
def test_serialize_pandas_labels(test_data, serialized_result):
    assert serialize_pandas_labels(test_data) == serialized_result


@pytest.mark.parametrize(
    "test_pandas_label, is_json_serializable",
    [
        (None, True),
        (1.234, True),
        ((1.234,), True),
        (False, True),
        ((False,), True),
        ((None,), True),
        ("abc", True),
        (("abc",), True),
        (native_pd.Timedelta(1, "d"), False),
        (
            native_pd.Timestamp(
                year=2023, month=9, day=28, hour=13, minute=20, second=1
            ),
            False,
        ),
        (
            (
                "abc",
                "def",
                "ghi",
            ),
            True,
        ),
        (
            (
                "abc",
                1.23,
                "ghi",
            ),
            True,
        ),
        (
            (
                "abc",
                None,
                "ghi",
            ),
            True,
        ),
        (
            (
                "abc",
                "def",
                None,
            ),
            True,
        ),
        ((1, True, 3.14519), True),
        (("1", "True", "3.14519"), True),
        ((np.float32(3.14159),), False),
        (("yes", np.ndarray([1, 2, 3])), False),
        (("yes", native_pd.Timedelta(1, "d")), False),
        (
            (
                "no",
                native_pd.Timestamp(
                    year=2023, month=9, day=28, hour=13, minute=20, second=1
                ),
            ),
            False,
        ),
    ],
)
def test_is_json_serializable_pandas_labels(test_pandas_label, is_json_serializable):
    assert (
        is_json_serializable_pandas_labels([test_pandas_label]) == is_json_serializable
    )


def test_assert_duplicate_free():
    test_arrays_and_result = [
        (["1", "2", "1", "3", "2"], False),
        (["1", "2", "3"], True),
        ([], True),
    ]

    for arr, ref in test_arrays_and_result:
        # for false, make sure function produces proper assert
        if not ref:
            # assertion error produces detailed response, match here with needle only.
            with pytest.raises(AssertionError, match="Found duplicates of type test"):
                assert_duplicate_free(arr, "test")
        else:
            assert_duplicate_free(arr, "test")


def test_to_pandas_label() -> None:
    # verify tuple with length 1
    assert to_pandas_label(("A",)) == "A"
    assert to_pandas_label((None,)) is None
    assert to_pandas_label(("",)) == ""
    assert to_pandas_label((("A",),)) == ("A",)
    assert to_pandas_label((("A", "B"),)) == ("A", "B")

    # verify tuple with length > 1
    assert to_pandas_label(("A", "B", "C")) == ("A", "B", "C")
    assert to_pandas_label(("A", "", None)) == ("A", "", None)
    assert to_pandas_label((None, None, None)) == (None, None, None)
    assert to_pandas_label((("a", "b"), ("c", ""), (None, None))) == (
        ("a", "b"),
        ("c", ""),
        (None, None),
    )


def test_from_pandas_label() -> None:
    assert from_pandas_label("A", num_levels=1) == ("A",)
    assert from_pandas_label(("A", "B"), num_levels=1) == (("A", "B"),)
    assert from_pandas_label(("A", "B"), num_levels=2) == (
        "A",
        "B",
    )


@pytest.mark.parametrize(
    "sf_quoted_identifier_to_expected_pandas_label",
    [('"abc"', "abc"), ('"ab""c"', 'ab"c'), ('"""ab""c"""', '"ab"c"')],
)
def test_extract_pandas_label_from_snowflake_quoted_identifier(
    sf_quoted_identifier_to_expected_pandas_label,
):
    (
        snowflake_quoted_identifier,
        expected_pandas_label,
    ) = sf_quoted_identifier_to_expected_pandas_label
    assert (
        extract_pandas_label_from_snowflake_quoted_identifier(
            snowflake_quoted_identifier
        )
        == expected_pandas_label
    )


@pytest.mark.parametrize(
    "sf_quoted_identifier_to_expected_pandas_label",
    [('"UPPER"', "UPPER"), ('"lower"', "lower"), ('"MixedCase"', "MixedCase")],
)
def test_extract_pandas_label_from_snowflake_quoted_identifier_case_is_preserved(
    sf_quoted_identifier_to_expected_pandas_label,
):
    (
        snowflake_quoted_identifier,
        expected_pandas_label,
    ) = sf_quoted_identifier_to_expected_pandas_label
    assert (
        extract_pandas_label_from_snowflake_quoted_identifier(
            snowflake_quoted_identifier
        )
        == expected_pandas_label
    )


@pytest.mark.parametrize("snowflake_identifier", ["abc", "'abc'", 'ab"cd', 'ab""cd'])
def test_extract_pandas_label_from_snowflake_quoted_identifier_negative(
    snowflake_identifier,
):
    # input identifier must be quoted.
    with pytest.raises(AssertionError):
        extract_pandas_label_from_snowflake_quoted_identifier(snowflake_identifier)


@pytest.mark.parametrize(
    "sf_quoted_identifier_to_expected_multi_pandas_label",
    [
        ('{"0":"abc","1":"def"}', ("abc", "def")),
        ('{"0":"ab\\"c","1":"def"}', ('ab"c', "def")),
        ('{"0":"abc\\"\\"","1":"def"}', ('abc""', "def")),
        ('{"0":"abc","1":"\\"def"}', ("abc", '"def')),
        ('{"0":"abc\\"\\"","1":"\\"\\"def"}', ('abc""', '""def')),
        ('{"0":"ab\\"","1":"\\"c\\"","2":"def"}', ('ab"', '"c"', "def")),
        ('{"0":"a,b,c","1":"d,e,f"}', ("a,b,c", "d,e,f")),
        ('{"0":"abc","1":"def\\"","2":"ghi"}', ("abc", 'def"', "ghi")),
        ('{"0":"abc,,","1":",,,def"}', ("abc,,", ",,,def")),
        ('{"0":"ab,c,","1":",de,f"}', ("ab,c,", ",de,f")),
        ('{"0":"abc,,","1":",\\",\\"def"}', ("abc,,", ',","def')),
        ('{"0":",abc","1":"def"}', (",abc", "def")),
        ('{"0":",\\",\\"abc","1":"def"}', (',","abc', "def")),
        ('{"0":"\\",\\"abc","1":"def"}', ('","abc', "def")),
        ('{"0":"abc","2":"ghi"}', ("abc", None, "ghi")),
        ('{"1":"def"}', (None, "def", None)),
        ("{}", (None, None, None)),
        (
            "{}",
            None,
        ),
        (
            '{"1":"null","2":"inside null string"}',
            (None, "null", "inside null string"),
        ),
    ],
)
def test_extract_pandas_label_tuple_from_object_construct_snowflake_quoted_identifier(
    sf_quoted_identifier_to_expected_multi_pandas_label,
):
    (
        identifier,
        expected_pandas_label,
    ) = sf_quoted_identifier_to_expected_multi_pandas_label
    snowflake_quoted_identifier = (
        '"' + repr(identifier.replace(DOUBLE_QUOTE, DOUBLE_QUOTE + DOUBLE_QUOTE)) + '"'
    )
    assert (
        extract_pandas_label_from_object_construct_snowflake_quoted_identifier(
            snowflake_quoted_identifier,
            len(expected_pandas_label)
            if isinstance(expected_pandas_label, tuple)
            else 1,
        )
        == expected_pandas_label
    )


@pytest.mark.parametrize(
    "obj_construct_quoted_identifier, expected_object_map",
    [
        ('"{""0"":""a""}"', {"0": "a"}),
        ('"{""0"":""a"",""1"":""b""}"', {"0": "a", "1": "b"}),
        ('"{""0"":""a"",""1"":""b"",""2"":""c""}"', {"0": "a", "1": "b", "2": "c"}),
        (
            '"{""0"":""a"",""1"":""b"", ""foo"":""bar""}"',
            {"0": "a", "1": "b", "foo": "bar"},
        ),
        ('"{""1"":""b"", ""foo"":""bar""}"', {"1": "b", "foo": "bar"}),
        ('"{}"', {}),
    ],
)
def test_parse_snowflake_object_construct_identifier_to_map(
    obj_construct_quoted_identifier, expected_object_map
):
    assert (
        parse_snowflake_object_construct_identifier_to_map(
            obj_construct_quoted_identifier
        )
        == expected_object_map
    )


@pytest.mark.parametrize(
    "obj_construct_map, levels, expected_pandas_label",
    [
        (
            {"0": "a"},
            1,
            "a",
        ),
        ({"0": "a", "1": "b"}, 2, ("a", "b")),
        ({"0": "a", "1": "b", "2": "c"}, 2, ("a", "b")),
        ({"0": "a", "1": "b", "2": "c"}, 3, ("a", "b", "c")),
        (
            {"0": "a", "1": "b", "foo": "bar"},
            2,
            ("a", "b"),
        ),
        ({"1": "b", "foo": "bar"}, 2, (None, "b")),
        ({"0": "a"}, 2, ("a", None)),
        ({}, 2, (None, None)),
    ],
)
def test_extract_pandas_label_tuple_from_object_construct_map(
    obj_construct_map, levels, expected_pandas_label
):
    assert (
        extract_pandas_label_from_object_construct_map(obj_construct_map, levels)
        == expected_pandas_label
    )


@pytest.mark.parametrize(
    "obj_construct_map, levels, expected_extra_kw",
    [
        ({"0": "a", "1": "b"}, 2, {}),
        (
            {"0": "a", "1": "b", "foo": "bar"},
            2,
            {"foo": "bar"},
        ),
        (
            {"1": "b", "foo": "bar"},
            2,
            {"foo": "bar"},
        ),
        ({"0": "a", "a": "0"}, 2, {"a": "0"}),
        ({"2": "val"}, 2, {"2": "val"}),
        ({}, 2, {}),
    ],
)
def test_extract_non_pandas_label_from_object_construct_map(
    obj_construct_map,
    levels,
    expected_extra_kw,
):
    assert (
        extract_non_pandas_label_from_object_construct_map(obj_construct_map, levels)
        == expected_extra_kw
    )


@pytest.mark.parametrize(
    "input_index_labels, existing_labels, expected_output_index_labels",
    [
        (["a"], None, ["a"]),
        ([None], None, [INDEX_LABEL]),
        ([None], [INDEX_LABEL], [f"{LEVEL_LABEL}_0"]),
        (["a", "b"], None, ["a", "b"]),
        (["a", None], None, ["a", f"{LEVEL_LABEL}_1"]),
        ([None, "b"], None, [f"{LEVEL_LABEL}_0", "b"]),
        ([None, None], None, [f"{LEVEL_LABEL}_0", f"{LEVEL_LABEL}_1"]),
        (
            [None, None],
            [INDEX_LABEL, f"{LEVEL_LABEL}_0", f"{LEVEL_LABEL}_1"],
            [f"{LEVEL_LABEL}_0", f"{LEVEL_LABEL}_1"],
        ),
        ([], None, []),
    ],
)
def test_fill_none_in_index_labels(
    input_index_labels, existing_labels, expected_output_index_labels
):
    assert (
        fill_none_in_index_labels(input_index_labels, existing_labels)
        == expected_output_index_labels
    )


@pytest.mark.parametrize(
    "name, unquoted_name",
    [
        ('""', ""),
        ("test_table", "test_table"),
        ('"test_table"', "test_table"),
        ('"test"_table"', 'test"_table'),
        ('"test""_table"', 'test"_table'),
    ],
)
def test_unquote_name_if_quoted(name, unquoted_name):
    assert unquote_name_if_quoted(name) == unquoted_name


@pytest.mark.parametrize(
    "identifier, unquoted_identifier",
    [
        ("foo", "foo"),
        ("'foo'", "foo"),
        ("'f''oo'", "f'oo"),
    ],
)
def test_convert_snowflake_string_constant_to_python_string(
    identifier, unquoted_identifier
):
    assert (
        convert_snowflake_string_constant_to_python_string(identifier)
        == unquoted_identifier
    )


def test_check_snowpark_pandas_object_in_arg():
    mock_dataframe = Mock(spec=DataFrame)
    mock_series = Mock(spec=Series)
    assert check_snowpark_pandas_object_in_arg(mock_dataframe)
    assert check_snowpark_pandas_object_in_arg(mock_series)
    assert check_snowpark_pandas_object_in_arg([1, mock_dataframe])
    assert check_snowpark_pandas_object_in_arg([1, {2: mock_dataframe}])
    assert check_snowpark_pandas_object_in_arg({1: mock_series})
    assert check_snowpark_pandas_object_in_arg({1: "pandas", 2: [3, mock_series]})


@pytest.mark.parametrize(
    "label, prefix_map, expected",
    [
        ["a", {"a": 3}, 3],
        [("a", "b"), {("a", "b"): 3}, 3],
        [("a", "b"), {"a": 3}, 3],
        [("a", "b", "c"), {("a", "b"): 3}, 3],
        ["a", {"b": 3}, None],
        [("a", "b"), {("a", "c"): 3}, None],
        [("a", "b"), {("a", None): 3}, None],
        [("a", "b"), {("a",): 3}, 3],
        [("a", "b"), {("a",): 3, ("a", "b"): 4}, 3],  # always respect the first match
    ],
)
def test_label_prefix_match(label, prefix_map, expected):
    assert label_prefix_match(label, prefix_map) == expected


@pytest.mark.parametrize(
    "label, prefix_map, level, expected",
    [
        [("a", "b"), {"a": 1}, 0, 1],
        [("a", "b"), {"a": 1}, 1, None],
        [("a", "b"), {"a": 1}, 10, None],
        [("a", "b"), {"b": 1}, 0, None],
        [("a", "b"), {"b": 1}, 1, 1],
        [("a", "b"), {"b": 1}, 10, None],
        [("a", "b"), {("a", "b"): 1}, 0, None],
        [("a", "b"), {("b",): 1}, 1, None],
    ],
)
def test_label_prefix_match_on_level(label, prefix_map, level, expected):
    assert label_prefix_match(label, prefix_map, level) == expected


@pytest.mark.parametrize(
    "value, columns, expected",
    [
        [{"a": 1, "b": 2, "d": 3}, native_pd.Index(["a", "b", "c"]), {"a": 1, "b": 2}],
        [
            {"a": 1},
            native_pd.Index(
                [
                    ("a", "aa", "aaa"),
                    ("b", "aa", "aaa"),
                    ("a", "bb", "bbb"),
                    ("b", "aa", "bbb"),
                ]
            ),
            {("a", "aa", "aaa"): 1, ("a", "bb", "bbb"): 1},
        ],
        [
            {("a", "aa"): 1},
            native_pd.Index(
                [
                    ("a", "aa", "aaa"),
                    ("b", "aa", "aaa"),
                    ("a", "bb", "bbb"),
                    ("b", "aa", "bbb"),
                ]
            ),
            {("a", "aa", "aaa"): 1},
        ],
        [
            {"aa": 1},
            native_pd.Index(
                [
                    ("a", "aa", "aaa"),
                    ("b", "aa", "aaa"),
                    ("a", "bb", "bbb"),
                    ("b", "aa", "bbb"),
                ]
            ),
            {},
        ],
    ],
)
def test_fillna_label_to_value_map(value, columns, expected):
    assert fillna_label_to_value_map(value, columns) == expected


@pytest.mark.parametrize(
    "value",
    [
        np.int8(1),
        np.int16(1),
        np.int32(1),
        np.int64(1),
        np.uint(1),
        np.longlong(1),
        np.float32(2.5),
        np.float64(2.5),
        np.double(2.5),
        np.bool_(True),
        np.datetime64("2005-02-25"),
        np.nan,
        native_pd.NaT,
        native_pd.NA,
    ],
)
def test_convert_numpy_pandas_scalar_to_snowpark_literal(value):
    def check(original_value, converted_value):
        assert isinstance(converted_value, VALID_PYTHON_TYPES_FOR_LITERAL_VALUE) and (
            converted_value is None
            if native_pd.isna(original_value)
            else original_value == converted_value
        )

    check(value, convert_numpy_pandas_scalar_to_snowpark_literal(value))


@pytest.mark.parametrize(
    "value",
    [
        native_pd.Timestamp(2017, 1, 1, 12),
        native_pd.Timestamp("2017-01-01T12"),
    ],
)
def test_convert_numpy_pandas_scalar_to_snowpark_literal_invalid_for_timestamp(value):
    with pytest.raises(
        ValueError, match="cannot represent Timestamp as a Snowpark literal"
    ):
        convert_numpy_pandas_scalar_to_snowpark_literal(value)


@pytest.mark.parametrize(
    "value",
    [[1, 1], (1, 1), np.array([1, 1]), native_pd.Series([1, 1])],
)
def test_convert_numpy_pandas_scalar_to_snowpark_literal_negative(value):
    with pytest.raises(AssertionError, match="is not a scalar"):
        convert_numpy_pandas_scalar_to_snowpark_literal(value)


@pytest.mark.parametrize(
    "pandas_label, expected_result",
    [
        (None, True),
        ("A", False),
        (("A",), False),
        (("A", "B"), False),
        ((None,), True),
        ((None, None), True),
        ((None, "A"), False),
    ],
)
def test_is_all_label_components_none(pandas_label, expected_result) -> None:
    result = is_all_label_components_none(pandas_label)
    assert result == expected_result


@pytest.mark.parametrize(
    "elements, expected_result",
    [
        (["a", "b", "c", "d"], []),
        (["a", "b", "a", "b", "e"], ["a", "b"]),
        ([("a", "e"), "b", ("a", "e"), "b", ("a", "e"), ("c", "d")], [("a", "e"), "b"]),
        ([("a", "e"), "b", ("a", "e"), None, ("a", "e"), None], [("a", "e"), None]),
        ([(None, "e"), "b", ("a", None), "c", ("a", None), None], [("a", None)]),
    ],
)
def test_extract_all_duplicates(elements, expected_result):
    assert extract_all_duplicates(elements) == expected_result


@pytest.mark.parametrize("arr", [[False], np.array([False, True, False])])
def test_is_boolean_array(arr):
    assert is_boolean_array(arr)


@pytest.mark.parametrize("arr", [[], ["abc"], np.array([1, 2, 3]), np.array([0, 1, 1])])
def test_is_boolean_array_negative(arr):
    assert not is_boolean_array(arr)


@pytest.mark.parametrize(
    "num_level, pandas_label, level_to_start, fill_value, expected",
    [
        (3, "a", 1, "c", ("c", "a", "c")),
        (1, ("a", "b"), 0, "c", ("a", "b")),
        (3, ("a", "b"), 1, None, (None, "a", "b")),
        (5, ("a", "b"), 1, None, (None, "a", "b", None, None)),
        (3, ("a", "b"), 1, ("d", "e"), (("d", "e"), "a", "b")),
        (3, "a", 0, ("d", "e"), ("a", ("d", "e"), ("d", "e"))),
    ],
)
def test_construct_pandas_label_with_filling(
    num_level, pandas_label, level_to_start, fill_value, expected
):
    result = fill_missing_levels_for_pandas_label(
        pandas_label, num_level, level_to_start, fill_value
    )
    assert result == expected


@pytest.mark.parametrize(
    "args,expected",
    [
        ((0, 1, 0, 0), False),
        ((0, 1, 2, 0), False),
        ((1, 0, 0, 3), False),
        ((100, 5, 5, 0), True),
        ((2, 5, 5, 0), False),
        ((10, 30, 0, 20), True),
        ((10, 3, 0, 20), False),
        ((10, 30, 10, 20), True),
        ((10, 10, 10, 20), False),
        ((1, 3, 10, 20), False),
    ],
)
def test_is_repr_truncated(args, expected):
    assert is_repr_truncated(*args) == expected


# TODO: SNOW-916739 refactor to optimize all cases to skip joins
@pytest.mark.parametrize(
    "input, output",
    [
        [slice(0, 1, 1), slice(0, 1, 1)],
        [slice(0, 1, None), slice(0, 1, 1)],
        [slice(None, 1, None), slice(0, 1, 1)],
        [slice(1, None, None), None],
        [slice(0, -1, None), None],
        [slice(0, 1, 2), None],
        [slice(-1, 1, 1), None],
        [slice(1, -1, 1), None],
        [slice(0, None, 1), None],
        [slice(0, None, None), None],
        [3, None],
    ],
)
def test_try_convert_to_simple_slice(input, output):
    assert output == try_convert_to_simple_slice(input)


def test_append_columns():
    fake_snowpark_dataframe = mock.create_autospec(SnowparkDataFrame)
    snowpark_df_schema = StructType(
        [
            StructField(
                column_identifier=ColumnIdentifier('"a"'), datatype=IntegerType
            ),
            StructField(
                column_identifier=ColumnIdentifier('"B"'), datatype=IntegerType
            ),
            StructField(
                column_identifier=ColumnIdentifier('"C"'), datatype=IntegerType
            ),
            StructField(
                column_identifier=ColumnIdentifier('"d"'), datatype=IntegerType
            ),
        ]
    )
    fake_snowpark_dataframe.schema = snowpark_df_schema
    ordered_dataframe = OrderedDataFrame(
        DataFrameReference(fake_snowpark_dataframe),
        ordering_columns=[OrderingColumn('"a"'), OrderingColumn('"B"')],
        projected_column_snowflake_quoted_identifiers=['"a"', '"B"', '"C"', '"d"'],
    )
    new_ordered_dataframe = append_columns(ordered_dataframe, '"e"', col("E"))
    assert new_ordered_dataframe.projected_column_snowflake_quoted_identifiers == [
        '"a"',
        '"B"',
        '"C"',
        '"d"',
        '"e"',
    ]

    new_ordered_dataframe = append_columns(
        ordered_dataframe, ['"e"', '"f"'], [col("E"), col("F")]
    )
    assert new_ordered_dataframe.projected_column_snowflake_quoted_identifiers == [
        '"a"',
        '"B"',
        '"C"',
        '"d"',
        '"e"',
        '"f"',
    ]

    with pytest.raises(
        AssertionError, match="is not equal to the number of column objects"
    ):
        append_columns(ordered_dataframe, ['"e"', '"f"'], [col("E")])


@pytest.mark.parametrize(
    "right_pandas_labels, right_snowflake_quoted_identifiers, left_pandas_labels, left_snowflake_quoted_identifiers, expected_mapping",
    [
        # no duplication, 1-to-1 mapping
        (
            ["a", "b", "c"],
            ['"A"', '"B"', '"C"'],
            ["a", "b", "c"],
            ['"A"', '"B"', '"C"'],
            {'"A"': '"A"', '"B"': '"B"', '"C"': '"C"'},
        ),
        (
            ["a", "b", "c"],
            ['"A"', '"E"', '"C"'],
            ["a", "b", "c"],
            ['"A"', '"B"', '"F"'],
            {'"A"': '"A"', '"E"': '"B"', '"C"': '"F"'},
        ),
        # duplication in right
        (
            ["a", "a", "c"],
            ['"A"', '"A_1"', '"C"'],
            ["a", "b", "c"],
            ['"A"', '"B"', '"C"'],
            {'"A"': '"A"', '"A_1"': '"A"', '"C"': '"C"'},
        ),
        # duplication and no mapping for right
        (
            ["a", "a", "c", "d"],
            ['"A"', '"A_1"', '"C"', '"D"'],
            ["a", "b", "c"],
            ['"A"', '"B"', '"C"'],
            {'"A"': '"A"', '"A_1"': '"A"', '"C"': '"C"', '"D"': None},
        ),
        # duplication in left
        (
            ["a", "b", "c"],
            ['"A"', '"B"', '"C"'],
            ["a", "c", "c"],
            ['"A"', '"C"', '"C_1"'],
            {'"A"': '"A"', '"B"': None, '"C"': '"C_1"'},
        ),
        # duplication in both right and left
        (
            ["a", "a", "b", "c"],
            ['"A"', '"A_1"', '"B"', '"C"'],
            ["a", "c", "c", "d"],
            ['"A"', '"C"', '"C_1"', '"D"'],
            {'"A"': '"A"', '"A_1"': '"A"', '"B"': None, '"C"': '"C_1"'},
        ),
    ],
)
def test_get_mapping_from_left_to_right_columns_by_label(
    right_pandas_labels,
    right_snowflake_quoted_identifiers,
    left_pandas_labels,
    left_snowflake_quoted_identifiers,
    expected_mapping,
):
    assert get_mapping_from_left_to_right_columns_by_label(
        right_pandas_labels,
        right_snowflake_quoted_identifiers,
        left_pandas_labels,
        left_snowflake_quoted_identifiers,
    )


@pytest.mark.parametrize(
    "pandas_labels,excluded",
    [(["a", "b", "a"], None), (["a", "b", "a"], ["a", "b"]), (["a", "b", "a"], ["a"])],
)
def test_generate_new_labels(pandas_labels, excluded):
    # ensure there's no overlap
    assert (
        len(
            set(generate_new_labels(pandas_labels=pandas_labels, excluded=excluded))
            & set(excluded or [])
        )
        == 0
    )
