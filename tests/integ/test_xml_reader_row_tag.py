#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import pytest

from snowflake.snowpark import Row
from snowflake.snowpark.exceptions import (
    SnowparkDataframeReaderException,
    SnowparkSQLException,
)
from snowflake.snowpark.functions import col, lit
from tests.utils import TestFiles, Utils

# XML test file constants
test_file_books_xml = "books.xml"
test_file_books2_xml = "books2.xml"
test_file_house_xml = "fias_house.xml"
test_file_house_large_xml = "fias_house.large.xml"
test_file_xxe_xml = "xxe.xml"
test_file_nested_xml = "nested.xml"
test_file_malformed_no_closing_tag_xml = "malformed_no_closing_tag.xml"
test_file_malformed_not_self_closing_xml = "malformed_not_self_closing.xml"
test_file_malformed_record_xml = "malformed_record.xml"
test_file_xml_declared_namespace = "declared_namespace.xml"
test_file_xml_undeclared_namespace = "undeclared_namespace.xml"

# Global stage name for uploading test files
tmp_stage_name = Utils.random_stage_name()


@pytest.fixture(scope="module", autouse=True)
def setup(session, resources_path, local_testing_mode):
    test_files = TestFiles(resources_path)
    if not local_testing_mode:
        Utils.create_stage(session, tmp_stage_name, is_temporary=True)

    # Upload all XML test files
    Utils.upload_to_stage(
        session, "@" + tmp_stage_name, test_files.test_books_xml, compress=False
    )
    Utils.upload_to_stage(
        session, "@" + tmp_stage_name, test_files.test_books2_xml, compress=False
    )
    Utils.upload_to_stage(
        session, "@" + tmp_stage_name, test_files.test_house_xml, compress=False
    )
    Utils.upload_to_stage(
        session, "@" + tmp_stage_name, test_files.test_house_large_xml, compress=False
    )
    Utils.upload_to_stage(
        session, "@" + tmp_stage_name, test_files.test_xxe_xml, compress=False
    )
    Utils.upload_to_stage(
        session, "@" + tmp_stage_name, test_files.test_nested_xml, compress=False
    )
    Utils.upload_to_stage(
        session,
        "@" + tmp_stage_name,
        test_files.test_malformed_no_closing_tag_xml,
        compress=False,
    )
    Utils.upload_to_stage(
        session,
        "@" + tmp_stage_name,
        test_files.test_malformed_not_self_closing_xml,
        compress=False,
    )
    Utils.upload_to_stage(
        session,
        "@" + tmp_stage_name,
        test_files.test_malformed_record_xml,
        compress=False,
    )
    Utils.upload_to_stage(
        session,
        "@" + tmp_stage_name,
        test_files.test_xml_declared_namespace,
        compress=False,
    )
    Utils.upload_to_stage(
        session,
        "@" + tmp_stage_name,
        test_files.test_xml_undeclared_namespace,
        compress=False,
    )

    yield
    # Clean up resources
    if not local_testing_mode:
        session.sql(f"DROP STAGE IF EXISTS {tmp_stage_name}").collect()


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="xml not supported in local testing mode",
)
@pytest.mark.parametrize(
    "file,row_tag,expected_row_count,expected_column_count",
    [
        [test_file_books_xml, "book", 12, 7],
        [test_file_books2_xml, "book", 2, 6],
        [test_file_house_xml, "House", 37, 22],
        [test_file_house_large_xml, "House", 740, 22],
    ],
)
def test_read_xml_row_tag(
    session, file, row_tag, expected_row_count, expected_column_count
):
    df = session.read.option("rowTag", row_tag).xml(f"@{tmp_stage_name}/{file}")
    result = df.collect()
    assert len(result) == expected_row_count
    assert len(result[0]) == expected_column_count


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="xml not supported in local testing mode",
)
def test_read_xml_no_xxe(session):
    row_tag = "bar"
    stage_file_path = f"@{tmp_stage_name}/{test_file_xxe_xml}"
    df = session.read.option("rowTag", row_tag).xml(stage_file_path)
    Utils.check_answer(df, [Row("null")])


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="xml not supported in local testing mode",
)
def test_read_xml_query_nested_data(session):
    row_tag = "tag"
    df = session.read.option("rowTag", row_tag).xml(
        f"@{tmp_stage_name}/{test_file_nested_xml}"
    )
    assert df._all_variant_cols is True
    Utils.check_answer(
        df.select(
            "'test'.num", "'test'.str", col("'test'.obj"), col("'test'.obj.bool")
        ),
        [Row('"1"', '"str1"', '{\n  "bool": "true",\n  "str": "str2"\n}', '"true"')],
    )


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="xml not supported in local testing mode",
)
def test_read_xml_non_existing_file(session):
    row_tag = "tag"
    with pytest.raises(ValueError, match="does not exist"):
        session.read.option("rowTag", row_tag).xml(
            f"@{tmp_stage_name}/non_existing_file.xml"
        )


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="xml not supported in local testing mode",
)
@pytest.mark.parametrize(
    "file",
    (
        test_file_malformed_no_closing_tag_xml,
        test_file_malformed_not_self_closing_xml,
        test_file_malformed_record_xml,
    ),
)
def test_read_malformed_xml(session, file):
    row_tag = "record"
    file_path = f"@{tmp_stage_name}/{file}"

    # permissive mode
    df = (
        session.read.option("rowTag", row_tag)
        .option("mode", "permissive")
        .xml(file_path)
    )
    result = df.collect()
    assert len(result) == 2
    assert len(result[0]) == 4  # has another column '_corrupt_record'
    assert (
        result[0]["'_corrupt_record'"] is not None
        or result[1]["'_corrupt_record'"] is not None
    )

    # dropmalformed mode
    df = (
        session.read.option("rowTag", row_tag)
        .option("mode", "dropmalformed")
        .xml(file_path)
    )
    result = df.collect()
    assert len(result) == 1
    assert len(result[0]) == 3

    # failfast mode
    df = (
        session.read.option("rowTag", row_tag).option("mode", "failfast").xml(file_path)
    )
    with pytest.raises(SnowparkSQLException, match="Malformed XML record at bytes"):
        df.collect()


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="xml not supported in local testing mode",
)
def test_read_xml_row_tag_not_found(session):
    row_tag = "non-existing-tag"
    df = session.read.option("rowTag", row_tag).xml(
        f"@{tmp_stage_name}/{test_file_books_xml}"
    )

    with pytest.raises(
        SnowparkDataframeReaderException, match="Cannot find the row tag"
    ):
        df.collect()

    # also works for nested query plan
    with pytest.raises(
        SnowparkDataframeReaderException, match="Cannot find the row tag"
    ):
        df.filter(lit(True)).collect()


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="xml not supported in local testing mode",
)
def test_read_xml_declared_namespace(session):
    row_tag = "px:root"
    expected_items = [
        {"_id": "1", "name": "Item One", "value": "100"},
        {"_id": "2", "name": "Item Two", "value": "200"},
    ]
    expected_data = json.dumps(expected_items, indent=2)

    df = (
        session.read.option("rowTag", row_tag)
        .option("stripNamespaces", True)
        .xml(f"@{tmp_stage_name}/{test_file_xml_declared_namespace}")
    )
    result = df.collect()
    assert len(result) == 1
    # Namespaces should be stripped
    assert result[0]["'item'"] == expected_data

    expected_items_with_ns = [
        {
            "_id": "1",
            "{http://example.com/px}name": "Item One",
            "{http://example.com/px}value": "100",
        },
        {
            "_id": "2",
            "{http://example.com/px}name": "Item Two",
            "{http://example.com/px}value": "200",
        },
    ]
    expected_data = json.dumps(expected_items_with_ns, indent=2)

    df = (
        session.read.option("rowTag", row_tag)
        .option("stripNamespaces", False)
        .xml(f"@{tmp_stage_name}/{test_file_xml_declared_namespace}")
    )
    result = df.collect()
    assert len(result) == 1
    # Namespaces should be replaced with URI
    assert result[0]["'{http://example.com/px}item'"] == expected_data


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="xml not supported in local testing mode",
)
@pytest.mark.parametrize("strip_namespaces", [True, False])
def test_read_xml_undeclared_namespace(session, strip_namespaces):
    # Read with undeclared namespace, stripNamespaces=true and false should have the same result
    # Prefixes without declarations should remain as they don't follow {namespace}tag format
    row_tag = "px:item"
    df = (
        session.read.option("rowTag", row_tag)
        .option("stripNamespaces", strip_namespaces)
        .xml(f"@{tmp_stage_name}/{test_file_xml_undeclared_namespace}")
    )
    result = df.collect()
    assert len(result) == 2
    assert result[0]["'px:name'"] in ['"Item One"', '"Item Two"']
    assert result[1]["'px:value'"] in ['"100"', '"200"']
