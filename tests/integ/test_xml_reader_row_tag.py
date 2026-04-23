#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
import datetime
import logging
import json
import os
from unittest import mock

import pytest

from snowflake.snowpark import Row
from snowflake.snowpark.exceptions import (
    SnowparkDataframeReaderException,
    SnowparkSQLException,
)
from snowflake.snowpark.functions import col, lit
from snowflake.snowpark.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    DoubleType,
    DateType,
    BooleanType,
    TimestampType,
    ArrayType,
)
import snowflake.snowpark.context as context
from tests.utils import TestFiles, Utils


pytestmark = [
    pytest.mark.skipif(
        "config.getoption('local_testing_mode', default=False)",
        reason="xml not supported in local testing mode",
    ),
    pytest.mark.udf,
]


@pytest.fixture()
def enable_scos_compatible_mode():
    """Enable SCOS compatible mode so that type validation runs inside the UDTF."""
    with mock.patch.object(context, "_is_snowpark_connect_compatible_mode", True):
        yield


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
test_file_null_value_xml = "null_value.xml"
test_file_books_xsd = "books.xsd"
test_file_dk_trace_xml = "dk_trace_sample.xml"
test_file_dblp_xml = "dblp_6kb.xml"
test_file_books_attr_val_xml = "books_attribute_value.xml"

# Global stage name for uploading test files
tmp_stage_name = Utils.random_stage_name()

# Inline XML strings for permissive/failfast/dropmalformed mode tests
SAMPLING_MISMATCH_XML = """\
<?xml version="1.0"?>
<data>
  <ROW><name>Alice</name><value>100</value></ROW>
  <ROW><name>Bob</name><value>200</value></ROW>
  <ROW><name>Carol</name><value>300</value></ROW>
  <ROW><name>Dave</name><value>400</value></ROW>
  <ROW><name>Eve</name><value>500</value></ROW>
  <ROW><name>Frank</name><value>hello</value></ROW>
</data>
"""

MULTIFIELD_MISMATCH_XML = """\
<?xml version="1.0"?>
<data>
  <ROW><int_col>42</int_col><bool_col>true</bool_col><dbl_col>3.14</dbl_col></ROW>
  <ROW><int_col>not_a_num</int_col><bool_col>maybe</bool_col><dbl_col>2.72</dbl_col></ROW>
  <ROW><int_col>99</int_col><bool_col>false</bool_col><dbl_col>not_a_dbl</dbl_col></ROW>
</data>
"""


def _upload_xml_string(session, stage, filename, xml_content):
    """Write XML string to a temp file and upload to stage."""
    import tempfile

    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".xml", delete=False, prefix=filename.replace(".xml", "_")
    ) as f:
        f.write(xml_content)
        tmp_path = f.name
    try:
        Utils.upload_to_stage(session, stage, tmp_path, compress=False)
    finally:
        os.unlink(tmp_path)
    return os.path.basename(tmp_path)


_staged_files = {}


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
    Utils.upload_to_stage(
        session,
        "@" + tmp_stage_name,
        test_files.test_xml_undeclared_attr_namespace,
        compress=False,
    )
    Utils.upload_to_stage(
        session,
        "@" + tmp_stage_name,
        test_files.test_null_value_xml,
        compress=False,
    )
    Utils.upload_to_stage(
        session,
        "@" + tmp_stage_name,
        test_files.test_books_xsd,
        compress=False,
    )
    Utils.upload_to_stage(
        session,
        "@" + tmp_stage_name,
        test_files.test_dk_trace_sample_xml,
        compress=False,
    )
    Utils.upload_to_stage(
        session,
        "@" + tmp_stage_name,
        test_files.test_dblp_6kb_xml,
        compress=False,
    )
    Utils.upload_to_stage(
        session,
        "@" + tmp_stage_name,
        test_files.test_books_attribute_value_xml,
        compress=False,
    )

    # Upload inline XML strings for mode tests
    for name, xml_str in {
        "sampling_mismatch": SAMPLING_MISMATCH_XML,
        "multifield_mismatch": MULTIFIELD_MISMATCH_XML,
    }.items():
        staged = _upload_xml_string(
            session, "@" + tmp_stage_name, f"{name}.xml", xml_str
        )
        _staged_files[name] = staged

    yield
    # Clean up resources
    if not local_testing_mode:
        session.sql(f"DROP STAGE IF EXISTS {tmp_stage_name}").collect()


@pytest.mark.parametrize(
    "file,row_tag,expected_row_count,expected_column_count",
    [
        [test_file_books_xml, "book", 12, 7],
        [test_file_books2_xml, "book", 2, 6],
        [test_file_house_xml, "House", 37, 22],
        [test_file_house_large_xml, "House", 740, 22],
        [test_file_dblp_xml, "mastersthesis", 6, 8],
        [test_file_books_attr_val_xml, "book", 5, 6],
    ],
)
def test_read_xml_row_tag(
    session, file, row_tag, expected_row_count, expected_column_count
):
    df = session.read.option("rowTag", row_tag).xml(f"@{tmp_stage_name}/{file}")
    # Use count() + len(df.columns) instead of collect() to avoid materializing
    # large result sets (e.g. 740 rows) that trigger paginated download URLs
    # unsupported by StoredProcRestfulSession inside a stored procedure.
    assert df.count() == expected_row_count
    assert len(df.columns) == expected_column_count


def test_read_xml_no_xxe(session):
    row_tag = "bar"
    stage_file_path = f"@{tmp_stage_name}/{test_file_xxe_xml}"
    df = session.read.option("rowTag", row_tag).xml(stage_file_path)
    Utils.check_answer(df, [Row("null")])


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


def test_read_xml_non_existing_file(session):
    row_tag = "tag"
    with pytest.raises(ValueError, match="does not exist"):
        session.read.option("rowTag", row_tag).xml(
            f"@{tmp_stage_name}/non_existing_file.xml"
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
    with pytest.raises(SnowparkSQLException, match="Malformed XML record at bytes"):
        df = (
            session.read.option("rowTag", row_tag)
            .option("mode", "failfast")
            .xml(file_path)
        )


def test_read_xml_row_tag_not_found(session):
    row_tag = "non-existing-tag"

    with pytest.raises(
        SnowparkDataframeReaderException, match="Cannot find the row tag"
    ):
        session.read.option("rowTag", row_tag).xml(
            f"@{tmp_stage_name}/{test_file_books_xml}"
        )

    df = (
        session.read.option("cacheResult", False)
        .option("rowTag", row_tag)
        .xml(f"@{tmp_stage_name}/{test_file_books_xml}")
    )

    with pytest.raises(
        SnowparkDataframeReaderException, match="Cannot find the row tag"
    ):
        df.collect()

    with pytest.raises(
        SnowparkDataframeReaderException, match="Cannot find the row tag"
    ):
        df.filter(lit(True)).collect()


def test_read_xml_declared_namespace(session):
    row_tag = "px:root"
    expected_items = [
        {"_id": "1", "name": "Item One", "value": "100"},
        {"_id": "2", "name": "Item Two", "value": "200"},
    ]
    expected_data = json.dumps(expected_items, indent=2)

    df = (
        session.read.option("rowTag", row_tag)
        .option("ignoreNamespace", True)
        .xml(f"@{tmp_stage_name}/{test_file_xml_declared_namespace}")
    )
    result = df.collect()
    assert len(result) == 1
    # Namespaces should be ignored
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
        .option("ignoreNamespace", False)
        .xml(f"@{tmp_stage_name}/{test_file_xml_declared_namespace}")
    )
    result = df.collect()
    print(result)
    assert len(result) == 1
    # Namespaces should be replaced with URI
    assert result[0]["'{http://example.com/px}item'"] == expected_data


@pytest.mark.parametrize("ignore_namespace", [True, False])
def test_read_xml_undeclared_namespace(session, ignore_namespace):
    # Read with undeclared namespace, ignoreNamespace=true and false should have the same result
    # Prefixes without declarations should remain as they don't follow {namespace}tag format
    row_tag = "px:item"
    df = (
        session.read.option("rowTag", row_tag)
        .option("ignoreNamespace", ignore_namespace)
        .xml(f"@{tmp_stage_name}/{test_file_xml_undeclared_namespace}")
    )
    result = df.collect()
    assert len(result) == 2
    assert result[0]["'px:name'"] in ['"Item One"', '"Item Two"']
    assert result[1]["'px:value'"] in ['"100"', '"200"']


@pytest.mark.parametrize("ignore_namespace", [True, False])
def test_read_xml_undeclared_attr_namespace(session, ignore_namespace):
    # File has attribute prefixes (e.g., diffgr:id, msdata:rowOrder) declared only on ancestors.
    # Reader extracts <Results> ... </Results> records without the declarations; parsing must still succeed.
    row_tag = "Results"
    df = (
        session.read.option("rowTag", row_tag)
        .option("cacheResult", False)
        .option("mode", "failfast")
        .option("ignoreNamespace", ignore_namespace)
        .xml(f"@{tmp_stage_name}/undeclared_attr_namespace.xml")
    )
    if not ignore_namespace:
        with pytest.raises(SnowparkSQLException, match="XMLSyntaxError"):
            df.collect()
    else:
        result = df.collect()
        assert len(result) == 3
        noms = {result[0]["'NOM'"], result[1]["'NOM'"], result[2]["'NOM'"]}
        assert '"CAMUT"' in noms
        assert any(v in noms for v in ['"CAMUT"', '"Test2"', '"Test3"'])


@pytest.mark.parametrize("attribute_prefix", ["_", ""])
def test_read_xml_attribute_prefix(session, attribute_prefix):
    row_tag = "book"
    df = (
        session.read.option("rowTag", row_tag)
        .option("attributePrefix", attribute_prefix)
        .xml(f"@{tmp_stage_name}/{test_file_books_xml}")
    )
    result = df.collect()
    assert len(result[0]) == 7
    assert result[0][f"'{attribute_prefix}id'"] is not None


def test_read_xml_exclude_attributes(session):
    row_tag = "book"
    df = (
        session.read.option("rowTag", row_tag)
        .option("excludeAttributes", True)
        .xml(f"@{tmp_stage_name}/{test_file_books_xml}")
    )
    result = df.collect()
    assert len(result[0]) == 6
    with pytest.raises(KeyError):
        _ = result[0]["'_id'"]


def test_read_xml_value_tag(session):
    row_tag = "author"
    df = (
        session.read.option("rowTag", row_tag)
        .option("valueTag", "value")
        .xml(f"@{tmp_stage_name}/{test_file_books_xml}")
    )
    result = df.collect()
    assert len(result) == 12
    assert len(result[0]) == 1
    assert result[0]["'value'"] is not None

    row_tag = "str3"
    df = (
        session.read.option("rowTag", row_tag)
        .option("valueTag", "value")
        .xml(f"@{tmp_stage_name}/{test_file_null_value_xml}")
    )
    result = df.collect()
    assert len(result) == 1
    assert len(result[0]) == 2
    assert result[0]["'value'"] == '"xxx"'


@pytest.mark.parametrize(
    "null_value, expected_row",
    [
        (
            "",
            Row('"1"', '"NULL"', "null", '{\n  "_VALUE": "xxx",\n  "_id": "empty"\n}'),
        ),
        (
            "NULL",
            Row('"1"', "null", "null", '{\n  "_VALUE": "xxx",\n  "_id": "empty"\n}'),
        ),
        (
            "empty",
            Row('"1"', '"NULL"', "null", '{\n  "_VALUE": "xxx",\n  "_id": null\n}'),
        ),
    ],
)
def test_read_xml_null_value(session, null_value, expected_row):
    row_tag = "test"
    df = (
        session.read.option("rowTag", row_tag)
        .option("nullValue", null_value)
        .xml(f"@{tmp_stage_name}/{test_file_null_value_xml}")
    )
    Utils.check_answer(df, [expected_row])


@pytest.mark.parametrize(
    "ignore_surrounding_whitespace, expected_row",
    [
        (
            True,
            Row('{\n  "_VALUE": "xxx",\n  "_id": null\n}'),
        ),
        (
            False,
            Row('{\n  "_VALUE": " xxx  ",\n  "_id": "  empty"\n}'),
        ),
    ],
)
def test_read_xml_ignore_surrounding_whitespace(
    session, ignore_surrounding_whitespace, expected_row
):
    row_tag = "test2"
    df = (
        session.read.option("rowTag", row_tag)
        .option("nullValue", "empty")
        .option("ignoreSurroundingWhitespace", ignore_surrounding_whitespace)
        .xml(f"@{tmp_stage_name}/{test_file_null_value_xml}")
    )
    Utils.check_answer(df, [expected_row])


def test_read_xml_warning_local_package(session, caplog):
    row_tag = "book"
    caplog.clear()
    with caplog.at_level(logging.WARNING):
        session.read.option("rowTag", row_tag).xml(
            f"@{tmp_stage_name}/{test_file_books_xml}"
        )
    assert (
        "Your UDF might not work when the package version is different between the server and your local environment"
        not in caplog.text
    )


def test_read_xml_row_validation_xsd_path(session):
    row_tag = "book"
    df = (
        session.read.option("rowTag", row_tag)
        .option("rowValidationXSDPath", f"@{tmp_stage_name}/{test_file_books_xsd}")
        .option("mode", "dropmalformed")
        .xml(f"@{tmp_stage_name}/{test_file_books_xml}")
    )

    # Only bk101 should pass XSD validation (author must be "Gambardella, Matthew")
    result = df.collect()
    assert len(result) == 1
    assert result[0]["'author'"] == '"Gambardella, Matthew"'
    assert result[0]["'title'"] == '"XML Developer\'s Guide"'
    assert result[0]["'genre'"] == '"Computer"'
    assert result[0]["'price'"] == '"44.95"'
    assert result[0]["'publish_date'"] == '"2000-10-01"'
    assert result[0]["'_id'"] == '"bk101"'


def test_read_xml_row_validation_xsd_path_failfast(session):
    row_tag = "book"
    with pytest.raises(SnowparkSQLException, match="XML record string:"):
        session.read.option("rowTag", row_tag).option(
            "rowValidationXSDPath", f"@{tmp_stage_name}/{test_file_books_xsd}"
        ).option("mode", "failfast").xml(f"@{tmp_stage_name}/{test_file_books_xml}")


def test_read_xml_with_custom_schema(session):

    # user input schema is missing description and adding 'extra_col',
    # the output shall have the structure as input schema, which does not have description
    # and have an 'extra_col' filled with null value
    # the case of schema is also preserved
    user_schema = StructType(
        [
            StructField("Author", StringType(), True),
            StructField("Title", StringType(), True),
            StructField("genre", StringType(), True),
            StructField("PRICE", DoubleType(), True),
            StructField("publish_Date", DateType(), True),
            StructField("extra_col", StringType(), True),
        ]
    )
    # case is preserved, same behavior as pyspark
    expected_schema = StructType(
        [
            StructField('"Author"', StringType(), nullable=True),
            StructField('"Title"', StringType(), nullable=True),
            StructField('"genre"', StringType(), nullable=True),
            StructField("PRICE", DoubleType(), nullable=True),
            StructField('"publish_Date"', DateType(), nullable=True),
            StructField('"extra_col"', StringType(), nullable=True),
        ]
    )

    df = (
        session.read.option("rowTag", "book")
        .schema(user_schema)
        .xml(f"@{tmp_stage_name}/{test_file_books_xml}")
    )
    expected_result = [
        Row(
            Author="Gambardella, Matthew",
            Title="XML Developer's Guide",
            genre="Computer",
            PRICE=44.95,
            publish_Date=datetime.date(2000, 10, 1),
            extra_col=None,
        ),
        Row(
            Author="Corets, Eva",
            Title="Maeve Ascendant",
            genre="Fantasy",
            PRICE=5.95,
            publish_Date=datetime.date(2000, 11, 17),
            extra_col=None,
        ),
        Row(
            Author="Kress, Peter",
            Title="Paradox Lost",
            genre="Science Fiction",
            PRICE=6.95,
            publish_Date=datetime.date(2000, 11, 2),
            extra_col=None,
        ),
        Row(
            Author="Ralls, Kim",
            Title="Midnight Rain",
            genre="Fantasy",
            PRICE=5.95,
            publish_Date=datetime.date(2000, 12, 16),
            extra_col=None,
        ),
        Row(
            Author="Knorr, Stefan",
            Title="Creepy Crawlies",
            genre="Horror",
            PRICE=4.95,
            publish_Date=datetime.date(2000, 12, 6),
            extra_col=None,
        ),
        Row(
            Author="Thurman, Paula",
            Title="Splish Splash",
            genre="Romance",
            PRICE=4.95,
            publish_Date=datetime.date(2000, 11, 2),
            extra_col=None,
        ),
        Row(
            Author="Randall, Cynthia",
            Title="Lover Birds",
            genre="Romance",
            PRICE=4.95,
            publish_Date=datetime.date(2000, 9, 2),
            extra_col=None,
        ),
        Row(
            Author="Corets, Eva",
            Title="The Sundered Grail",
            genre="Fantasy",
            PRICE=5.95,
            publish_Date=datetime.date(2001, 9, 10),
            extra_col=None,
        ),
        Row(
            Author="Corets, Eva",
            Title="Oberon's Legacy",
            genre="Fantasy",
            PRICE=5.95,
            publish_Date=datetime.date(2001, 3, 10),
            extra_col=None,
        ),
        Row(
            Author="O'Brien, Tim",
            Title="Microsoft .NET: The Programming Bible",
            genre="Computer",
            PRICE=36.95,
            publish_Date=datetime.date(2000, 12, 9),
            extra_col=None,
        ),
        Row(
            Author="O'Brien, Tim",
            Title="MSXML3: A Comprehensive Guide",
            genre="Computer",
            PRICE=36.95,
            publish_Date=datetime.date(2000, 12, 1),
            extra_col=None,
        ),
        Row(
            Author="Galos, Mike",
            Title="Visual Studio 7: A Comprehensive Guide",
            genre="Computer",
            PRICE=49.95,
            publish_Date=datetime.date(2001, 4, 16),
            extra_col=None,
        ),
    ]
    Utils.check_answer(df, expected_result)
    assert df.schema == expected_schema


def test_xml_custom_schema_nested(session):
    review_schema = StructType(
        [
            StructField('"User"', StringType(), True),
            StructField('"Rating"', StringType(), True),
            StructField("comment", StringType(), True),
        ]
    )

    edition_schema = StructType(
        [
            StructField("_year", StringType(), True),
            StructField("_format", StringType(), True),
        ]
    )

    user_schema = StructType(
        [
            StructField("Title", StringType(), True),
            StructField("Author", StringType(), True),
            StructField("Price", StringType(), True),
            StructField(
                "reviews",
                StructType(
                    [
                        StructField("review", ArrayType(review_schema), True),
                    ]
                ),
                True,
            ),
            StructField(
                "editions",
                StructType(
                    [
                        StructField("edition", ArrayType(edition_schema), True),
                    ]
                ),
                True,
            ),
        ]
    )
    df = (
        session.read.option("rowTag", "book")
        .schema(user_schema)
        .xml(f"@{tmp_stage_name}/{test_file_books2_xml}")
    )
    expected_res = [
        Row(
            Title="XML for Data Engineers",
            Author="John Smith",
            Price="35.50",
            reviews='{\n  "review": {\n    "Rating": "5",\n    "User": "xml_master",\n    "comment": "Perfect for mastering XML parsing."\n  }\n}',
            editions='{\n  "edition": {\n    "_format": "Paperback",\n    "_year": "2022"\n  }\n}',
        ),
        Row(
            Title="The Art of Snowflake",
            Author="Jane Doe",
            Price="29.99",
            reviews='{\n  "review": [\n    {\n      "Rating": "5",\n      "User": "tech_guru_87",\n      "comment": "Very insightful and practical."\n    },\n    {\n      "Rating": "4",\n      "User": "datawizard",\n      "comment": "Great read for data engineers."\n    }\n  ]\n}',
            editions='{\n  "edition": [\n    {\n      "_format": "Hardcover",\n      "_year": "2023"\n    },\n    {\n      "_format": "eBook",\n      "_year": "2024"\n    }\n  ]\n}',
        ),
    ]
    Utils.check_answer(df, expected_res)


def test_user_schema_without_rowtag(session):
    user_schema = StructType(
        [
            StructField("Author", StringType(), True),
            StructField("Title", StringType(), True),
            StructField("genre", StringType(), True),
            StructField("PRICE", DoubleType(), True),
            StructField("publish_Date", DateType(), True),
        ]
    )
    with pytest.raises(
        ValueError, match="When reading XML with user schema, rowtag must be set."
    ):
        session.read.schema(user_schema).xml(f"@{tmp_stage_name}/{test_file_books_xml}")


@pytest.mark.parametrize("ignore_namespace", [True, False])
def test_read_xml_custom_schema_with_colon_tags(session, ignore_namespace):
    schema = StructType(
        [
            StructField("px:name", StringType(), True),
            StructField("px:value", StringType(), True),
        ]
    )
    df = (
        session.read.option("rowTag", "px:item")
        .schema(schema)
        .option("ignoreNamespace", ignore_namespace)
        .xml(f"@{tmp_stage_name}/{test_file_xml_undeclared_namespace}")
    )
    result = df.collect()
    assert len(result) == 2
    names = {r[0] for r in result}
    values = {r[1] for r in result}
    assert names == {"Item One", "Item Two"}
    assert values == {"100", "200"}


def test_value_tag_custom_schema(session):
    test_schema1 = StructType(
        [
            StructField("num", StringType(), True),
            StructField("str1", StringType(), True),
            StructField("str2", StringType(), True),
            StructField(
                "str3",
                StructType(
                    [
                        StructField(
                            "_VALUE", StringType(), True
                        ),  # element text (because str3 has an attribute)
                        StructField(
                            "_id", StringType(), True
                        ),  # attribute id (default attributePrefix is "_")
                    ]
                ),
                True,
            ),
        ]
    )
    row_tag = "test"
    df = (
        session.read.option("rowTag", row_tag)
        .schema(test_schema1)
        .xml(f"@{tmp_stage_name}/{test_file_null_value_xml}")
    )
    Utils.check_answer(
        df,
        [
            Row(
                num="1",
                str1="NULL",
                str2=None,
                str3='{\n  "_VALUE": "xxx",\n  "_id": "empty"\n}',
            )
        ],
    )

    test_schema2 = StructType(
        [
            StructField("num", StringType(), True),
            StructField("str1", StringType(), True),
            StructField("str2", StringType(), True),
            StructField("str3", StringType(), True),
        ]
    )
    df = (
        session.read.option("rowTag", row_tag)
        .schema(test_schema2)
        .xml(f"@{tmp_stage_name}/{test_file_null_value_xml}")
    )
    Utils.check_answer(df, [Row(num="1", str1="NULL", str2=None, str3="xxx")])


def test_read_xml_namespace_user_schema(session):
    """User-provided schema with namespace-prefixed fields (ignoreNamespace=false)."""
    user_schema = StructType(
        [
            StructField("eqTrace:event-id", StringType(), True),
            StructField("eqTrace:event-name", StringType(), True),
            StructField("eqTrace:event-version-number", DoubleType(), True),
            StructField("eqTrace:is-planned", BooleanType(), True),
            StructField("eqTrace:is-synthetic", BooleanType(), True),
            StructField("eqTrace:date-time", TimestampType(), True),
        ]
    )
    df = (
        session.read.option("rowTag", "eqTrace:event")
        .option("ignoreNamespace", False)
        .schema(user_schema)
        .xml(f"@{tmp_stage_name}/{test_file_dk_trace_xml}")
    )
    result = df.collect()
    assert len(result) == 5
    assert len(result[0]) == 6
    col_names = [f.name.strip('"') for f in df.schema.fields]
    assert "eqTrace:event-id" in col_names
    assert "eqTrace:event-name" in col_names
    event_ids = {r[0] for r in result}
    assert "f0e765d9-599b-46bf-9aef-bd33e0c2183f" in event_ids
    assert "dd9a4616-e41c-4571-9bda-9a5506a2b78d" in event_ids


def test_read_xml_dblp_user_schema(session):
    """User-provided schema on dblp_6kb.xml mastersthesis with nested ee StructType."""
    user_schema = StructType(
        [
            StructField("_mdate", DateType(), True),
            StructField("_key", StringType(), True),
            StructField("author", StringType(), True),
            StructField("title", StringType(), True),
            StructField("year", LongType(), True),
            StructField("school", StringType(), True),
            StructField(
                "ee",
                StructType(
                    [
                        StructField("_VALUE", StringType(), True),
                        StructField("_type", StringType(), True),
                    ]
                ),
                True,
            ),
        ]
    )
    df = (
        session.read.option("rowTag", "mastersthesis")
        .schema(user_schema)
        .xml(f"@{tmp_stage_name}/{test_file_dblp_xml}")
    )
    result = df.collect()
    assert len(result) == 6
    assert len(result[0]) == 7
    brown = df.filter(col('"_key"') == "ms/Brown92").collect()
    assert len(brown) == 1
    assert json.loads(brown[0]["ee"]) == {"_VALUE": None, "_type": None}
    assert brown[0]["year"] == 1992


def test_read_xml_dblp_incollection_user_schema(session):
    """User-provided ArrayType(StructType) for author, StructType for ee on dblp incollection."""
    author_element_schema = StructType(
        [
            StructField("_VALUE", StringType(), True),
            StructField("_orcid", StringType(), True),
        ]
    )
    ee_schema = StructType(
        [
            StructField("_VALUE", StringType(), True),
            StructField("_type", StringType(), True),
        ]
    )
    user_schema = StructType(
        [
            StructField("_key", StringType(), True),
            StructField("_mdate", DateType(), True),
            StructField("author", ArrayType(author_element_schema, True), True),
            StructField("booktitle", StringType(), True),
            StructField("crossref", StringType(), True),
            StructField("ee", ee_schema, True),
            StructField("pages", StringType(), True),
            StructField("title", StringType(), True),
            StructField("url", StringType(), True),
            StructField("year", LongType(), True),
        ]
    )
    df = (
        session.read.option("rowTag", "incollection")
        .schema(user_schema)
        .xml(f"@{tmp_stage_name}/{test_file_dblp_xml}")
    )
    result = df.collect()
    assert len(result) == 6
    assert len(result[0]) == 11

    parker = df.filter(col('"_key"') == "series/ifip/ParkerD14").collect()
    assert len(parker) == 1
    authors = json.loads(parker[0]["author"])
    assert len(authors) == 2
    assert authors[0]["_VALUE"] == "Kevin R. Parker"
    assert authors[0]["_orcid"] == "0000-0003-0549-3687"
    assert authors[1]["_VALUE"] == "Bill Davey"
    assert authors[1]["_orcid"] is None
    ee = json.loads(parker[0]["ee"])
    assert ee["_VALUE"] == "https://doi.org/10.1007/978-3-642-55119-2_14"
    assert ee["_type"] == "oa"

    scheid = df.filter(col('"_key"') == "series/ifip/ScheidRKFRS21").collect()
    assert len(scheid) == 1
    scheid_authors = json.loads(scheid[0]["author"])
    assert len(scheid_authors) == 6
    assert all(a["_orcid"] is not None for a in scheid_authors)

    rheingans = df.filter(col('"_key"') == "series/ifip/RheingansL95").collect()
    assert len(rheingans) == 1
    ee_no_type = json.loads(rheingans[0]["ee"])
    assert ee_no_type["_type"] is None
    assert "doi.org" in ee_no_type["_VALUE"]
    assert parker[0]["year"] == 2014


def test_read_xml_attribute_value_user_schema_struct_publisher(session):
    """User StructType schema for publisher on books_attribute_value.xml."""
    publisher_schema = StructType(
        [
            StructField("_VALUE", StringType(), True),
            StructField("_country", StringType(), True),
            StructField("_language", StringType(), True),
        ]
    )
    user_schema = StructType(
        [
            StructField("_id", LongType(), True),
            StructField("title", StringType(), True),
            StructField("author", StringType(), True),
            StructField("price", DoubleType(), True),
            StructField("publisher", publisher_schema, True),
        ]
    )
    df = (
        session.read.option("rowTag", "book")
        .schema(user_schema)
        .xml(f"@{tmp_stage_name}/{test_file_books_attr_val_xml}")
    )
    result = df.collect()
    assert len(result) == 5
    assert len(result[0]) == 5

    book1 = df.filter(col('"_id"') == 1).collect()
    assert len(book1) == 1
    pub1 = json.loads(book1[0]["publisher"])
    assert pub1 == {"_VALUE": "O'Reilly Media", "_country": "USA", "_language": None}

    book3 = df.filter(col('"_id"') == 3).collect()
    assert len(book3) == 1
    pub3 = json.loads(book3[0]["publisher"])
    assert pub3 == {"_VALUE": "Springer", "_country": "Canada", "_language": "English"}

    book4 = df.filter(col('"_id"') == 4).collect()
    assert len(book4) == 1
    pub4 = json.loads(book4[0]["publisher"])
    assert pub4 == {"_VALUE": "Some Publisher", "_country": None, "_language": None}
    assert book1[0]["price"] == 29.99
    assert book1[0]["_id"] == 1


def test_permissive_type_mismatch_user_schema(session, enable_scos_compatible_mode):
    schema = StructType(
        [
            StructField("name", StringType()),
            StructField("value", LongType()),
        ]
    )
    df = (
        session.read.option("rowTag", "ROW")
        .schema(schema)
        .xml(f"@{tmp_stage_name}/{_staged_files['sampling_mismatch']}")
    )
    result = df.order_by('"name"').collect()
    assert len(result) == 6

    alice = [r for r in result if r["name"] == "Alice"][0]
    assert alice["value"] == 100
    assert alice["_corrupt_record"] is None

    frank = [r for r in result if r["name"] == "Frank"][0]
    assert frank["value"] is None
    assert frank["_corrupt_record"] is not None
    assert "hello" in frank["_corrupt_record"]

    col_names = [c.strip('"') for c in df.columns]
    assert "_corrupt_record" in col_names


def test_permissive_multifield_per_field_granularity(
    session, enable_scos_compatible_mode
):
    schema = StructType(
        [
            StructField("int_col", LongType()),
            StructField("bool_col", BooleanType()),
            StructField("dbl_col", DoubleType()),
        ]
    )
    df = (
        session.read.option("rowTag", "ROW")
        .schema(schema)
        .xml(f"@{tmp_stage_name}/{_staged_files['multifield_mismatch']}")
    )
    result = df.order_by('"int_col"').collect()
    assert len(result) == 3

    assert result[0]["int_col"] is None
    assert result[0]["bool_col"] is None
    assert abs(result[0]["dbl_col"] - 2.72) < 0.001

    assert result[1]["int_col"] == 42
    assert result[1]["bool_col"] is True
    assert abs(result[1]["dbl_col"] - 3.14) < 0.001

    assert result[2]["int_col"] == 99
    assert result[2]["bool_col"] is False
    assert result[2]["dbl_col"] is None


def test_failfast_type_mismatch_raises(session, enable_scos_compatible_mode):
    narrow_schema = StructType(
        [
            StructField("name", StringType()),
            StructField("value", LongType()),
        ]
    )
    with pytest.raises(SnowparkSQLException):
        session.read.option("rowTag", "ROW").option("mode", "FAILFAST").schema(
            narrow_schema
        ).xml(f"@{tmp_stage_name}/{_staged_files['sampling_mismatch']}")


def test_dropmalformed_type_mismatch_drops_rows(session, enable_scos_compatible_mode):
    narrow_schema = StructType(
        [
            StructField("name", StringType()),
            StructField("value", LongType()),
        ]
    )
    df = (
        session.read.option("rowTag", "ROW")
        .option("mode", "DROPMALFORMED")
        .schema(narrow_schema)
        .xml(f"@{tmp_stage_name}/{_staged_files['sampling_mismatch']}")
    )
    result = df.order_by('"name"').collect()
    assert len(result) == 5
    names = [r["name"] for r in result]
    assert "Frank" not in names
    for r in result:
        assert r["value"] is not None


def test_dropmalformed_multifield_drops_any_bad_field(
    session, enable_scos_compatible_mode
):
    schema = StructType(
        [
            StructField("int_col", LongType()),
            StructField("bool_col", BooleanType()),
            StructField("dbl_col", DoubleType()),
        ]
    )
    df = (
        session.read.option("rowTag", "ROW")
        .option("mode", "DROPMALFORMED")
        .schema(schema)
        .xml(f"@{tmp_stage_name}/{_staged_files['multifield_mismatch']}")
    )
    result = df.collect()
    assert len(result) == 1
    assert result[0]["int_col"] == 42
    assert result[0]["bool_col"] is True
    assert abs(result[0]["dbl_col"] - 3.14) < 0.001
