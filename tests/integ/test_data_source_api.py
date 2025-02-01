#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import decimal
import time
from _decimal import Decimal
import datetime
from unittest import mock
from unittest.mock import MagicMock
import pytest

from snowflake.snowpark.dataframe_reader import (
    task_fetch_from_data_source_with_retry,
    MAX_RETRY_TIME,
)
from snowflake.snowpark.types import IntegerType, DateType, MapType

SQL_SERVER_TABLE_NAME = "RandomDataWith100Columns"


schema = (
    ("ID", int, None, 10, 10, 0, False),
    ("Column0", int, None, 10, 10, 0, True),
    ("Column1", float, None, 53, 53, 0, True),
    ("Column2", decimal.Decimal, None, 10, 10, 2, True),
    ("Column3", str, None, 1, 1, 0, True),
    ("Column4", str, None, 50, 50, 0, True),
    ("Column5", datetime.datetime, None, 23, 23, 3, True),
    ("Column6", bool, None, 1, 1, 0, True),
    ("Column7", int, None, 10, 10, 0, True),
    ("Column8", float, None, 53, 53, 0, True),
    ("Column9", decimal.Decimal, None, 10, 10, 2, True),
    ("Column10", str, None, 1, 1, 0, True),
    ("Column11", str, None, 50, 50, 0, True),
    ("Column12", datetime.datetime, None, 23, 23, 3, True),
    ("Column13", bool, None, 1, 1, 0, True),
    ("Column14", int, None, 10, 10, 0, True),
    ("Column15", float, None, 53, 53, 0, True),
    ("Column16", decimal.Decimal, None, 10, 10, 2, True),
    ("Column17", str, None, 1, 1, 0, True),
    ("Column18", str, None, 50, 50, 0, True),
    ("Column19", datetime.datetime, None, 23, 23, 3, True),
    ("Column20", bool, None, 1, 1, 0, True),
    ("Column21", int, None, 10, 10, 0, True),
    ("Column22", float, None, 53, 53, 0, True),
    ("Column23", decimal.Decimal, None, 10, 10, 2, True),
    ("Column24", str, None, 1, 1, 0, True),
    ("Column25", str, None, 50, 50, 0, True),
    ("Column26", datetime.datetime, None, 23, 23, 3, True),
    ("Column27", bool, None, 1, 1, 0, True),
    ("Column28", int, None, 10, 10, 0, True),
    ("Column29", float, None, 53, 53, 0, True),
    ("Column30", decimal.Decimal, None, 10, 10, 2, True),
    ("Column31", str, None, 1, 1, 0, True),
    ("Column32", str, None, 50, 50, 0, True),
    ("Column33", datetime.datetime, None, 23, 23, 3, True),
    ("Column34", bool, None, 1, 1, 0, True),
    ("Column35", int, None, 10, 10, 0, True),
    ("Column36", float, None, 53, 53, 0, True),
    ("Column37", decimal.Decimal, None, 10, 10, 2, True),
    ("Column38", str, None, 1, 1, 0, True),
    ("Column39", str, None, 50, 50, 0, True),
    ("Column40", datetime.datetime, None, 23, 23, 3, True),
    ("Column41", bool, None, 1, 1, 0, True),
    ("Column42", int, None, 10, 10, 0, True),
    ("Column43", float, None, 53, 53, 0, True),
    ("Column44", decimal.Decimal, None, 10, 10, 2, True),
    ("Column45", str, None, 1, 1, 0, True),
    ("Column46", str, None, 50, 50, 0, True),
    ("Column47", datetime.datetime, None, 23, 23, 3, True),
    ("Column48", bool, None, 1, 1, 0, True),
    ("Column49", int, None, 10, 10, 0, True),
    ("Column50", float, None, 53, 53, 0, True),
    ("Column51", decimal.Decimal, None, 10, 10, 2, True),
    ("Column52", str, None, 1, 1, 0, True),
    ("Column53", str, None, 50, 50, 0, True),
    ("Column54", datetime.datetime, None, 23, 23, 3, True),
    ("Column55", bool, None, 1, 1, 0, True),
    ("Column56", int, None, 10, 10, 0, True),
    ("Column57", float, None, 53, 53, 0, True),
    ("Column58", decimal.Decimal, None, 10, 10, 2, True),
    ("Column59", str, None, 1, 1, 0, True),
    ("Column60", str, None, 50, 50, 0, True),
    ("Column61", datetime.datetime, None, 23, 23, 3, True),
    ("Column62", bool, None, 1, 1, 0, True),
    ("Column63", int, None, 10, 10, 0, True),
    ("Column64", float, None, 53, 53, 0, True),
    ("Column65", decimal.Decimal, None, 10, 10, 2, True),
    ("Column66", str, None, 1, 1, 0, True),
    ("Column67", str, None, 50, 50, 0, True),
    ("Column68", datetime.datetime, None, 23, 23, 3, True),
    ("Column69", bool, None, 1, 1, 0, True),
    ("Column70", int, None, 10, 10, 0, True),
    ("Column71", float, None, 53, 53, 0, True),
    ("Column72", decimal.Decimal, None, 10, 10, 2, True),
    ("Column73", str, None, 1, 1, 0, True),
    ("Column74", str, None, 50, 50, 0, True),
    ("Column75", datetime.datetime, None, 23, 23, 3, True),
    ("Column76", bool, None, 1, 1, 0, True),
    ("Column77", int, None, 10, 10, 0, True),
    ("Column78", float, None, 53, 53, 0, True),
    ("Column79", decimal.Decimal, None, 10, 10, 2, True),
    ("Column80", str, None, 1, 1, 0, True),
    ("Column81", str, None, 50, 50, 0, True),
    ("Column82", datetime.datetime, None, 23, 23, 3, True),
    ("Column83", bool, None, 1, 1, 0, True),
    ("Column84", int, None, 10, 10, 0, True),
    ("Column85", float, None, 53, 53, 0, True),
    ("Column86", decimal.Decimal, None, 10, 10, 2, True),
    ("Column87", str, None, 1, 1, 0, True),
    ("Column88", str, None, 50, 50, 0, True),
    ("Column89", datetime.datetime, None, 23, 23, 3, True),
    ("Column90", bool, None, 1, 1, 0, True),
    ("Column91", int, None, 10, 10, 0, True),
    ("Column92", float, None, 53, 53, 0, True),
    ("Column93", decimal.Decimal, None, 10, 10, 2, True),
    ("Column94", str, None, 1, 1, 0, True),
    ("Column95", str, None, 50, 50, 0, True),
    ("Column96", datetime.datetime, None, 23, 23, 3, True),
    ("Column97", bool, None, 1, 1, 0, True),
    ("Column98", int, None, 10, 10, 0, True),
    ("Column99", float, None, 53, 53, 0, True),
    ("Column100", decimal.Decimal, None, 10, 10, 2, True),
    ("Column101", str, None, 1, 1, 0, True),
    ("Column102", str, None, 50, 50, 0, True),
    ("Column103", datetime.datetime, None, 23, 23, 3, True),
    ("Column104", bool, None, 1, 1, 0, True),
)
rows = [
    (
        1,
        77600,
        0.3571664774352337,
        Decimal("380.01"),
        "L",
        "6BFA6307",
        datetime.datetime(2008, 5, 17, 0, 0),
        False,
        20699,
        0.09502200342803091,
        Decimal("686.56"),
        "X",
        "F20C10FC",
        datetime.datetime(2016, 7, 14, 0, 0),
        True,
        56445,
        0.1736087185152762,
        Decimal("753.87"),
        "F",
        "241CF9EF",
        datetime.datetime(2021, 9, 14, 0, 0),
        True,
        73981,
        0.26741937438621344,
        Decimal("308.05"),
        "Z",
        "9AFC11D0",
        datetime.datetime(2007, 3, 14, 0, 0),
        False,
        88222,
        0.48778831285963614,
        Decimal("829.69"),
        "G",
        "200C4DB6",
        datetime.datetime(2025, 1, 20, 0, 0),
        False,
        88706,
        0.7208103836472111,
        Decimal("366.81"),
        "O",
        "DC62F922",
        datetime.datetime(2007, 5, 10, 0, 0),
        False,
        82391,
        0.4788805474667721,
        Decimal("241.61"),
        "Z",
        "465D9716",
        datetime.datetime(2012, 9, 8, 0, 0),
        True,
        22826,
        0.0877749715641643,
        Decimal("803.63"),
        "I",
        "CB26C4A1",
        datetime.datetime(2012, 5, 25, 0, 0),
        True,
        16205,
        0.5196701536968851,
        Decimal("384.86"),
        "V",
        "13D3EEAF",
        datetime.datetime(2010, 12, 24, 0, 0),
        True,
        96319,
        0.811693761735523,
        Decimal("464.40"),
        "D",
        "1D185465",
        datetime.datetime(2001, 11, 22, 0, 0),
        False,
        58127,
        0.12127042380081851,
        Decimal("437.35"),
        "B",
        "01603573",
        datetime.datetime(2006, 9, 16, 0, 0),
        True,
        16035,
        0.2572325347254611,
        Decimal("685.59"),
        "D",
        "B35B0B36",
        datetime.datetime(2016, 5, 28, 0, 0),
        True,
        82243,
        0.6567410735603615,
        Decimal("161.77"),
        "Q",
        "0532F9B1",
        datetime.datetime(2021, 6, 3, 0, 0),
        True,
        86224,
        0.5135530946122759,
        Decimal("988.84"),
        "J",
        "6840D2F8",
        datetime.datetime(2026, 7, 31, 0, 0),
        True,
        47844,
        0.849711705341906,
        Decimal("508.25"),
        "E",
        "27BC8967",
        datetime.datetime(2026, 10, 11, 0, 0),
        False,
    ),
    (
        2,
        18748,
        0.11532269592869611,
        Decimal("189.56"),
        "Q",
        "9A998CD6",
        datetime.datetime(2015, 4, 26, 0, 0),
        False,
        41588,
        0.7365175178788479,
        Decimal("210.79"),
        "B",
        "DA2AA32A",
        datetime.datetime(2006, 3, 27, 0, 0),
        True,
        21722,
        0.030756712332495502,
        Decimal("707.55"),
        "U",
        "D8662363",
        datetime.datetime(2002, 9, 6, 0, 0),
        False,
        36446,
        0.9950733241831643,
        Decimal("516.67"),
        "B",
        "C258808B",
        datetime.datetime(2026, 6, 4, 0, 0),
        False,
        62772,
        0.2580659385329584,
        Decimal("59.27"),
        "T",
        "0B392A0D",
        datetime.datetime(2012, 10, 3, 0, 0),
        False,
        67827,
        0.1176098058391464,
        Decimal("857.89"),
        "T",
        "6854888E",
        datetime.datetime(2002, 3, 7, 0, 0),
        False,
        50891,
        0.9084495679354407,
        Decimal("579.38"),
        "Z",
        "F7407026",
        datetime.datetime(2027, 1, 20, 0, 0),
        False,
        54661,
        0.44010223669949033,
        Decimal("294.29"),
        "S",
        "85DE6F36",
        datetime.datetime(2010, 10, 29, 0, 0),
        True,
        50479,
        0.9595201835796825,
        Decimal("707.29"),
        "Q",
        "03590539",
        datetime.datetime(2017, 5, 1, 0, 0),
        True,
        83310,
        0.49523856583408093,
        Decimal("316.18"),
        "S",
        "18E72E79",
        datetime.datetime(2015, 5, 12, 0, 0),
        True,
        94838,
        0.7765976462775713,
        Decimal("889.53"),
        "I",
        "851B204F",
        datetime.datetime(2017, 8, 18, 0, 0),
        False,
        21570,
        0.2642281555877964,
        Decimal("537.08"),
        "I",
        "5B3E7D16",
        datetime.datetime(2017, 2, 17, 0, 0),
        True,
        28132,
        0.2673651341579894,
        Decimal("799.66"),
        "U",
        "EBB8E346",
        datetime.datetime(2024, 12, 21, 0, 0),
        True,
        33464,
        0.39357737546654153,
        Decimal("12.67"),
        "H",
        "C6D2E4E5",
        datetime.datetime(2012, 12, 14, 0, 0),
        True,
        89736,
        0.7970062837241896,
        Decimal("542.29"),
        "C",
        "27D1A360",
        datetime.datetime(2006, 10, 21, 0, 0),
        True,
    ),
    (
        3,
        15938,
        0.3362642893020359,
        Decimal("164.21"),
        "U",
        "EB96256A",
        datetime.datetime(2004, 11, 9, 0, 0),
        False,
        61714,
        0.9707415959891611,
        Decimal("684.78"),
        "O",
        "F719AD59",
        datetime.datetime(2021, 7, 7, 0, 0),
        False,
        64260,
        0.453853810469293,
        Decimal("680.80"),
        "Z",
        "B9A6B191",
        datetime.datetime(2026, 1, 28, 0, 0),
        False,
        26250,
        0.9512013656442999,
        Decimal("651.61"),
        "Z",
        "18F3306A",
        datetime.datetime(2003, 8, 27, 0, 0),
        False,
        31878,
        0.2910646256230791,
        Decimal("235.72"),
        "I",
        "53BDD233",
        datetime.datetime(2017, 4, 21, 0, 0),
        True,
        88696,
        0.7341953245803944,
        Decimal("360.35"),
        "X",
        "C53F0491",
        datetime.datetime(2009, 10, 21, 0, 0),
        False,
        20254,
        0.8873763167953814,
        Decimal("976.06"),
        "U",
        "F7568D45",
        datetime.datetime(2023, 5, 4, 0, 0),
        False,
        36170,
        0.5231413364507655,
        Decimal("261.63"),
        "Z",
        "7AFFB010",
        datetime.datetime(2012, 4, 1, 0, 0),
        True,
        28102,
        0.30873884450240263,
        Decimal("650.31"),
        "J",
        "B76B5D4F",
        datetime.datetime(2025, 10, 6, 0, 0),
        False,
        49383,
        0.13138130572163742,
        Decimal("322.97"),
        "A",
        "83C2D793",
        datetime.datetime(2001, 10, 1, 0, 0),
        False,
        6167,
        0.6706595030871801,
        Decimal("15.13"),
        "P",
        "1A74C534",
        datetime.datetime(2004, 3, 17, 0, 0),
        True,
        19333,
        0.9116856178098539,
        Decimal("755.10"),
        "Q",
        "6D98D4C3",
        datetime.datetime(2016, 7, 4, 0, 0),
        True,
        92188,
        0.16072985362631992,
        Decimal("433.73"),
        "G",
        "E6201FBD",
        datetime.datetime(2023, 2, 15, 0, 0),
        False,
        4891,
        0.2841543232241298,
        Decimal("846.33"),
        "R",
        "128F09A2",
        datetime.datetime(2006, 7, 19, 0, 0),
        True,
        66427,
        0.7581234240788156,
        Decimal("39.97"),
        "X",
        "77D5DCA1",
        datetime.datetime(2002, 6, 30, 0, 0),
        True,
    ),
    (
        4,
        20309,
        0.9846162086502951,
        Decimal("43.37"),
        "F",
        "DC4B2491",
        datetime.datetime(2006, 7, 12, 0, 0),
        False,
        86320,
        0.3731676064629643,
        Decimal("305.27"),
        "Q",
        "679941DC",
        datetime.datetime(2010, 9, 8, 0, 0),
        True,
        37690,
        0.6752032425820279,
        Decimal("302.97"),
        "P",
        "9EB7AA28",
        datetime.datetime(2009, 2, 18, 0, 0),
        False,
        86385,
        0.2456679141053982,
        Decimal("904.57"),
        "A",
        "B29AD12F",
        datetime.datetime(2008, 8, 29, 0, 0),
        False,
        5348,
        0.591785691044678,
        Decimal("519.35"),
        "V",
        "E4579FC9",
        datetime.datetime(2021, 1, 11, 0, 0),
        False,
        54437,
        0.6931176104443423,
        Decimal("706.26"),
        "N",
        "20B8C7FF",
        datetime.datetime(2016, 2, 4, 0, 0),
        False,
        75586,
        0.2118164530946841,
        Decimal("789.52"),
        "I",
        "66683418",
        datetime.datetime(2001, 10, 9, 0, 0),
        True,
        64234,
        0.37167135754364333,
        Decimal("914.31"),
        "B",
        "FDA7CCF1",
        datetime.datetime(2000, 3, 3, 0, 0),
        True,
        94374,
        0.1371325766792254,
        Decimal("871.58"),
        "T",
        "5C9E3CEC",
        datetime.datetime(2013, 10, 11, 0, 0),
        True,
        18653,
        0.8604774472145762,
        Decimal("401.87"),
        "M",
        "6043D3FA",
        datetime.datetime(2026, 6, 8, 0, 0),
        False,
        55607,
        0.2693989173363202,
        Decimal("856.72"),
        "N",
        "30743B0B",
        datetime.datetime(2009, 7, 4, 0, 0),
        False,
        78563,
        0.8073492624127997,
        Decimal("263.29"),
        "G",
        "728CECAE",
        datetime.datetime(2010, 5, 4, 0, 0),
        False,
        36373,
        0.36274791752576,
        Decimal("865.55"),
        "T",
        "C93F7F3D",
        datetime.datetime(2024, 12, 20, 0, 0),
        True,
        81149,
        0.811763867044238,
        Decimal("735.54"),
        "C",
        "B16C20A3",
        datetime.datetime(2020, 5, 6, 0, 0),
        True,
        55388,
        0.42196391043192494,
        Decimal("627.24"),
        "E",
        "0CD60FC0",
        datetime.datetime(2001, 4, 3, 0, 0),
        True,
    ),
    (
        5,
        99893,
        0.17584771909417832,
        Decimal("494.22"),
        "R",
        "BD6253C2",
        datetime.datetime(2018, 5, 9, 0, 0),
        False,
        9870,
        0.8684083767376395,
        Decimal("214.82"),
        "J",
        "0EDE9A14",
        datetime.datetime(2019, 10, 1, 0, 0),
        True,
        87440,
        0.44413828430540014,
        Decimal("900.28"),
        "S",
        "83AE0516",
        datetime.datetime(2014, 4, 8, 0, 0),
        False,
        73881,
        0.08484643183941601,
        Decimal("767.97"),
        "G",
        "99F27459",
        datetime.datetime(2005, 4, 26, 0, 0),
        False,
        37967,
        0.9206897266630427,
        Decimal("529.30"),
        "F",
        "F2219EE7",
        datetime.datetime(2000, 12, 21, 0, 0),
        True,
        54569,
        0.5188709587654697,
        Decimal("96.94"),
        "V",
        "950FF8CB",
        datetime.datetime(2021, 10, 5, 0, 0),
        True,
        4418,
        0.8944625064522602,
        Decimal("277.46"),
        "H",
        "5FA0A903",
        datetime.datetime(2014, 1, 20, 0, 0),
        True,
        70833,
        0.8917340469526216,
        Decimal("608.78"),
        "Y",
        "32597D4C",
        datetime.datetime(2010, 3, 29, 0, 0),
        False,
        23429,
        0.2144996824465924,
        Decimal("158.12"),
        "G",
        "31EED035",
        datetime.datetime(2010, 1, 17, 0, 0),
        True,
        31272,
        0.0997469899423712,
        Decimal("69.40"),
        "D",
        "944F05A7",
        datetime.datetime(2003, 7, 16, 0, 0),
        False,
        748,
        0.9282010711404185,
        Decimal("402.84"),
        "A",
        "619FE729",
        datetime.datetime(2000, 7, 11, 0, 0),
        True,
        42083,
        0.8646879916137736,
        Decimal("252.26"),
        "Q",
        "BD09463D",
        datetime.datetime(2018, 2, 22, 0, 0),
        False,
        735,
        0.0173174240689892,
        Decimal("97.40"),
        "Q",
        "AC650522",
        datetime.datetime(2018, 3, 11, 0, 0),
        True,
        16145,
        0.0779280464916213,
        Decimal("683.86"),
        "S",
        "C6EA5560",
        datetime.datetime(2014, 5, 3, 0, 0),
        False,
        92894,
        0.684080147203557,
        Decimal("49.70"),
        "N",
        "D6792337",
        datetime.datetime(2010, 10, 23, 0, 0),
        True,
    ),
]
mock_conn = MagicMock()

mock_cursor = MagicMock()
mock_cursor.description = schema
mock_cursor.execute.return_value = mock_cursor
mock_cursor.fetchall.return_value = rows
mock_conn.cursor.return_value = mock_cursor


# we manually mock these objects because mock object cannot be used in multi-process as they are not pickleable
class FakeConnection:
    def cursor(self):
        return self

    def execute(self, sql: str):
        return self

    def fetchall(self):
        return rows

    description = schema


def create_connection():
    return FakeConnection()


def fake_task_fetch_from_data_source_with_retry(
    create_connection, query, schema, i, tmp_dir, query_timeout
):
    time.sleep(2)


def upload_and_copy_into_table_with_retry(
    self,
    local_file,
    snowflake_stage_name,
    snowflake_table_name,
    on_error,
):
    time.sleep(2)


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="feature not available in local testing",
)
def test_dbapi_with_temp_table(session):
    df = session.read.dbapi(create_connection, SQL_SERVER_TABLE_NAME, max_workers=4)
    assert df.collect() == rows


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="feature not available in local testing",
)
def test_dbapi_retry(session):

    with mock.patch(
        "snowflake.snowpark.dataframe_reader._task_fetch_from_data_source",
        side_effect=Exception("Test error"),
    ) as mock_task:
        result = task_fetch_from_data_source_with_retry(
            create_connection=create_connection,
            query="SELECT * FROM test_table",
            schema=(("col1", int, 0, 0, 0, 0, False),),
            i=0,
            tmp_dir="/tmp",
        )
        assert mock_task.call_count == MAX_RETRY_TIME
        assert isinstance(result, Exception)

    with mock.patch(
        "snowflake.snowpark.dataframe_reader.DataFrameReader._upload_and_copy_into_table",
        side_effect=Exception("Test error"),
    ) as mock_task:
        result = session.read.upload_and_copy_into_table_with_retry(
            local_file="fake_file",
            snowflake_stage_name="fake_stage",
            snowflake_table_name="fake_table",
        )
        assert mock_task.call_count == MAX_RETRY_TIME
        assert isinstance(result, Exception)


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="feature not available in local testing",
)
def test_parallel(session):
    num_partitions = 3
    # this test meant to test whether ingest is fully parallelized
    # we cannot mock this function as process pool does not all mock object
    with mock.patch(
        "snowflake.snowpark.dataframe_reader.task_fetch_from_data_source_with_retry",
        new=fake_task_fetch_from_data_source_with_retry,
    ):
        with mock.patch(
            "snowflake.snowpark.dataframe_reader.DataFrameReader.upload_and_copy_into_table_with_retry",
            wrap=upload_and_copy_into_table_with_retry,
        ) as mock_upload_and_copy:
            start = time.time()
            session.read.dbapi(
                create_connection,
                SQL_SERVER_TABLE_NAME,
                column="ID",
                upper_bound=100,
                lower_bound=0,
                num_partitions=num_partitions,
                max_workers=4,
            )
            end = time.time()
            # totally time without parallel is 12 seconds
            assert end - start < 12
            # verify that mocked function is called for each partition
            assert mock_upload_and_copy.call_count == num_partitions


def test_partition_logic(session):
    # same result as spark
    expected_queries1 = [
        "SELECT * FROM fake_table WHERE ID < '8' OR ID is null",
        "SELECT * FROM fake_table WHERE ID >= '8' AND ID < '10'",
        "SELECT * FROM fake_table WHERE ID >= '10' AND ID < '12'",
        "SELECT * FROM fake_table WHERE ID >= '12'",
    ]
    expected_queries2 = [
        "SELECT * FROM fake_table WHERE ID < '-2' OR ID is null",
        "SELECT * FROM fake_table WHERE ID >= '-2' AND ID < '0'",
        "SELECT * FROM fake_table WHERE ID >= '0' AND ID < '2'",
        "SELECT * FROM fake_table WHERE ID >= '2'",
    ]
    queries = session.read._generate_partition(
        table="fake_table",
        column_type=IntegerType(),
        column="ID",
        lower_bound=5,
        upper_bound=15,
        num_partitions=4,
    )
    for r, expected_r in zip(queries, expected_queries1):
        assert r == expected_r

    queries = session.read._generate_partition(
        table="fake_table",
        column_type=IntegerType(),
        column="ID",
        lower_bound=-5,
        upper_bound=5,
        num_partitions=4,
    )
    for r, expected_r in zip(queries, expected_queries2):
        assert r == expected_r


def test_partition_date_timestamp(session):
    expected_queries1 = [
        "SELECT * FROM fake_table WHERE DATE < '2020-07-30 18:00:00+00:00' OR DATE is null",
        "SELECT * FROM fake_table WHERE DATE >= '2020-07-30 18:00:00+00:00' AND DATE < '2020-09-14 12:00:00+00:00'",
        "SELECT * FROM fake_table WHERE DATE >= '2020-09-14 12:00:00+00:00' AND DATE < '2020-10-30 06:00:00+00:00'",
        "SELECT * FROM fake_table WHERE DATE >= '2020-10-30 06:00:00+00:00'",
    ]
    queries = session.read._generate_partition(
        table="fake_table",
        column_type=DateType(),
        column="DATE",
        lower_bound=str(datetime.date(2020, 6, 15)),
        upper_bound=str(datetime.date(2020, 12, 15)),
        num_partitions=4,
    )

    for r, expected_r in zip(queries, expected_queries1):
        assert r == expected_r

    expected_queries2 = [
        "SELECT * FROM fake_table WHERE DATE < '2020-07-31 05:06:13+00:00' OR DATE is null",
        "SELECT * FROM fake_table WHERE DATE >= '2020-07-31 05:06:13+00:00' AND DATE < '2020-09-14 21:46:55+00:00'",
        "SELECT * FROM fake_table WHERE DATE >= '2020-09-14 21:46:55+00:00' AND DATE < '2020-10-30 14:27:37+00:00'",
        "SELECT * FROM fake_table WHERE DATE >= '2020-10-30 14:27:37+00:00'",
    ]
    queries = session.read._generate_partition(
        table="fake_table",
        column_type=DateType(),
        column="DATE",
        lower_bound=str(datetime.datetime(2020, 6, 15, 12, 25, 30)),
        upper_bound=str(datetime.datetime(2020, 12, 15, 7, 8, 20)),
        num_partitions=4,
    )

    for r, expected_r in zip(queries, expected_queries2):
        assert r == expected_r


def test_partition_unsupported_type(session):
    with pytest.raises(TypeError, match="unsupported column type for partition:"):
        session.read._generate_partition(
            table="fake_table",
            column_type=MapType(),
            column="DATE",
            lower_bound=0,
            upper_bound=1,
            num_partitions=4,
        )
