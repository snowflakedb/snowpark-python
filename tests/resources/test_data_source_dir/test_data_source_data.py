import datetime
import sqlite3
from _decimal import Decimal
from dateutil import parser
from oracledb import LOB

from snowflake.snowpark._internal.data_source_utils import DBMS_TYPE


# we manually mock these objects because mock object cannot be used in multi-process as they are not pickleable
class FakeConnection:
    def __init__(self, data, schema, connection_type) -> None:
        self.__class__.__module__ = connection_type
        self.sql = ""
        self.start_index = 0
        self.data = data
        self.schema = schema

    def cursor(self):
        return self

    def execute(self, sql: str):
        self.sql = sql
        return self

    def fetchall(self):
        if "INFORMATION_SCHEMA" in self.sql or "USER_TAB_COLUMNS" in self.sql:
            return self.schema
        else:
            return self.data

    def fetchmany(self, row_count: int):
        end_index = self.start_index + row_count
        res = (
            self.data[self.start_index : end_index]
            if end_index < len(self.data)
            else self.data[self.start_index :]
        )
        self.start_index = end_index
        return res

    def getinfo(self, sql_dbms_name):
        return "sqlserver"


class FakeImpl:
    def free_lob(self):
        return


class FakeOracleLOB(LOB):
    def __init__(self, value) -> None:
        self.value = value
        self._impl = FakeImpl()

    def read(self, offset: int = 1, amount: int = None):
        return self.value


def fake_detect_dbms_pyodbc(conn):
    return DBMS_TYPE.SQL_SERVER_DB


sql_server_all_type_schema = (
    ("Id", "int", 10, 0, "NO"),
    ("SmallIntCol", "smallint", 5, 0, "YES"),
    ("TinyIntCol", "tinyint", 3, 0, "YES"),
    ("BigIntCol", "bigint", 19, 0, "YES"),
    ("DecimalCol", "decimal", 10, 2, "YES"),
    ("FloatCol", "float", 53, None, "YES"),
    ("RealCol", "real", 24, None, "YES"),
    ("MoneyCol", "money", 19, 4, "YES"),
    ("SmallMoneyCol", "smallmoney", 10, 4, "YES"),
    ("CharCol", "char", None, None, "YES"),
    ("VarCharCol", "varchar", None, None, "YES"),
    ("TextCol", "text", None, None, "YES"),
    ("NCharCol", "nchar", None, None, "YES"),
    ("NVarCharCol", "nvarchar", None, None, "YES"),
    ("NTextCol", "ntext", None, None, "YES"),
    ("DateCol", "date", None, None, "YES"),
    ("TimeCol", "time", None, None, "YES"),
    ("DateTimeCol", "datetime", None, None, "YES"),
    ("DateTime2Col", "datetime2", None, None, "YES"),
    ("SmallDateTimeCol", "smalldatetime", None, None, "YES"),
    ("BinaryCol", "binary", None, None, "YES"),
    ("VarBinaryCol", "varbinary", None, None, "YES"),
    ("BitCol", "bit", None, None, "YES"),
    ("UniqueIdentifierCol", "uniqueidentifier", None, None, "YES"),
)

oracledb_all_type_schema = (
    ("ID", "NUMBER", None, None, "N"),
    ("NUMBER_COL", "NUMBER", 10, 2, "Y"),
    ("BINARY_FLOAT_COL", "BINARY_FLOAT", None, None, "Y"),
    ("BINARY_DOUBLE_COL", "BINARY_DOUBLE", None, None, "Y"),
    ("VARCHAR2_COL", "VARCHAR2", None, None, "Y"),
    ("CHAR_COL", "CHAR", None, None, "Y"),
    ("CLOB_COL", "CLOB", None, None, "Y"),
    ("NCHAR_COL", "NCHAR", None, None, "Y"),
    ("NVARCHAR2_COL", "NVARCHAR2", None, None, "Y"),
    ("NCLOB_COL", "NCLOB", None, None, "Y"),
    ("DATE_COL", "DATE", None, None, "Y"),
    ("TIMESTAMP_COL", "TIMESTAMP(6)", None, 6, "Y"),
    ("TIMESTAMP_TZ_COL", "TIMESTAMP(6) WITH TIME ZONE", None, 6, "Y"),
    ("TIMESTAMP_LTZ_COL", "TIMESTAMP(6) WITH LOCAL TIME ZONE", None, 6, "Y"),
    ("BLOB_COL", "BLOB", None, None, "Y"),
    ("RAW_COL", "RAW", None, None, "Y"),
)

oracledb_all_type_data = [
    (
        1,
        123.45,
        123.0,
        12345678900.0,
        "Sample1",
        "Char1     ",
        FakeOracleLOB("Large text data 1"),
        "Hello     ",
        "World",
        FakeOracleLOB("sample text 1"),
        datetime.datetime(2024, 1, 1, 0, 0),
        datetime.datetime(2024, 1, 1, 12, 0),
        "2024-01-01T12:00:00.000000 -08:00",
        "2024-01-01T12:00:00.000000 -08:00",
        None,
        b"Binary1",
    ),
    (
        2,
        234.56,
        234.0,
        234567890000.0,
        "Sample2",
        "Char2     ",
        FakeOracleLOB("Large text data 2"),
        "Goodbye   ",
        "Everyone",
        FakeOracleLOB("sample text 2"),
        datetime.datetime(2024, 1, 2, 0, 0),
        datetime.datetime(2024, 1, 2, 13, 30),
        "2024-01-02T13:30:00.000000 -08:00",
        "2024-01-02T13:30:00.000000 -08:00",
        None,
        b"Binary2",
    ),
    (
        3,
        345.67,
        345.0,
        3456789000000.0,
        "Sample3",
        "Char3     ",
        FakeOracleLOB("Large text data 3"),
        "Morning   ",
        "Sunrise",
        FakeOracleLOB("sample text 3"),
        datetime.datetime(2024, 1, 3, 0, 0),
        datetime.datetime(2024, 1, 3, 8, 15),
        "2024-01-03T08:15:00.000000 -08:00",
        "2024-01-03T08:15:00.000000 -08:00",
        None,
        b"Binary3",
    ),
    (
        4,
        456.78,
        456.0,
        45678900000000.0,
        "Sample4",
        "Char4     ",
        FakeOracleLOB("Large text data 4"),
        "Afternoon ",
        "Clouds",
        FakeOracleLOB("sample text 4"),
        datetime.datetime(2024, 1, 4, 0, 0),
        datetime.datetime(2024, 1, 4, 14, 45),
        "2024-01-04T14:45:00.000000 -08:00",
        "2024-01-04T14:45:00.000000 -08:00",
        None,
        b"Binary4",
    ),
    (
        5,
        567.89,
        567.0,
        567890000000000.0,
        "Sample5",
        "Char5     ",
        FakeOracleLOB("Large text data 5"),
        "Evening   ",
        "Stars",
        FakeOracleLOB("sample text 5"),
        datetime.datetime(2024, 1, 5, 0, 0),
        datetime.datetime(2024, 1, 5, 19, 0),
        "2024-01-05T19:00:00.000000 -08:00",
        "2024-01-05T19:00:00.000000 -08:00",
        None,
        b"Binary5",
    ),
    (
        6,
        678.9,
        678.0,
        6789000000000000.0,
        "Sample6",
        "Char6     ",
        FakeOracleLOB("Large text data 6"),
        "Night     ",
        "Moon",
        FakeOracleLOB("sample text 6"),
        datetime.datetime(2024, 1, 6, 0, 0),
        datetime.datetime(2024, 1, 6, 23, 59),
        "2024-01-06T23:59:00.000000 -08:00",
        "2024-01-06T23:59:00.000000 -08:00",
        None,
        b"Binary6",
    ),
    (
        7,
        789.01,
        789.0,
        7.89e16,
        "Sample7",
        "Char7     ",
        FakeOracleLOB("Large text data 7"),
        "Dawn      ",
        "Mist",
        FakeOracleLOB("sample text 7"),
        datetime.datetime(2024, 1, 7, 0, 0),
        datetime.datetime(2024, 1, 7, 4, 30),
        "2024-01-07T04:30:00.000000 -08:00",
        "2024-01-07T04:30:00.000000 -08:00",
        None,
        b"Binary7",
    ),
    (
        8,
        890.12,
        890.0,
        8.9e17,
        "Sample8",
        "Char8     ",
        FakeOracleLOB("Large text data 8"),
        "Midday    ",
        "Heat",
        FakeOracleLOB("sample text 8"),
        datetime.datetime(2024, 1, 8, 0, 0),
        datetime.datetime(2024, 1, 8, 12, 0),
        "2024-01-08T12:00:00.000000 -08:00",
        "2024-01-08T12:00:00.000000 -08:00",
        None,
        b"Binary8",
    ),
    (
        9,
        901.23,
        901.0,
        9.01e18,
        "Sample9",
        "Char9     ",
        FakeOracleLOB("Large text data 9"),
        "Sunset    ",
        "Horizon",
        FakeOracleLOB("sample text 9"),
        datetime.datetime(2024, 1, 9, 0, 0),
        datetime.datetime(2024, 1, 9, 18, 45),
        "2024-01-09T18:45:00.000000 -08:00",
        "2024-01-09T18:45:00.000000 -08:00",
        None,
        b"Binary9",
    ),
    (
        10,
        1012.34,
        1010.0,
        1.01e19,
        "Sample10",
        "Char10    ",
        FakeOracleLOB("Large text data 10"),
        "Twilight  ",
        "Calm",
        FakeOracleLOB("sample text 10"),
        datetime.datetime(2024, 1, 10, 0, 0),
        datetime.datetime(2024, 1, 10, 21, 15),
        "2024-01-10T21:15:00.000000 -08:00",
        "2024-01-10T21:15:00.000000 -08:00",
        None,
        b"Binary10",
    ),
]

oracledb_all_type_data_result = []
for row in oracledb_all_type_data:
    new_row = []
    for i, item in enumerate(row):
        if i == 6 or i == 9:
            new_row.append(item.read())
        elif i == 1:
            new_row.append(Decimal(str(item)))
        elif i == 12 or i == 13:
            new_row.append(parser.parse(item))
        elif i == 10:
            new_row.append(item.date())
        else:
            new_row.append(item)
    oracledb_all_type_data_result.append(tuple(new_row))
sql_server_all_type_data = [
    (
        1,
        100,
        10,
        100000,
        Decimal("12345.67"),
        1.23,
        0.4560000002384186,
        Decimal("1234.5600"),
        Decimal("12.3400"),
        "FixedStr1 ",
        "VarStr1",
        "Text1",
        "UniFix1   ",
        "UniVar1",
        "UniText1",
        datetime.date(2023, 1, 1),
        datetime.time(12, 0),
        datetime.datetime(2023, 1, 1, 12, 0),
        datetime.datetime(2023, 1, 1, 12, 0, 0, 123000),
        datetime.datetime(2023, 1, 1, 12, 0),
        b"\x01\x02\x03\x04\x05",
        b"\x01\x02\x03\x04",
        True,
        "06D48351-6EA7-4E64-81A2-9921F0EC42A5",
    ),
    (
        2,
        200,
        20,
        200000,
        Decimal("23456.78"),
        2.34,
        1.5670000314712524,
        Decimal("2345.6700"),
        Decimal("23.4500"),
        "FixedStr2 ",
        "VarStr2",
        "Text2",
        "UniFix2   ",
        "UniVar2",
        "UniText2",
        datetime.date(2023, 2, 1),
        datetime.time(13, 0),
        datetime.datetime(2023, 2, 1, 13, 0),
        datetime.datetime(2023, 2, 1, 13, 0, 0, 234000),
        datetime.datetime(2023, 2, 1, 13, 0),
        b"\x02\x03\x04\x05\x06",
        b"\x02\x03\x04\x05",
        False,
        "41B116E8-7D42-420B-A28A-98D53C782C79",
    ),
    (
        3,
        300,
        30,
        300000,
        Decimal("34567.89"),
        3.45,
        2.677999973297119,
        Decimal("3456.7800"),
        Decimal("34.5600"),
        "FixedStr3 ",
        "VarStr3",
        "Text3",
        "UniFix3   ",
        "UniVar3",
        "UniText3",
        datetime.date(2023, 3, 1),
        datetime.time(14, 0),
        datetime.datetime(2023, 3, 1, 14, 0),
        datetime.datetime(2023, 3, 1, 14, 0, 0, 345000),
        datetime.datetime(2023, 3, 1, 14, 0),
        b"\x03\x04\x05\x06\x07",
        b"\x03\x04\x05\x06",
        True,
        "F418999E-15F9-4FB0-9161-3383E0BC1B3E",
    ),
    (
        4,
        400,
        40,
        400000,
        Decimal("45678.90"),
        4.56,
        3.7890000343322754,
        Decimal("4567.8900"),
        Decimal("45.6700"),
        "FixedStr4 ",
        "VarStr4",
        "Text4",
        "UniFix4   ",
        "UniVar4",
        "UniText4",
        datetime.date(2023, 4, 1),
        datetime.time(15, 0),
        datetime.datetime(2023, 4, 1, 15, 0),
        datetime.datetime(2023, 4, 1, 15, 0, 0, 456000),
        datetime.datetime(2023, 4, 1, 15, 0),
        b"\x04\x05\x06\x07\x08",
        b"\x04\x05\x06\x07",
        False,
        "13DF4C45-682A-4C17-81BA-7B00C77E3F9C",
    ),
    (
        5,
        500,
        50,
        500000,
        Decimal("56789.01"),
        5.67,
        4.889999866485596,
        Decimal("5678.9000"),
        Decimal("56.7800"),
        "FixedStr5 ",
        "VarStr5",
        "Text5",
        "UniFix5   ",
        "UniVar5",
        "UniText5",
        datetime.date(2023, 5, 1),
        datetime.time(16, 0),
        datetime.datetime(2023, 5, 1, 16, 0),
        datetime.datetime(2023, 5, 1, 16, 0, 0, 567000),
        datetime.datetime(2023, 5, 1, 16, 0),
        b"\x05\x06\x07\x08\t",
        b"\x05\x06\x07\x08",
        True,
        "16592D8F-D876-4629-B8E5-C9C882A23C9D",
    ),
    (
        5,
        500,
        50,
        500000,
        Decimal("56789.01"),
        5.67,
        4.889999866485596,
        Decimal("5678.9000"),
        Decimal("56.7800"),
        "FixedStr5 ",
        "VarStr5",
        "Text5",
        "UniFix5   ",
        "UniVar5",
        "UniText5",
        datetime.date(2023, 5, 1),
        datetime.time(16, 0),
        datetime.datetime(2023, 5, 1, 16, 0),
        datetime.datetime(2023, 5, 1, 16, 0, 0, 567000),
        datetime.datetime(2023, 5, 1, 16, 0),
        b"\x05\x06\x07\x08\t",
        b"\x05\x06\x07\x08",
        True,
        "16592D8F-D876-4629-B8E5-C9C882A23C9D",
    ),
    (
        6,
        600,
        60,
        600000,
        Decimal("67890.12"),
        6.78,
        5.999999866485596,
        Decimal("6789.0100"),
        Decimal("67.8900"),
        "FixedStr6 ",
        "VarStr6",
        "Text6",
        "UniFix6   ",
        "UniVar6",
        "UniText6",
        datetime.date(2023, 6, 1),
        datetime.time(17, 0),
        datetime.datetime(2023, 6, 1, 17, 0),
        datetime.datetime(2023, 6, 1, 17, 0, 0, 678000),
        datetime.datetime(2023, 6, 1, 17, 0),
        b"\x06\x07\x08\t\n",
        b"\x06\x07\x08\t",
        False,
        "26592D8F-D876-4629-B8E5-C9C882A23C9D",
    ),
    (
        7,
        700,
        70,
        700000,
        Decimal("78901.23"),
        7.89,
        7.099999866485596,
        Decimal("7890.1200"),
        Decimal("78.9000"),
        "FixedStr7 ",
        "VarStr7",
        "Text7",
        "UniFix7   ",
        "UniVar7",
        "UniText7",
        datetime.date(2023, 7, 1),
        datetime.time(18, 0),
        datetime.datetime(2023, 7, 1, 18, 0),
        datetime.datetime(2023, 7, 1, 18, 0, 0, 789000),
        datetime.datetime(2023, 7, 1, 18, 0),
        b"\x07\x08\t\n\x0b",
        b"\x07\x08\t\n",
        True,
        "36592D8F-D876-4629-B8E5-C9C882A23C9D",
    ),
    (
        8,
        800,
        80,
        800000,
        Decimal("89012.34"),
        8.90,
        8.199999866485596,
        Decimal("8901.2300"),
        Decimal("89.0100"),
        "FixedStr8 ",
        "VarStr8",
        "Text8",
        "UniFix8   ",
        "UniVar8",
        "UniText8",
        datetime.date(2023, 8, 1),
        datetime.time(19, 0),
        datetime.datetime(2023, 8, 1, 19, 0),
        datetime.datetime(2023, 8, 1, 19, 0, 0, 890000),
        datetime.datetime(2023, 8, 1, 19, 0),
        b"\x08\t\n\x0b\x0c",
        b"\x08\t\n\x0b",
        False,
        "46592D8F-D876-4629-B8E5-C9C882A23C9D",
    ),
    (
        9,
        900,
        90,
        900000,
        Decimal("90123.45"),
        9.01,
        9.299999866485596,
        Decimal("9012.3400"),
        Decimal("90.1200"),
        "FixedStr9 ",
        "VarStr9",
        "Text9",
        "UniFix9   ",
        "UniVar9",
        "UniText9",
        datetime.date(2023, 9, 1),
        datetime.time(20, 0),
        datetime.datetime(2023, 9, 1, 20, 0),
        datetime.datetime(2023, 9, 1, 20, 0, 0, 901000),
        datetime.datetime(2023, 9, 1, 20, 0),
        b"\t\n\x0b\x0c\r",
        b"\t\n\x0b\x0c",
        True,
        "56592D8F-D876-4629-B8E5-C9C882A23C9D",
    ),
    (
        10,
        1000,
        100,
        1000000,
        Decimal("12345.67"),
        10.12,
        10.399999866485596,
        Decimal("1234.5600"),
        Decimal("12.3400"),
        "FixedStr10",
        "VarStr10",
        "Text10",
        "UniFix10  ",
        "UniVar10",
        "UniText10",
        datetime.date(2023, 10, 1),
        datetime.time(21, 0),
        datetime.datetime(2023, 10, 1, 21, 0),
        datetime.datetime(2023, 10, 1, 21, 0, 0, 123000),
        datetime.datetime(2023, 10, 1, 21, 0),
        b"\n\x0b\x0c\r\x0e",
        b"\n\x0b\x0c\r",
        False,
        "66592D8F-D876-4629-B8E5-C9C882A23C9D",
    ),
]

sql_server_all_type_small_data = sql_server_all_type_data[5:]
oracledb_all_type_small_data = oracledb_all_type_data[5:]
oracledb_all_type_small_data_result = oracledb_all_type_data_result[5:]


def sql_server_create_connection():
    return FakeConnection(
        sql_server_all_type_data, sql_server_all_type_schema, "pyodbc"
    )


def sql_server_create_connection_small_data():
    return FakeConnection(
        sql_server_all_type_small_data, sql_server_all_type_schema, "pyodbc"
    )


def sqlite3_db(db_path):
    conn = create_connection_to_sqlite3_db(db_path)
    cursor = conn.cursor()
    table_name = "PrimitiveTypes"
    columns = [
        "id",
        "int_col",
        "real_col",
        "text_col",
        "blob_col",
        "null_col",
        "ts_col",
        "date_col",
        "time_col",
        "short_col",
        "long_col",
        "double_col",
        "decimal_col",
        "map_col",
        "array_col",
        "var_col",
    ]
    # Create a table with different primitive types
    # sqlite3 only supports 5 types: NULL, INTEGER, REAL, TEXT, BLOB
    cursor.execute(
        f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id INTEGER PRIMARY KEY,   -- Auto-incrementing primary key
        int_col INTEGER,          -- Integer column
        real_col REAL,            -- Floating point column
        text_col TEXT,            -- String column
        blob_col BLOB,            -- Binary data column
        null_col NULL,            -- Explicit NULL type (for testing purposes)
        ts_col TEXT,              -- Timestamp column in TEXT format
        date_col TEXT,            -- Date column in TEXT format
        time_col TEXT,            -- Time column in TEXT format
        short_col INTEGER,        -- Short integer column
        long_col INTEGER,         -- Long integer column
        double_col REAL,          -- Double column
        decimal_col REAL,         -- Decimal column
        map_col TEXT,             -- Map column in TEXT format
        array_col TEXT,           -- Array column in TEXT format
        var_col TEXT              -- Variant column in TEXT format
    )
    """
    )
    test_datetime = datetime.datetime(2021, 1, 2, 12, 34, 56)
    test_date = test_datetime.date()
    test_time = test_datetime.time()
    example_data = [
        (
            1,
            42,
            3.14,
            "Hello, world!",
            b"\x00\x01\x02\x03",
            None,
            test_datetime.isoformat(),
            test_date.isoformat(),
            test_time.isoformat(),
            1,
            2,
            3.0,
            4.0,
            '{"a": 1, "b": 2}',
            "[1, 2, 3]",
            "1",
        ),
        (
            2,
            -10,
            2.718,
            "SQLite",
            b"\x04\x05\x06\x07",
            None,
            test_datetime.isoformat(),
            test_date.isoformat(),
            test_time.isoformat(),
            1,
            2,
            3.0,
            4.0,
            '{"a": 1, "b": 2}',
            "[1, 2, 3]",
            "2",
        ),
        (
            3,
            9999,
            -0.99,
            "Python",
            b"\x08\x09\x0A\x0B",
            None,
            test_datetime.isoformat(),
            test_date.isoformat(),
            test_time.isoformat(),
            1,
            2,
            3.0,
            4.0,
            '{"a": 1, "b": 2}',
            "[1, 2, 3]",
            "3",
        ),
        (
            4,
            0,
            123.456,
            "Data",
            b"\x0C\x0D\x0E\x0F",
            None,
            test_datetime.isoformat(),
            test_date.isoformat(),
            test_time.isoformat(),
            1,
            2,
            3.0,
            4.0,
            '{"a": 1, "b": 2}',
            "[1, 2, 3]",
            "4",
        ),
    ]
    assert_data = [
        (
            1,
            42,
            3.14,
            "Hello, world!",
            b"\x00\x01\x02\x03",
            None,
            test_datetime,
            test_date,
            test_time,
            1,
            2,
            3.0,
            4.0,
            '{\n  "a": 1,\n  "b": 2\n}',
            '[\n  "[1, 2, 3]"\n]',
            '"1"',
        ),
        (
            2,
            -10,
            2.718,
            "SQLite",
            b"\x04\x05\x06\x07",
            None,
            test_datetime,
            test_date,
            test_time,
            1,
            2,
            3.0,
            4.0,
            '{\n  "a": 1,\n  "b": 2\n}',
            '[\n  "[1, 2, 3]"\n]',
            '"2"',
        ),
        (
            3,
            9999,
            -0.99,
            "Python",
            b"\x08\x09\x0A\x0B",
            None,
            test_datetime,
            test_date,
            test_time,
            1,
            2,
            3.0,
            4.0,
            '{\n  "a": 1,\n  "b": 2\n}',
            '[\n  "[1, 2, 3]"\n]',
            '"3"',
        ),
        (
            4,
            0,
            123.456,
            "Data",
            b"\x0C\x0D\x0E\x0F",
            None,
            test_datetime,
            test_date,
            test_time,
            1,
            2,
            3.0,
            4.0,
            '{\n  "a": 1,\n  "b": 2\n}',
            '[\n  "[1, 2, 3]"\n]',
            '"4"',
        ),
    ]
    cursor.executemany(
        f"INSERT INTO {table_name} VALUES ({','.join('?' * 16)})", example_data
    )
    conn.commit()
    conn.close()
    return table_name, columns, example_data, assert_data


def create_connection_to_sqlite3_db(db_path):
    return sqlite3.connect(db_path)


def oracledb_create_connection():
    return FakeConnection(oracledb_all_type_data, oracledb_all_type_schema, "oracledb")


def oracledb_create_connection_small_data():
    return FakeConnection(
        oracledb_all_type_small_data, oracledb_all_type_schema, "oracledb"
    )
