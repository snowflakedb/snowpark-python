import datetime
import sqlite3
import contextlib
from _decimal import Decimal

sql_server_all_type_schema = (
    ("Id", "int", None, 10, 0, "NO"),
    ("SmallIntCol", "smallint", None, 5, 0, "YES"),
    ("TinyIntCol", "tinyint", None, 3, 0, "YES"),
    ("BigIntCol", "bigint", None, 19, 0, "YES"),
    ("DecimalCol", "decimal", None, 10, 2, "YES"),
    ("FloatCol", "float", None, 53, None, "YES"),
    ("RealCol", "real", None, 24, None, "YES"),
    ("MoneyCol", "money", None, 19, 4, "YES"),
    ("SmallMoneyCol", "smallmoney", None, 10, 4, "YES"),
    ("CharCol", "char", 10, None, None, "YES"),
    ("VarCharCol", "varchar", 50, None, None, "YES"),
    ("TextCol", "text", 2147483647, None, None, "YES"),
    ("NCharCol", "nchar", 10, None, None, "YES"),
    ("NVarCharCol", "nvarchar", 50, None, None, "YES"),
    ("NTextCol", "ntext", 1073741823, None, None, "YES"),
    ("DateCol", "date", None, None, None, "YES"),
    ("TimeCol", "time", None, None, None, "YES"),
    ("DateTimeCol", "datetime", None, None, None, "YES"),
    ("DateTime2Col", "datetime2", None, None, None, "YES"),
    ("SmallDateTimeCol", "smalldatetime", None, None, None, "YES"),
    ("BinaryCol", "binary", 5, None, None, "YES"),
    ("VarBinaryCol", "varbinary", 50, None, None, "YES"),
    ("BitCol", "bit", None, None, None, "YES"),
    ("UniqueIdentifierCol", "uniqueidentifier", None, None, None, "YES"),
)

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


# we manually mock these objects because mock object cannot be used in multi-process as they are not pickleable
class FakeConnection:
    def __init__(self, data, schema) -> None:
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
        if "INFORMATION_SCHEMA" in self.sql:
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


def sql_server_create_connection():
    return FakeConnection(sql_server_all_type_data, sql_server_all_type_schema)


def sql_server_create_connection_small_data():
    return FakeConnection(sql_server_all_type_small_data, sql_server_all_type_schema)


@contextlib.contextmanager
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
    ]
    # Create a table with different primitive types
    cursor.execute(
        f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id INTEGER PRIMARY KEY,   -- Auto-incrementing primary key
        int_col INTEGER,          -- Integer column
        real_col REAL,            -- Floating point column
        text_col TEXT,            -- String column
        blob_col BLOB,            -- Binary data column
        null_col NULL             -- Explicit NULL type (for testing purposes)
    )
    """
    )
    example_data = [
        (1, 42, 3.14, "Hello, world!", b"\x00\x01\x02\x03", None),
        (2, -10, 2.718, "SQLite", b"\x04\x05\x06\x07", None),
        (3, 9999, -0.99, "Python", b"\x08\x09\x0A\x0B", None),
        (4, 0, 123.456, "Data", b"\x0C\x0D\x0E\x0F", None),
    ]
    cursor.executemany(
        f"INSERT INTO {table_name} VALUES (?, ?, ?, ?, ?, ?)", example_data
    )
    conn.commit()
    try:
        yield conn, table_name, columns, example_data
    finally:
        conn.close()


def create_connection_to_sqlite3_db(db_path):
    return sqlite3.connect(db_path)
