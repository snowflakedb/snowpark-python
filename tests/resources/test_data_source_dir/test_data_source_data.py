#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
import datetime
import decimal
import sqlite3

from collections import namedtuple
from decimal import Decimal

import pytz

from snowflake.snowpark import Row
from snowflake.snowpark.types import (
    StructType,
    StructField,
    IntegerType,
    FloatType,
    StringType,
    BinaryType,
    LongType,
    VariantType,
    ArrayType,
    MapType,
    DecimalType,
    DoubleType,
    ShortType,
    TimeType,
    DateType,
    TimestampType,
    NullType,
    TimestampTimeZone,
)


# we manually mock these objects because mock object cannot be used in multi-process as they are not pickleable
class FakeConnection:
    def __init__(self, data, schema, connection_type) -> None:
        self.__class__.__module__ = connection_type
        self.sql = ""
        self.start_index = 0
        self.data = data
        self.schema = schema
        self.outputtypehandler = None

    def cursor(self):
        return self

    def close(self):
        pass

    @property
    def description(self):
        return self.schema

    def execute(self, sql: str):
        self.sql = sql
        return self

    def fetchall(self):
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


class FakeConnectionWithException(FakeConnection):
    def execute(self, sql: str):
        self.sql = sql
        if sql.lower().startswith("select *") and "1 = 0" not in sql:
            raise RuntimeError("Fake exception")
        else:
            return self


oracledb_real_schema = StructType(
    [
        StructField("ID", LongType(), nullable=False),
        StructField("NUMBER_COL", DecimalType(10, 2), nullable=True),
        StructField("BINARY_FLOAT_COL", DoubleType(), nullable=True),
        StructField("BINARY_DOUBLE_COL", DoubleType(), nullable=True),
        StructField("VARCHAR2_COL", StringType(16777216), nullable=True),
        StructField("CHAR_COL", StringType(16777216), nullable=True),
        StructField("CLOB_COL", StringType(16777216), nullable=True),
        StructField("NCHAR_COL", StringType(16777216), nullable=True),
        StructField("NVARCHAR2_COL", StringType(16777216), nullable=True),
        StructField("NCLOB_COL", StringType(16777216), nullable=True),
        StructField("DATE_COL", DateType(), nullable=True),
        StructField(
            "TIMESTAMP_COL", TimestampType(TimestampTimeZone.NTZ), nullable=True
        ),
        StructField(
            "TIMESTAMP_TZ_COL", TimestampType(TimestampTimeZone.TZ), nullable=True
        ),
        StructField(
            "TIMESTAMP_LTZ_COL", TimestampType(TimestampTimeZone.LTZ), nullable=True
        ),
        StructField("BLOB_COL", BinaryType(), nullable=True),
        StructField("RAW_COL", BinaryType(), nullable=True),
        StructField("GUID_COL", BinaryType(), nullable=True),
    ]
)


oracledb_real_data = [
    Row(
        ID=1,
        NUMBER_COL=Decimal("2720.24"),
        BINARY_FLOAT_COL=2170.613525390625,
        BINARY_DOUBLE_COL=263.5460877388568,
        VARCHAR2_COL="zKtAGe0Jrv83Um9qDyKHDRISQ2JyeI7EVAoTigJsCE54x1pWCS",
        CHAR_COL="Oz8xoFidXB",
        CLOB_COL="IhiE0xRUBuIQwGcAPsXkmUltJnl3yerJJ8g1EtvvrxwheEhSxxu4Xf9v6f3ByiJ6lisPHT6vQRg0JktRVZfoSzV8SNeWWcVokHmDymwcNmcvIRBOO3Y86jnXSzZ84urx7zlrqFCWhEXeSta9N4zTgxzKWlmPgsYZG56V64QtBUevRv38HlqLPkLBDRmnyytiLQwEPBOBCsHigv4n9nTqtJASM04qrj3aJM0tJdUYcuouNog1G0yHVsb89H0quvSEg19HvinqeiHuXmumCBC1Rx7nYo0VIJXoYdO9MZuXik9f69JKRoC138CzMfeJhtcVOyVV8bt6SUTjxlMvk5iZXuRhlulRTXWVexb4SAsacK6XHHs3OZvfA69hSggBsU4Tlxueq0Ifz0PAXNSRWpfpBsbcpvf3iiFqnlcDFhYOXTGCLzrg5jFyzthD1qcyXuvm836sg2wXQ2JUp4LDWJ6J6GSpfOO8KaRgTO4ct8X9C0I0tjg5fsi4jxqQ9Ce29azZnREWnXP14lUVfXBORurYUsLDkmd15X8HevS2O7YJXx1JCE3Nr46K6mndTx6iNJQl1Tr1qJvPS9ztY4tyFldDqlsiziEmDpxQ51uKaWYJBTdqP8Ij0Rd0CpZKhZhy4eG6B3D2nHmHoh0UrIzdC9gUSQ4IwJd5QA2GdZcXNFm2sYfCysuh8oGcBWtxR41Z22haxC03Mdfq96KRnIbC66AOxuLPSxw3WZx6Vh7CFN866trEHNnpcpgp2qWXf0RoeVDOY3cHvvP1I1ZLQeX1i2RvLZjHWOY8G98C9nbplUo3aEi9txuWQ2zwz1Qz0IEI4DBXDJyKIR8lW3wEz46zC3x9vuw73ivVnmlhzgIoyIMcZpqbCOKqP5feYhagDKhrz79YlrobiRSODCr9WiAlm5LMywwV6gzPrdhlEUr91EXsDAW3Y7Fb0mDCoZlH6oM7t88pHWbTlpqF4HyAkiuUTPpn9lKasrfyJ7j0Dvl32ds4",
        NCHAR_COL="1MKNa0fueY",
        NVARCHAR2_COL="3vQv4U3n0mWuOtAo8Sn3CLjb6DFb66Md1Xi38J5N6zWiVLrkUE",
        NCLOB_COL="ADE2cN3kr2rmqnKZL5WHI9WYh8BmT1A2Z6oIS3go4hG7FewkkIyl8m6uelcYfBxC4lSNW1n4y8zdBQOcBdZAmkkAejVYLbcudKmLulxCmgXZVnzqRKeDB1dteDntM5RvbNunPJ6pg0kwv7zqoAEjnK80e7MWtSBg5qULybgEaTLVZo7GMZXcaaw7Z43RkQ88KbZbRyf6LXo79H9DtBTWZmEZukwFjAzQ2OIz4uZA5LxlBtLuUBFbUhNXbbomPgcy26UNOvYbx2kGVDrZ8vZ60fvLJT4DjwEFy9EHXSI1vEbCv67Ups1rqQU4NU7HrgU1i3tGb19jZcVHJiQLcXeJ2XbeCG1xRsomSFfNnQTC2jQJwIF6LSm4AJ6BVmPfLMr3mwmBncNgclO5LdnaTvob5UaGEp74Uo47ZcNO42fnxcRnHMpNOpPxPL0cnGC6bKzvVjwd4XxZM3xwLi0kaELCLucFWkYi4jR6bvLJKqwhu0eIyQhOwHgFPDmUEDLadotLjVU4NVubIe1qPAZIvfzOs87IbZ9d1TkZP1VvgczsoQEod5AIIgtJTHb7zGoKR0xckSePOxMJ4TVIOIwQAkHwbLpOAOQYIXthRibYzrECqrAQpwyeWTMlqfm7Uc8OgzS4gcWAAfOlE3sPWgtGkg3vxhKbS5IXNBaWzDVh5VjSUVyT9aUyOPGC0uRwnOdVR4SloIMmi0HuEjyWNFohjoarlifE9zQuO5wknopucfbRoYRGX3fDXIiRuf90C1ExLP5j8JJFrRfcbIAIA4YBLaiY4o3P2L81sqXps3Fl0ixvg5UgEDKg3pAyw9jPMXIX2p1BVYBSTZMZjxfL5vVW4IYKoWhHZ7UG6qxTjo3ZG2j7eIRQimQLiQJIFTkVejX1DHza4E3xPoCQWsaOA7iyqRfI7Al3fqQMmcbomjnwgl6uxUftyLqUoEGidm0dd30cHWbX43fOILyE7GGZ2Lvch895aJpsbn0QCnRreZ0TcjSn",
        DATE_COL=datetime.date(2024, 3, 20),
        TIMESTAMP_COL=datetime.datetime(2024, 4, 24, 17, 54),
        TIMESTAMP_TZ_COL=datetime.datetime(
            2024, 5, 6, 14, 40, tzinfo=pytz.FixedOffset(-420)
        ),
        TIMESTAMP_LTZ_COL=pytz.timezone("America/Los_Angeles").localize(
            datetime.datetime(2024, 12, 7, 13, 44)
        ),
        BLOB_COL=bytearray(b"g\xedx\xc2k\rD\xb9\xebc\xde\x8b\x97\xd4O\x8b"),
        RAW_COL=bytearray(b"-\xea\x99S(\x0eA\x8d\xbd\xd3q(\t\x0b\xf7\xc2"),
        GUID_COL=bytearray(b"1\xe6\x92\xc1\x7f\xaf\x1dv\xe0c\x01\x00\x00\x7f\x1c\xb6"),
    ),
    Row(
        ID=2,
        NUMBER_COL=Decimal("1455.24"),
        BINARY_FLOAT_COL=2015.8450927734375,
        BINARY_DOUBLE_COL=4674.780525758695,
        VARCHAR2_COL="SGgW21laivJmQ6BJsJRS1TESwJYqyEZ4zrEEQg7E5ZJCdVy6n2",
        CHAR_COL="UzPBs4PnEY",
        CLOB_COL="txe4nGk3W4Uy9DNGkDv758LROx6F8HBZkISA8LNif4JaogYpk7BTQf6iAGo7aESP4lqkhCypZIgat1pDomdGNAgcwfxoGV0gY4KXQJzMrGqnKzfRcG8PhsLcZ4dpnlLVdwBlSgjTwThftBJOzpHrrSUxNSeRnFdrmr4ryw9KTxBsMFSbJQBBOY9PqRJexoyrzkAs4vtPgFKs0IQWCn2KA4eWwsiqGdddRn5rsO58LSacJ8mOHrcNTMPAFxdNRU0ptQPC6PHioHUU0f5eCfWiJme6iKJshYVZfyRkxdC13GdncxmgWYzoWQMxLuZR7P9LsQaLlQEVx25rwNuZih6NZXJerGFI3h0yal2PnRnCOW1aIBGdX5iNhm8bhHUdQiWyGJFoXeNRrU1A5bT0w9gL9aIdAxELSocyiPh0U9PfC1SZDkVfsQFGt8qA6xfCRkYXwc1f0umuGSYcPyasqKDlmdLwB0XVE1E1OlFsv2bgektFpyXZFRecCKzauoVEIJwH6QY2zac33hTC2ec9xiGDQNPln3VaT3WR7hYEdh5i0zhUTjazBGnOzvyT8OcIZWFmDbQuBAcywa4p3zQyObbsocHXGwBI8hYVHesMCxs14u5jSfk1LjqaJQcE46aJUllpJRDnc9BR49hcs5D2f3AQGkZEvbQ3GXTz0XR7pOQADLTdfJPA8XYCKjoAvv3CCfuKKRJjbZ7lZodNNSqiiAJTQgreVuuH7fVf19CLndZUVa0wrfXWUTFHxuFTFTnAMpBbvtIcxli3HPnmgQdcqr1EJNErG0G7iygDD9Tc5U6Ox6hrDlrT3WSe7iVLz21tObEURdmO5RrS2Oemb4I9tgvOxBNuS1OJTOeUFcOe8T1dAiC9MKx8S82dCQUBI33yzO8XofcsApVoOCgVaHC8y54tfbUwKOfW8WEkXSXujqfBjHOLashHPskcgyC5vX4PQuNgF6vGWNY52rGbOuatcayVqa114vKvWdL1P04B2yQN",
        NCHAR_COL="AykSIFb31E",
        NVARCHAR2_COL="3UAYQK9sKoQ0h6rr5ymQDawI1sSZRZFIURlrJn8y3DIHHfYN0a",
        NCLOB_COL="Jqasrwt3Pj1hTeE8BikmTHRamPpqwvT7LF5N375ENjnfuUGARQzy4TpYvTLzFDICQ4q2CvnNGWV5xOL1W5k3wqYs7L1xioEBxeGOgxd7iBy16XuKHdnjCaxSqf4KrdHODPvNT6XnSHUFpuq0ZPA0CR3Rcjm4hRi4p9jpmsxpkXMLsQeO9LsQzbsbK3MaUD4VZHRwbKzj3NoZR4dArdq0IkT3eEsWFDl2grOF6kwyLrybHasfk05XXqYiSEWwlk6cIWkf5IYyqmnNYG5SL5rWmDomn2A61HROZ4wxLh8nsMZdj1QdmBSbDgCZrsFctonf3RCztYI94j2EEppwDVW9WSzuJmrNDrykg1QjzkDRxE64wfBsA5KhUWSiAD8bgiDMcWCrS5vovFkE8vbLoVFeNCDSoNNoygwDE7mbJ4xolMpBsDcpjE6yWFNs6W4hdxTgMzvvOa2ffV0KoQQv08VGp5DRxVcuWh9LhjCCoh1CLUPUm9Y8xFMvPeDN0RInRYgEF7Q7udD7WDtbrZyPMAr90Fye8mylTzMj7yQTAKMKZxJPMniQZ5PjsG8oAJqGD9zhgvFMEd2Jqftvz28COPnFvxtGBMfeM4CctyHbqSaGGEln1goKzMxsQlqHVTgrgfEtAjjKMtzbojf1M3TA1uLBoJM0N32rCeBCS5hqS6TkZ62ECKP0otQY3uClYzjXI7f1e2hpHW1nKvTRFI7gNpCACoMcH2HRVMqGFBiDDrxjQi2yF9LI6kbvyHXKOFtghcw7tRxQ9cQgFZDyKoFWgyqnJOszmoXssVfPS1nUHTTePVqRSHqkAsSilCOAJDlBAGwvVsyyRjy3JjL8wCTGErPI2UFXJcd2AMWv5oqRqmDVNcYLKDiF2nHapkM1XBIqQeSYtGoUmsH7SCrhQJKIBBFyzqAtd3B1dPpbtrpVvVQQkybwi3st3tjNu9ciHZhM2CpR2r9mtR70rRP5boTJvnwq8Oqg7lvIGeoe1GwzU5ew",
        DATE_COL=datetime.date(2024, 9, 27),
        TIMESTAMP_COL=datetime.datetime(2024, 3, 27, 2, 9),
        TIMESTAMP_TZ_COL=datetime.datetime(
            2024, 4, 8, 8, 17, tzinfo=pytz.FixedOffset(-420)
        ),
        TIMESTAMP_LTZ_COL=pytz.timezone("America/Los_Angeles").localize(
            datetime.datetime(2024, 5, 1, 21, 7)
        ),
        BLOB_COL=bytearray(
            b"\xbd\x89\x06\xd6\x8b\xbdC\x1a\xee\x81\xfd\x89\x1f\x86\x05\x19"
        ),
        RAW_COL=bytearray(b"G&\xd2g+\xebC\xda\x9f\xe7F\x187j\x0e3"),
        GUID_COL=bytearray(b"1\xe6\x92\xc1\x7f\xb0\x1dv\xe0c\x01\x00\x00\x7f\x1c\xb6"),
    ),
    Row(
        ID=3,
        NUMBER_COL=Decimal("3496.89"),
        BINARY_FLOAT_COL=9770.486328125,
        BINARY_DOUBLE_COL=3217.986565049973,
        VARCHAR2_COL="Rh3AMxcMzU4qvCxze4t0vCD9ju6tYGd5filK4TvTcBlf7fDiwT",
        CHAR_COL="7dxd33fMlF",
        CLOB_COL="jNJ8lT9Z8R7Ex3sBxEVCegIgLHiax80xSQ26e5ahAU8s0QGJZw3OVPTYIZ84PZB1XJ7AESU5fFFdqAjnRfZMRT4o1hEtMBuNcXrxED1lrJVaS6G6mhmdBHcXerZzEvNUMLSu3vJ3gxUVxPt9XLyhy0GQwViUHYR21rQkkeSJ39CSS2VlJPaSAcWV6UTqudpvOIP5UKlWysEFHMBqcetzndV10ULAvsLrKuwu0ySnjbnnbHmKIlXthecOeynFJtiR3zUPgYhCY4pJvK7OTKTvAHON2InpF20maE0oYvRJXSoQZChNqr08Szbk4ZvpshJuxb965PAuXkiB2QNYg3EbsKMbccSHwtQZLqPY3gOwxNwvopAU9KVY2DRFAtr5TD2Lb99sT51iUXGAGbmFuB2KySGPSEKDgYE58Y2semeSn1yPwjCX0WBxhzcKZudHTG1SoxbxhQKDD2OdeZtRys596nz9ACeukv0Bydd3F5G7oEIvVJNa3HrNIWQ2WLpeCVlyqQ7XH11pWLBMY3SX29NUavszr2OgOeylVGPE3fq8OLP15YtC7rRkfN3PL7GyD4Ddr8vtjJVvM9uQ81lxcZ77CRbv9IGpbfpaA6JxkwqtAI5vAL4s7skhLFBj65lmypWgBosQJNfx51OGCZrfDF87AA9GG7wmKdSOOWeoaTxLcVVQXXpc7SqShmsu4riZOb7rD0hisqZjQQNpg1ub5vkwm6uRYbKdrMmdlMrJDz5hRfXCRuN7q5oYKWXRcj8QkXEfZKDC09OtPTWYvrLRpPhlW2kuJMJcsBm8fi3ploWFI4jQd2129vN3W6KU0u1RMYuGQa6ygSbd1sleq0CYbJdRKlmL26vjg6eoreuWkhYI26zqjU6AxUO5olsB2UAUdBULzlicIJEmhCrIPlXHC1dKz3Udz5zS9EDNhTtRDfR7T7x0Yf2gc3HItEtZumNaNXEYwjSL3r11qDWfI3YFmgQETuuyfT67U1nhBYqPzZN7",
        NCHAR_COL="HdEFwfVmfC",
        NVARCHAR2_COL="uzLFvFMoetVUvgHZ9EzUlWeI8Dpblupg5G6PLITxdH6tgfLl8V",
        NCLOB_COL="1026LQraJdt7iT1wRdukQSiP3ZNnW9wkehNVE9klukNZFmRSu872KxUP5CYNPlJ7RoSq62fc3i6e5AivJ5cCnnTo2DryzUxuMR7VoZDdudvLtusVDn2oWjVbzibL5ttTqu5j3GlGVXYSugouTXqcOUoyaAtiou8wS8JvHF1rM5VcnbrtAdw3KSOkFPJcQwvFndGQhahYgvBJfBfJcWLxFqYEoNDQa4m7GzHVVcqTn7sKGrjG0V6lnqH3KU1ZK5h07LTWQr23ov7IGAeFSJtzP3l4KujwqJj37dMlIWMHbLjdD2kZHK6haXkDgB3lYMs9ZdqDTfwLOOVhDnYltGkbkR1N7ZNJYRdWZecmSlOkg4AEwHfzh2JI7ELAZhgS95D25UO3sB1nkgXqY6rXrMdubnpkws8u98O2tWCqsYSgJIoqdr3ZHSzvuSWr5aewbN0jG5YawBISHBRNE3xpswdJcxuJENVzmU6XJx9X5jpxHZik0bXENGU49izFiSFNNe9EUm93N61gdrqVK9ssttIx2EwZCBSgC9LhpByRR84cDewQXsOVa0xgxvqRjwg0lEPTYJYPdi0yb6GtahnN65r0eIulD59wUu6T9ymFrUgMf27FOEJkbobJRRM57m696mTVN0G9hzTpxYpFHVJjxh6iOYLlM0EqPluh5JnPp4LILxfEhz9K5ohokjrgEODfzJRz8oDTr2JhtouuwJcfKFlVALr29hCsR7uKKiTfv7JtAN8rXB0pWs2PsGD6a51pfw1ca1z3rKOq8l4K4eD3MMCUwalN9vpUKFv1d7HzxDr5J4Bj0COV9cS4QCOj7Dhb12xeZiEgg4mxddHUAWHHe0VvvmFFka5FbJFATmzqwLzLJU7rBTbne8rW260kNI5wlVqZOlC7JTiIHC44FHWYwQigoeMuySuQjrFJk4d9jy8Ci4DQyvRB9T4oF4TmDOt3woNvFAjoi1aKLHEbfMDdfnQsYNqucKxmPNOk15ag1hJq",
        DATE_COL=datetime.date(2024, 6, 16),
        TIMESTAMP_COL=datetime.datetime(2024, 11, 10, 0, 23),
        TIMESTAMP_TZ_COL=datetime.datetime(
            2024, 1, 24, 1, 37, tzinfo=pytz.FixedOffset(-420)
        ),
        TIMESTAMP_LTZ_COL=pytz.timezone("America/Los_Angeles").localize(
            datetime.datetime(2024, 9, 11, 11, 50)
        ),
        BLOB_COL=bytearray(b"\xe7\x86-\x83pb8l\x8dw\x15\xc3\xbe\xd6hT"),
        RAW_COL=bytearray(b"\x026\xb1\x7f\x7f\x91O\xda\xbd\xe6\xa0a\xc2\xcct8"),
        GUID_COL=bytearray(b"1\xe6\x92\xc1\x7f\xb1\x1dv\xe0c\x01\x00\x00\x7f\x1c\xb6"),
    ),
    Row(
        ID=4,
        NUMBER_COL=Decimal("7940.18"),
        BINARY_FLOAT_COL=4795.6435546875,
        BINARY_DOUBLE_COL=4710.60248895784,
        VARCHAR2_COL="VCstfBccaCbj1q926zWcCrkBZmTGuDYoXNvc7t1kNcRMLOAbwf",
        CHAR_COL="cvpihb0684",
        CLOB_COL="80WrcXVRIJEx8oogELoZj3QX1TntofPDHTkNTK5C7wtm28VNUdIYd2qrSOnFrlwUKPqoDdnvaQ8f5i9uBxY0ZdydYlaE8wWPpkIGFs0mYC4lEOYt6XXkcovD28BlAPLPkbxF3lfZIz7KFqpqK1EFJnBRUEaJZVonaAAP1WWJu5WhZVM0SRDk5IkumPmESfZyT0anwtA564jKwzHznsQghaSWZruIXfu1E9ZJddSqYolIhyTIpbA99pzs2z7h4awGbt17G6TqwotzL9uXE3AKk6QcEsFJhegAROFYIMCjFA0cGyIYzvo3qEmcZtvAk6FNjmcYaojoxAmCSMN0oAt8hVzRjXJZxYCmpRQa56mYxnCN1ls1UHyppVqE2zKmg4CkKptoMqOkejqQ88SU2DwwBOGWXas4AbsVcOcO7Yx5YPUYovxMPup7iI1R8K20XELUKpOv48pdaRUwLdVvfA6fqTV8nbfVVCbGQe8lRbAep4aDSPm4gxZznoKHx8ZGMFBlh3GPYvFJN8ji1IeZZeS7MVZRFpm9MGlvafJ8gSWhHuDN2Z8r3uiV3AhxPJ6SoInsEelSW7LmHOZWzisdDGuBStPY5IyBjAkMxorpT5RClzfoh6bWQnZkQqTt07saBS5B7PlyckrmdfOzVXNX35o8uxc2fLTxVCxVffiAdFARAwtW5ZQy9jWI5bd2IxxUHpLEsqIBlncFldrhwEoG4Rv5k0NIGFTbuX6vW9CgDUgWpM1aYOICGNCw2nFmsaf0ViWbUSws5LjmIMgspCjpeDm3gGSL1BPHtQbq4od543lAXPKzGHosxqfpySateVIxi9HtGWEi852KQHoZipqZ8rMHcSgUGdmKmIJ9HjJdSFx0nLNLQbjYkD28SfasBIWcA7rDiXCxjORv0UBpEJjnTsfD3669gvKge6iUJIOP0eoJCK07Q1AZeOAc35FF8JalVT7SnUczjhzOAvgamr4ejb0IdzEmSeSOXru3mnEQnOZJ",
        NCHAR_COL="4djYgM922D",
        NVARCHAR2_COL="J8OypMyAQrs4Ipd80cNvh5VwF9yACJIHt7uLfRuzWR9NuQne0K",
        NCLOB_COL="C3dswh1Ooac6Vrl6aAjj5hPloku5ARwfcNejtxm85T5RmZaGxopjb8uReMdTU1PzDynMiiElURCYneumv1c2vQsyCy2BiSYDNvroxzHTIJKnQtb99lezBNKorcrs7NXIuUUM29UuMLxVYYwxEm1j7O2INP3Xp3hRUMGTUEigDR3RK5LiH6fbX1w0hQcM6PxQihviOaMjP8nXY0VO0qkKmPeSchcgK23H2ZqFUjzai88HPwaj3OTsgl14LLZYYfq1FnI1fwSvCt7Do9ad1vAi80rT2bvZ8o2QekUJjyhrr1NAAHAwTXZhiO8eM8DHZZkVpTnE0gVBYgt5IIUJ2T2lQqqKeg4DhOojFeRzq09wNjgTmkxDADSWlEjpmBjAkhZrQP7PRuCBEBOT2r07GgJ3kgAIYveyrGdqqjs1fdF23jl6u7iovGsM05MR7L6GlVm1sSMNkOeOWeXOWPMqYtbXq2zVfmHU8VVfRd8YZmhpHHDIntjm0BK5UqSb1Fk68IdZj6812m7djfSMhHcspUWKjELLPfejqBz9rS0t1F9NrpOfYUQnQYRwYbjASKrqSiAo4781tpCw3V9UC0kphe5nwEgjy2xk0QW26nIA4RlY401ksBBoj238m1d01kjI2zKoCTXLoKwwdagd4A3vr0xhrVjbzPvBuSaxXe0gIE5xmWWA9Tpb6v7bLp9LfofETGhNHJZqnJbCISCOe4gNXnarqpvGpYtS4P0ByhHeApJzxQAyS3hzvEg2V6B9xJGiPWYyLy3Z8HjZ5mlvfinn0cVVv0d47UCiprkfCQZpNdqAZ6lYRzOSD3ogXFxviJFOgca4mYWhyNLUPJoLG97Ew3PxgQWmzvjRFO1m4AmiAraqun2C1V3kYRLZ8t26YGQnzsKKLyZt3AGCEHH28RYpLzrri3FNh89zg1r2GgEp9PbMO6AkrHP7LfoeTkPJ7Wr2sgMZxHMVZ7sxZBFiZtAi2jT9GMYfge092vbboG8ZfPzA",
        DATE_COL=datetime.date(2024, 9, 5),
        TIMESTAMP_COL=datetime.datetime(2024, 2, 1, 23, 47),
        TIMESTAMP_TZ_COL=datetime.datetime(
            2024, 7, 9, 3, 7, tzinfo=pytz.FixedOffset(-420)
        ),
        TIMESTAMP_LTZ_COL=pytz.timezone("America/Los_Angeles").localize(
            datetime.datetime(2024, 2, 27, 0, 8)
        ),
        BLOB_COL=bytearray(b"X\xeb\xa9R\xedTW\xd3\xe3\x01\x00\xd9\x93b\x82\x94"),
        RAW_COL=bytearray(b"C|rhfRO\x12\x95y*9\xcf`\xf4x"),
        GUID_COL=bytearray(b"1\xe6\x92\xc1\x7f\xb2\x1dv\xe0c\x01\x00\x00\x7f\x1c\xb6"),
    ),
    Row(
        ID=5,
        NUMBER_COL=Decimal("3987.78"),
        BINARY_FLOAT_COL=6009.37451171875,
        BINARY_DOUBLE_COL=7887.4022739645225,
        VARCHAR2_COL="pVUnA3QDm6rJL28X0SjslkRl31MAyoL7NpnJKeZTOZGflq8TSZ",
        CHAR_COL="iMNZXXDnww",
        CLOB_COL="KUu9mH1yWS9w0lMT9ISU9H1ArgdN0A9GKxb2Nn4Tc5NJWhuzrSWslfjYALda2wm6LvtdhuIOZXzKadKMA1Z6nILXAmvvXmqX057h1THCzdiPoN8on1880f5f0EilZrqcm3XjOuD8qLBRpCYmfVcBBLBtvRdSQU1ujxeEJMGiNxXv8OQwLJfeKGNJG5hq9UxhdhA5kF52fMSPTWzogqWxPn72Z5exIGoMxy9v2qdo8dXfSkkcgTCMEweOhrLgmGmhwFRyj3o7w7nLGRxOZ7lnFNOjN9WIzuLMBzXbPDrw4uApW8JghDZbOPXJ5OSumNglGo4cbSwWax3tefCw9Q47gfeOyfHtJnuohwErIZU5LWZMpA2Aw3dPHdfNqHDsW06927g5Zclfch2QdwwLCaZaqwJcJw3iKjXPgEwbzRtvTfFXUULgKQijQINFLxVj4ipBGPZ2YEfXQVnGLTRkOqFkR53TH02bJJYBQQVvBczdHPNKGtHBNE2EFhSjsFJn4shils0MiIebi8OLMnfhwzK3AmLG8WA84RDN4w324yRUcjzA0StOMIgPCjoTcNnBvRvkqmvmcA1UFWnT1v7x4DIKm51Dmo3Lo2dwXAANgxs48ElmHL7OEQjPuxRLfuzjQEZeRDPYTuJ6oBBTCoIldepwC0qt9MfYAYDO2YtuMr2cwSWwmBmv9t7P3LP1THt2AeS5aWsORQ0K5GvHKXbghSs8vGeDQyzGYGKL0r8DLMZuyT0q9VmZqrYK0Ya3jqTpTkLTtPWRPeUQCkSG9MqBoCH3mIhTl666Y1BXZ9vrWgetrFezJkPSAMUuISsjbAfksQaGAo7ggKOHfxfDBv1SVc1nybJL9AhwsVnZa4GQ3khUL2UO7rqwAJO1BbhD0jP3PjQtSNVougDPBGGD6fpq9GjKwnSzM5sD0jJC7nVEXvMfl1KN9ofwOJEs70ZY7sofSBWkpsAo3xXnbdBTiJVkBT3GiXaeYjm7E9dcJ8ffKTIV",
        NCHAR_COL="vWzW6HxHe1",
        NVARCHAR2_COL="0zaItRZG8ic0JECjciZUpZIVZr9NsPOzBCRIfHkcMv3zFL20kd",
        NCLOB_COL="g99GhtFI2XIqEscpdL0GUpjSp5FCUHav7TO6ZDt7Z2FiksMyYJg5yxM05TlaK04nriOBgVhTKdXckWLSXA4cVtxAaA4Nrj8MaQidqnArq8QYhAuuTTm9h1MyytEQXmXKTIFSyjPSYdZ1NI4zBxzt9WaFfJdmc57UHEKnuGN3Qwbc0nHvrlUFB7H2KWEtloFzB0AGIXWhklJ1kud8NkzH6vtjD6o5tlui0TFKlcL3SiFMFPzyTlsYgMCYwTEjjL7WA5U97Eb4wn4T3r56QS3oT4PXpzz1iHDS0xgK2nJOGurvJDhFZYJM8ekzyJqHqGONc1AdijxM89pMNMqcwiA9XIoiwYATRE7YtsH71B0KfDlmRX4QuTu19WREkOt5LaljiLo6uOA68H2AKxtATwIXO4bgoBK1TvduFrLfFL7WViGZh6tmk5gGrq0YA1UseAp9SuD4hIA3TTUL2jANvSe65Q5iedMz7Hsfl0Ndofaw6l5Hej3o7mZNL1dNBc4KLZgRciN4m7OlYWw1N3TWbO7j9WAspZlGrFPPkVf8sT3qJWhigQKh0v7b1B4sqhRDIFgRFN7f891ddDqMe5vq49ziehm3FoZyqMC8qfZ3IAil05VdbQHgUG1c2WwI2Fz6LM76r0ZnKG87bgnTt4lMYVf2mNNRcgDYhi0yxz2YO4PaWzq8dEy5CRHKoU52xnqEMEiBJYesyPwnG5ajl4TdVbjf88SgLHqcCmst42txzF5CKOlSaUpEQB5n54pKVcce45Uckk7gGsMgTUZr6Qornywfkfo1JWCkWbLV85g3xZLGAwB8T1dU0byrIxd47GsVj0viWTh36THV8IpIZ1L2ByedtmRmmwz1L1w1A3IDgUlQuakkdZyvQuv5qVcVM0yJ7zePslTLB6qaX4apI7oPbqO3OBIDJQV83WP4vncsGwXGGQ2z9Wza3egBsJMMX5CI03aAAkTzo3crWQE7X1hGJGWnUQM2bbcHZ1tLPCmmjV02",
        DATE_COL=datetime.date(2024, 11, 28),
        TIMESTAMP_COL=datetime.datetime(2024, 2, 22, 21, 8),
        TIMESTAMP_TZ_COL=datetime.datetime(
            2024, 10, 19, 22, 31, tzinfo=pytz.FixedOffset(-420)
        ),
        TIMESTAMP_LTZ_COL=pytz.timezone("America/Los_Angeles").localize(
            datetime.datetime(2024, 3, 19, 16, 39)
        ),
        BLOB_COL=bytearray(
            b"\x0c\xb1[\x8b\x1e\xd2\xe5\xd1\xf2\x8f\x9b\x99\x16\x05\xa2\xba"
        ),
        RAW_COL=bytearray(b"}6BR\xfa\x83M\xb0\x8f;\xeb\xd3\xddu\xab@"),
        GUID_COL=bytearray(b"1\xe6\x92\xc1\x7f\xb3\x1dv\xe0c\x01\x00\x00\x7f\x1c\xb6"),
    ),
    Row(
        ID=6,
        NUMBER_COL=Decimal("4965.05"),
        BINARY_FLOAT_COL=1111.767333984375,
        BINARY_DOUBLE_COL=4114.005053147933,
        VARCHAR2_COL="bFo2eGCOQ2FZQlDfnyDewTXYi4qJcIFP2uXhuOB9rSCSmkoCRk",
        CHAR_COL="sNKO50cu5F",
        CLOB_COL="2QLDM1wbLd02Bx1y30zgwLHrVsXMgHBab2LYdoREn8AfpvTi0l0Zq1gq0P8C4tc9yjM0JG1VZD3ZyLQxvt1vx2n9WPcQwdRac87sTxdLjrRgHeMNfNQUQdoerXrph313CWHgfdRBAFmUBlO4sTkUukIvrrmy4jxSouLK6zQX7n7Hh2Hrr3OjCDdjHLQeRdFV1Olq5k2EXApnXtrtbfWcobxggsB6iDVIwzMb9UOVoq8WywRWTzYXWTxcKCfvtsJHfzHFJK1DiiwIfV7sFG9LtXQJCpLODwrWk4fqZJDPrCAnM5l2Rgk0nlcCb6KnYYE4Rk4UAYLYJtHvprjyCaSD3RbamzrqnAjJCQZ2lHoVaBDcouHVpiZRY9lpBCf1lbNoZYDHNHLaXSgJp5vygWKCOli9jKKAoqkBpU28VSEzVdwvPj8qsLayqg2xspGeTeX6jlQ5A7EDNDNTvreiGlUhHaGSpFUrpAVZmjyBGYjuhjNHzegbjKoVhiL8obOpr7Y5EIWNVnfxGS2AwmJTel9Gk7Gz47t0sdHaqLRQl2yql6Gn96V0yQyjbAxr15SLyk8ASBiQMC7gA0Z9R9yAvAgPnumLfBLtzbYYunpDWgE7zx5xbLaLANebEtGL5TrgDbEllRneuwdM5wjLbn60OtfahKzWwIMLgx9z53YUzNwaIv3DyvChHWFVWnhDBfzgsqyIS6bayLEsmcS6l2D800sgZvXBLjHHMYoj6YZOid9xpM25J3UTZ9rz6ytCYyrw8exBVkT4qOH5WGt4IfvNULprxHuVmEbJ6J1TWuDXFtL9KZdTf3Gvik6S1uXenqSm9ltuMXd66X5oDDvnOg1guQpYucym0bD6pycoJRkN7oAXmw2n8jCfgj8uvkQFX3baEVaPAPKqAQZmexwomUAkm2jdM8ABqCaSbhuDhAFnlsGl1v90kp09VCg2BKPesvidBW3vAWCojS7btOiZkbk2yC4vrEoCKQqoK2TOiR01ysWj",
        NCHAR_COL="1wzoCrNbLS",
        NVARCHAR2_COL="MSX4ucrUYhHW55F9KvfExdWCe4cipsGBWzvHN1IF0UcnXqsFvw",
        NCLOB_COL="4JcREGhRpCgwcqBJclmCeVKuyqwxnF7spmeh9XiUOuIDGNoTWErqXCW6fNu6lWaS9bhrPVofbyVNAUyOQqE7o7CV6k243Y10FImv0xkpq58tgifCGM8lz4R5Upk1T8jJt17XzWiZH4QWh6hN5VTpKF6sj4S8uTnbuTgDvcc062jFLpdJkYKXacpaxOeUsi77swheD9Nob0uQVUgSEeTrf4QTUKOAwAlRcmQEN3dUrdlXYFZcbAz31UmSEisdRWExQItAkkUPloCOUdLl91AZEHEYbyCVnBYIXZB3CvTN4WonS8rLkEnrlYWheZxoXF639me3zfnW5nJZWc2MHYx1PypbzxomuhhvLPLcnOiHnSmPMYr5fNLAspXCCIcNsYmoSSQmVgAdn5myuoSFJpcrN4QOVGWvrpj6xxzpjfyhS353u0O1AfRb2Hej0npiquYEXSTb8vawefXsypD5D0L3dJSzKfKowoAppyQVwZ8mb1SKKPhjAwDkCANEKtW3oxT5srMPiVHCGhV9jySigPF92AbCVtWSUUk39XxmRnJzoB8bm0ckGbQjJaRkEUriO1Pwk4qwm4nGrxmRiX7f5ZgdtL5PAmNTgKXxJZtFn57H8IRLa8advDPlOrVeNoiFX1JiGtCvxJihzOPqgt825nZLnQzhefUq8VKrmu93B3TTjKjl4juee6ql4kjayrDagDsmldUMfkP0CRP06Rd8RiQa3ZSJWCGc5NPOAzTiRxcaN1TCuwo112TqXgyULwzGb1yyccFH4nCsuWRCJZ8eLfvHhjwbx1WaVwtvuXlbV2j8ojdd7skUtQfe2WECi8uMCSuarknG6N9psbDEcsjLZUrWVYRuauOdE3uyQHCBu4vvbvfMAjwWCbAt7R9FojgJ0XjbLrU1GZQVQDZsuNbkvovJYuZRjmYcQKwvv6vFGa4novNo8GXGriHBIQqfGaUB3ObVjbqqWC0IELjdwJR5mxyY0NVusuZaQ8A3WRRqxtcb",
        DATE_COL=datetime.date(2024, 3, 20),
        TIMESTAMP_COL=datetime.datetime(2024, 3, 7, 22, 43),
        TIMESTAMP_TZ_COL=datetime.datetime(
            2024, 4, 11, 15, 32, tzinfo=pytz.FixedOffset(-420)
        ),
        TIMESTAMP_LTZ_COL=pytz.timezone("America/Los_Angeles").localize(
            datetime.datetime(2024, 4, 4, 15, 34)
        ),
        BLOB_COL=bytearray(b"\t\xfcF\x03\xec\xfdx\x8fH\xe3\x1b\xa3\xdc\n\xbc8"),
        RAW_COL=bytearray(b"\xa5/\x99??\xf8Gu\x9cPf s\xf32;"),
        GUID_COL=bytearray(b"1\xe6\x92\xc1\x7f\xb4\x1dv\xe0c\x01\x00\x00\x7f\x1c\xb6"),
    ),
    Row(
        ID=7,
        NUMBER_COL=Decimal("8291.57"),
        BINARY_FLOAT_COL=4817.7333984375,
        BINARY_DOUBLE_COL=7539.126257650768,
        VARCHAR2_COL="sTyuhAAzbsbvLrNqufe5Aro8aloGWj7uNKOtBXPdNLtPp61ibZ",
        CHAR_COL="wzzxKuVEnu",
        CLOB_COL="EurQTZWpEO56YVnAO7VZ9rQcDl3DwpQhwdtVHfvia6pGqIEWpj0NMQpOOfKlXsoYNDl7Wr6kKIVXHTyaJDpeHPQFHKXUOrATpUSY8H6eDlREZr0TVCsukYT2OPERTEndaf8xCifTHMqaLGNsXzpCgx9Z2XEmroFUwkqDcpyptgAN4FjDv8Q4rGTPYEwrATAUCVbr88Xq0yyAKfTdnr8A5aJnUB1ZVjsvB2yN1YNZyAaq8PKtqcoR6SyzumTqXqpzcYonkltXNOMakMGd6pZerCGtPAJgqH3AEnpGo4Vs0jZcdnF5OnBdaZS9H1FLhu091luZgxem2EcdtZPWBkYwQsqNv85frZUiY1Xj5BiC0Vg4vzSsRRqYCIAhRsxLZUnfH8tX81iwSrYvoADNofqRFk4N7fbAblVb94wxtt3989SsfRX0pIMWF5zA10xaOQd0OQPS2b1meTLjVhJs8ppeGnBsr2yBbq3LFBq8Y3clO2gRawIVDl3SvApGiTkgG9jRUeNSVusdKViZjqaqI6X52RaQPfyMjhZPtqvYrgB61Vie7y3ekCDIn0SyYI6DgXfoMPDMu1t5NvvNite6i3cOFQMskjKGsr1nnhN4AaZrD3z3kO4nfiqiPYM1bIh9QWAxmO8kALlVFN9O6Q89L9OtdR7fAitI2joJTWSGQ4YpHe90simSU75JlJKKWPSKhmNhAy2jf9v2YOqfxBXQ3x5WK795iOHOspdZBhMVXUIqtHX8UjsBk40NpIr115E3XaGDZxxcFahnucltoukxIC0CyXz35v9XnItLKV97GfeUfeDdY1jSU2ymFvP4fcDbJnj72NcKiACp17sXJQFylshlxcVj0R26xSAKvMeNofFrPHrHuA47UV6VaVxMxOLmDrtORHXEUYX0aV3sXrdNJMSpjaGWKZ28vNkfbEAd5r0N2o09JxOW5NtrUhDlqOSfmg1om2YBKKduIXd3tL1y2TBJ9BZCa5ilcmu9vac6osBt",
        NCHAR_COL="p5SlXsYgOz",
        NVARCHAR2_COL="709jQyldoI0EMDwbubLRRaIAmcBjsXK70ThjJ7HWbwp2V1gdcP",
        NCLOB_COL="NUgcAsWsEoae882kEalk4d9Vh4Z3cqnIn3mRbR7kCKDflrERdnS3YAzfVNUIxPB5RplcAr0vrSl0Je17oFfL1yaCaEry3nTI7RU3fUWLGXQLj2rluonMa2ICDb5pztUiehfJdq7JUTwzfXAPKCyMOLbRhKJSIV8ZftQBJ0Cp9zkE10ZRosewlASMdXEARsXaQGBgz2qYX3gSr2aH5SD0mjedynTIawUiUzPikZYcI8GCu3PMTSCGmFyUeLnWROnOSwmiV64FMEnZzYClHEBaWtPkVxe6zSUnwjqQCftM4lSpOkUfUnjF8wvx8kGMbalAqbDtX8apancLIjQj7IPO6h3ts8WVWYVvwuqoEsy5rD6atBcXqiCfJhyzEg6LgoOwTpbFFS54zrV4CGQ98AFEGbM4VmWfEfQVKDdDR3TiuEyiNdNfYtetrqsDJ0pa089FB5Gee8hQlWElh2JZgqAfsoA7nsfLkrZrtMXrm6vtgpS70v76lHqlUg9SDY6v0Hg6OuHDFKXSnsaqmWbjkfaTqjEIf8mWlHenlfDdubcg9NLdyEs1cbOb5bB2TO0GubA7RF2Ewcq3AH0s799JH6IfaCVbTxXElwepuNip9c4aV5bb5PYkG7Ae6lQHFR3r1T3ZjQ295FWKvTiB8rDrpriDrNyc78rx8ifVZXrFGv0caIggOR7FUwC5WhQwEl5IMvRpSWtwidilyRb9DD4BGASEOs4s2T7soKLClUOL86uV4lUkEkSNuvF7QPXi5Iu2XTTEs6A1TXUDeG0fxXn6IZzueRAUO6QmqJJAuR5vBC2JSL2eUcGYjBpfRvZ9WJTTs8ZmmRAI1bNR6znsVkc6yT0R6mnTUSjo8wTakLR8iaoJuMTceSNbDKBGo1YkVcR7i7mIvuQe4inZF5ig2rSJARJZTjF6YYB2lIugOMHIPDSoBvYiX5YnaCEUZyRk1hhsKQGjhGbe9YOWYJZmaIRLO1tA5hOvpaBPG4ugF2FqxKXA",
        DATE_COL=datetime.date(2024, 3, 19),
        TIMESTAMP_COL=datetime.datetime(2024, 3, 18, 18, 6),
        TIMESTAMP_TZ_COL=datetime.datetime(
            2024, 9, 11, 7, 25, tzinfo=pytz.FixedOffset(-420)
        ),
        TIMESTAMP_LTZ_COL=pytz.timezone("America/Los_Angeles").localize(
            datetime.datetime(2024, 10, 8, 6, 57)
        ),
        BLOB_COL=bytearray(b"b\x8aH\xe3qF\xca\xa5\xe0\x96\x02R[\x84\xfa\xc7"),
        RAW_COL=bytearray(b"c\xb4\x9f\x0f\xda\x14L\xd5\x8d\x0cl\xc6\xacQW\x9d"),
        GUID_COL=bytearray(b"1\xe6\x92\xc1\x7f\xb5\x1dv\xe0c\x01\x00\x00\x7f\x1c\xb6"),
    ),
    Row(
        ID=8,
        NUMBER_COL=Decimal("9276.36"),
        BINARY_FLOAT_COL=1571.267822265625,
        BINARY_DOUBLE_COL=8772.394278058591,
        VARCHAR2_COL="H6PCBzcHbDX3QjtIIrJw8oDN1cJuT3bZWzU8Enetvga0NS9Sab",
        CHAR_COL="XdC7MiGOcj",
        CLOB_COL="FXowU9q8Tzx8e3xQR43HJmmF3cajDvAAqOF01KXu2lY1rIXY3XKD0XZT0NKxt9nOUnyHT4DVEjVQy0bcrIkodqTduiui2DBOGwwOES16KMpbB3XAx5iFuQYaoivS8RQRThY1bvrlHd5ry1x0SWe0ic9zkMkAIolLY179MaaDFdwq2lJwLdkVgAVyQoGlylFeWQXiVAL8JsTWg6XIS4W3gPeVnz37Tyy1B0CCWf7vg23WUh9ofAvGYw1GneXvi3TvEPKFBatk5DSmqp5fDdvHtQfgNTS5Is2TzIwBcxc9wzsFxqH8I0EHOW3nxWvtT88cGwc0rJdsyscGwG4GlrKUZlXYaCRJI817xxhy7dl7AXBtHgUbNEYGcNELpgfNMDPsQdygTT0KGL9ULeDZO3MNBc4Qi2dQpWp3di9U0MD94WUO5ZatDfQ9Og1TTsdnFz9xfMfKqXwdJ2OfwLIOiG4jhBhwTo2alQJBpSquBRfGprp2CHSq7nCHVbrpWofhIgjBFjGxxfaIuMubmU0tnalyhQqNBLAizHZXws171en1SXY5yhz9LHcB1DjlpNUccg7axEML57nir1ZvnttaIo04klNFnq6oMGJe8eF00r3LvCP3FcP23lLo6h1OvIqLEx7CoiwHQi8hcRCTuCyN5iVBdf6VL9YO1JS00iaUjSnrLtd0vDpibSgL6hbVAoEDqBbJhrxqVwygNJ6TpRB8odZwtGbxWt4Vl923glIMHUKad0uJoZNu6cwdDJvMO8QHDIhaPckBdNNi1wS91Hb4PdEjVVY0ebRF9391SrhJJE8s6i0ffZDUgjSFXOnTbySCjeXTd8pwQMpbJfzwINXBES3od0yasZdAxZmBXy3y2v4DYdD6hFBcmNzQRYzMrxhadbFlP2XPCmzEWaf3p8wSRuY0H2Fl2HMsWPqT9b5VigSihfCjHzZDPjLryZQW7MO4wWqw5bywIqzkceYWUm9gV6CaEmR6Daod2eRmTHv2jYk4",
        NCHAR_COL="Ez8YlApvvz",
        NVARCHAR2_COL="gmPfOxjfh9si3nGJdT9opdPSiN22NvUfQmNmsCHycYJvKP0h28",
        NCLOB_COL="8jEBfvdX9RBjAr48u8GXRDpBazIT2CXTH3ETVn0QvaspDJ2YhZaZtiLSIJjof9Ol2nzwkCX1Moqfk4Ux08U1wKjVx7aFeFmIyQps5G2lEHcjK6HKkT7JvW74GyKuIXE3cj5efVAwnQxEaB4fMBVYj5PBjrYwL0OAX0yJTM1UqwRx8tbmQJ9b8XHcvIwV0fsDZmflzhWHuDojn0GgGwOV2KGxfy7PmNJfOYCVzoHtJ5y6ZI0TEZvv8QNLwpewpcXQBKDt6AmpeGFZKDpfUS6QC1l2bbkNjsfC9FAg1iTHPXNAl7aHrcFbXV1tRpuWUB1qKIELhjaVaDF21sysFiUjLmjtyp7XFvmXIyseguiUHFvzRMnkMGnVE0CDn4sOfpH6inpxVWepCjDVMyydEmFVtRzD0sKdCOAAnx3bxptTrI6hhmGw1iEED9Z8HlNgOStCo2B2ImOj7JI5VXGjH1z1XOtypSNaH8ihaO29Os76drWiaQGpFZIeFedMY1m47QL6LyjSAI2zoK3TY1rZyiYH1vJVZMhgIqIqBtFTVFejRb8TryqEwVW1SvlhY16Jv8Jg7dME2ZVQcSUrohrwtvRPjLgYfY7J6Cs56GNDMGBFr3oZob6xjuFUkyO49uSM7bDnbJ48oq9ttYMPakN28pbmpccdhTuOcvIexpODaoMf8fr7SsHpaFv3BhbjXZFYX8ZSkbZoSEla9UWmltkg47aHqxpEGsrHUiEjT2Y0EfD0MW0E9MjkQdOw8TaQ0mIakcrq2N0l91SK52BSGM1SCawmb75gSZ1zDKZrm3vZ4CzQbBvbZzpvk1HD8X4jAcloAnLtv4A7t4FXdLMJk960Syhvc0yapz4vM0SdtMcQUd5gjYnaJa0HRUjLyC0zAu14bAT2ibGIzN0yjwK8V0tk0eQ4DkMfVE6yhNXn2pM3DGkBAkkZ0XXgXG9kOOFRyZzGyeENIbu1rbi33U03VDdu2TtJnmzUaftneSPohTRH3UMm",
        DATE_COL=datetime.date(2024, 5, 2),
        TIMESTAMP_COL=datetime.datetime(2024, 7, 23, 3, 7),
        TIMESTAMP_TZ_COL=datetime.datetime(
            2024, 1, 8, 16, 43, tzinfo=pytz.FixedOffset(-420)
        ),
        TIMESTAMP_LTZ_COL=pytz.timezone("America/Los_Angeles").localize(
            datetime.datetime(2024, 1, 1, 8, 29)
        ),
        BLOB_COL=bytearray(b"&\xf8\x001u\xd2\xc7\xea\xbb\xa7_b\xb6*\x92\x94"),
        RAW_COL=bytearray(b"z\xb2L\xc7&HI\xe5\x83\xe4F\xc6\x04?\xd13"),
        GUID_COL=bytearray(b"1\xe6\x92\xc1\x7f\xb6\x1dv\xe0c\x01\x00\x00\x7f\x1c\xb6"),
    ),
    Row(
        ID=9,
        NUMBER_COL=Decimal("7498.42"),
        BINARY_FLOAT_COL=3271.755126953125,
        BINARY_DOUBLE_COL=5220.24584422112,
        VARCHAR2_COL="t0vbSvcYu6iUrnIZVQuLJfpTdBNQE6U3JHz3OOdizizgypYAnq",
        CHAR_COL="bRVWCIaLtm",
        CLOB_COL="doE0hc3ajgaKy132fPMG2DHvwhE8pmObIKnrZLOEfhubFOCpdNr9U601dazrdQ9QjaktExjy9DGLxPOMkpLhE3bpICY9w6in0TrpcZfNNjq5N6PSogrGary0GGfeqJFnhP2TDVIrEHQgp8Y46a8XUJx0osO4v6V0Cpb0rXf1AqqXxPzZDG8mHGwUmkQaEjGbnTQlQl2M5rvvaiZ6DjQnD7nAtrSEn1Yy6oSpnbNf4NNo7uK2wRFJlAiWxgwHjgbGttGt6v5rQJeHgkNLL13nZlZCtDjDkxTRgotpIDtYdWICyhDYNU4rch0WMZPIUHOPRwMvQ4lF2llYj4YNHcUvS1YIE0EEpByoSQUuiJhuqeL0uVqYpgRt9twMW1zH5qIIrGa4AapJUeNKMakyCFUeGeFIK1rP4p3daXWOcpyotrEoperkCyGwZka5Gbpp9uy7Je0TYMM3cG1OkjjXxSBcwB88gSUO45pYgrQMSTdJJbmIohkvkms4DBCw694lzPvFHlVsTzdhiQRJQxeIpHWdzSdQVSO92gv38NKyn9WD9d1wuqiV7lsWmYtizFQwUSCLtrgqutqIAe7BEMg9hQ4X8W5nyH5vHt2f8ei2VApC0MnHoMfoXvE4l6bEAyjYzJH50NsQmH1SZilx3FAMtnKQtDpTWvMnZ7z8vdjxIhWNYxG7Xddm144DJCvNFY6t6VyGxeJHseCco7YYo0A0PFTHNAc0mxQiDa7XjlHclMNVkwRpjAHoQ7GiTeCG0CfBn5VMOiwNkgDXBviXoqqyma862WLJiSLHfn3wVNse1nZ8vAFUgZoYpNdEXzkezVWZabGSl7vCykutpzECegPTr56LIoFeKiSYeiBf6sGdKAuyAHjZu0nmB5GZ2DtGbZOxMTWDwt7hVCZnmm84wJRwms48fgvPtK2mYPNDEGESNkomkhZsQiyDc1c7nDwxamgV0tdI2pSmzeZljHipiIZoN0emC839HQNZlCJheg54AfsI",
        NCHAR_COL="S2Q7WrPop8",
        NVARCHAR2_COL="o2sYwsmqUk1dIIiH2MkgHodKlINzT7wm812iXWOj6JmAuvRyiS",
        NCLOB_COL="feQy1MP85PrhzpBbV9uiCWG5uOuvm9QfMv0GB7l3r6yfm74Weiwvqi84sB7UhelZM0yhqNFjsKW6oZRsDm5fQBUWWXxVg5nGRWTr9O00Nw8K4K3smtsbj68qNCkAFmfXPckQP88nrLFYAc5UfuSq4NdNsYCoKqgV8N3TghivLGrztIIMGtYigV2dzFwhsyumJiL9K2LiIMCCEoxCRepppHXeYfAkHxlU6YWxbCNbbrSoKutK9qTcv8RVU89kpcsmcJ9TyDArRq6IU4czcnOzkYCCBES9aQ5btfELvKLgFlljG74l44LAG7gsRHncBH3YegM3txugKCQsxAvRaoQqb5ypHRkKxHFHp04I3HMWBcWnEgPGMaF66BlO0AXo4A2JT9SVphyAhHzYEb6lTi5b0USg3BN1Bv5Tae1xWOvZd2sEMxI5HRmW46IXhYOzhXFgyyJdmHnWbgoDw8cv49vbt5fenVQGokWT4mBuxybVWixalV3R9xtZu13julXkUmG9dfOxKRrrEyTAiQfSBR93wgu1goUh4lIyaPOzKpf2SaWsO6VWhk0v2gVuZVTcDoTW3J43AoaZ6PbgDYEJ1QDam5I1BEs348dcQ6MOhNzYBthOXkwZtlfjJa0KLXksbH6sB0bOvzNmebxLwi48uwApBuxTkalZEgIJ50vGCkcxrMZa3hehM54gwSRrDR8HlmRKxWUDBsTTjSXyQhy6ilCipJFQ10rpmHPnUvQm4m8x6r0DgZQqE7uKPdOu4soKcWUWZsyri9a1CIJRACxdRwt3GficaPZrhOElCJmmIWaxprnm1UElJAKrduokB6SRroeL2iiCBQjMDz4JgrErWmIM98ITN8P7Zj6qmL2803dNCIuQ8ma0hYTsWm3s0arhhgmJJWjRxjjNiZRJ4lwC9ilZfxTik7bNOGtfbWEUlLGR3ovghagE3W0kqwDV8KyfDlGdC1BOHJMBpCFRxpwuBxV6mLwQhT20X0d5oeYEPkQO",
        DATE_COL=datetime.date(2024, 8, 3),
        TIMESTAMP_COL=datetime.datetime(2024, 10, 5, 5, 17),
        TIMESTAMP_TZ_COL=datetime.datetime(
            2024, 1, 16, 10, 46, tzinfo=pytz.FixedOffset(-420)
        ),
        TIMESTAMP_LTZ_COL=pytz.timezone("America/Los_Angeles").localize(
            datetime.datetime(2024, 9, 11, 7, 22)
        ),
        BLOB_COL=bytearray(b"\x9a\xcfT\xf3'?8\xfc\x0f\xe1\x99kF\r\xb6\xe5"),
        RAW_COL=bytearray(b"\x03\xa1\xf0K^\x9fBI\xb4\x12\n?\x0f]\xc2\\"),
        GUID_COL=bytearray(b"1\xe6\x92\xc1\x7f\xb7\x1dv\xe0c\x01\x00\x00\x7f\x1c\xb6"),
    ),
    Row(
        ID=10,
        NUMBER_COL=Decimal("5945.87"),
        BINARY_FLOAT_COL=8524.162109375,
        BINARY_DOUBLE_COL=8750.63656795761,
        VARCHAR2_COL="wilvhztOIPf6femDVHXAiOOBYLspDGjWfhrlIT0vaNqiEnQesw",
        CHAR_COL="KwE9IPwLI2",
        CLOB_COL="YEby9IUvMKjHWnN2kRLct4hOkAss4cLSTkEk43CtLL05FNyAuZ59eYgebWVsdtyOu7jY2g5bX6eubhuM8Toy8UDxBROMny0a7CGczHqhZJuvUAbicvRwwsPn2jLoDzjWvave6MPgThIy6NWyARfnONUwKxigpDvTMz3mexaSiAzPSzOK84QQUUhAlsPYjCWfS4lnN0wYySvLggFDyQ7kK9JyaetMrl08J8RiuB3RRywYJWLUySWCNywCTv2QbRTwHCetczHnM29EnOOPD4otDbdr4tzkit6AkxQtcRTa95pjhP8XU4jC0GHiywEndBiFGAiBkn5kdQuckhqxtQsZq7s1t4kndTRNSyUHOv3R6qNYW4wpWLe0JtPnhlhQTBEwImdOhkmzhR4xfa7RoBgydEaIdfCoxaH9GPZMjA60mCEQmOsbDaOPwT7ff9iWWspjAgAs28Wt3ojZ83cz8hFWRvn40lLNfwhnnECFBymAObuIIkN0Ik5myTcozjmGrOjIB9QFT3zzlANabEBNDGp66n0k49ji8E41q1bZW6Zutw8FKsXpsaoSq4pcp94axHhAgk7cWJTRqlvWiePHTgzLe7S2MQEIyvd3pNosMez6pjDcULSK8ihTk5L5hhiB5Q8OnTOjcmYWcJCyGFyag16g9yhrPICdbYjbdUJKQ2YwFRLcj5GFlS4OUjg3r4RLgWgY4HKjfFTzw5CKD848GYT1kxfsq1ponv8bDNDCIXLoMSTk6pQN6JDLFaleKeZq5obyPpAqzPwqyBocSDBsLtpDP9KnKbWS3Y9InmZzSszKryEpZMaZfUrhzX2V4sXiVpoKloIe1Awrrv5Z7fXrb2X3yDIOt228w8cCGMUmdhMlQ57IYftOyOLbdeJA2Og86waem0igRErwCBnH6eCj5jwnBIQ3Qw8IBYsU2khkkxcnU8VAUsHDdGnklhCSWDK96mKLF6xIYJZcJ0HcuKrBSpOjkcagGCeqWAKZEBRnRfEX",
        NCHAR_COL="7COp3aFb8t",
        NVARCHAR2_COL="73xGULo2BmHN1EIuc16Rg8q9JvUF7nTUKCehjZPebqdeXfkm7P",
        NCLOB_COL="ltLGlmhydXz5iZnPMYGwZ6U3aNV4qVYPLZ4WWPMluMOoZPQsXcc9Jxq2syzOY6xua39rxRU1gayJhW62kqQyOQpKD7yYHTZvJKSQY9ywm5OpKEeSUXtQFSDUj3qMOEzzjFXMAkOXnJjrSOK1icbr9gl6cgIbF8cIPWjC5ctGjXhh92gIqkPxiYUo1mLeQzdAu9cZJZAwmXe69JWjzP2Y07XKilHWI6VVApeO32DTSUeVIdV9BQW2Bxj1YdM32INsxOSadNTHAIeQC7xNgLwdW0hZN1bZr397KmA8pYXL9zncqCnpBotgeSbSIl4kP42LlwXOan7hgN5g1n7dPJAjncbXBSHvPxunV0HBM2Rg1PkwGtqoXHhXEPtV1A5aojY4aYEjuHEd8XynGDLcK08bN54GrfgiblgE6ZBpRNzeudpt6yFmmNyJgacCfbYcXR6AWH5z68bKI4ELS8nmWDOJv8zvoeVLmWe9MoV51dmueuaYmZpetMIuiDnssZSM3ODvgDwzk2VLOeduKXsi9jQ27DscyVfM3OJ8OjBzc1ZELZQrnDAhVtuBSahBjJ5r9NMFAQ3EuQ2aBYPEgfN3FTWJQmZ1FUDZHtFdmJQBeIdsNdaD3SvCSbi3Yxc5ghZf9GFQ4TBv24ba9Aironm4IVlxj5VdXx2kSjxaEqqnR5YdAO0pvXksyj22ayAuMXrTD4yEzfmsi8aFHNJ10iiKbJdMHmPyIXdvrJGG6K1mZiGHrmJkaCQ0eH3yD9yEzEBw0mIIWa9TfcgHfw76vSYlTGlZiIdhI1zhpG4bI29vQbgP1qA9uLquJilDT36UGZ3ydI98wMPJqN9B4bNCAfRR2PEOOV9NAVEDJ3GZj5iFkZMCWPsOat5aT2LSzQYbFUDSXC3z7KI3xeiSdgVukXbNhSkzdxhaD81fLjjLR3TI8pdg0WZmgXIF9z59e8kQYLZPxTCkPeEQQ6Xe5HlxLSaleAs2R8s0h0y9BjXwfB7ms4JH",
        DATE_COL=datetime.date(2024, 12, 10),
        TIMESTAMP_COL=datetime.datetime(2024, 5, 25, 22, 37),
        TIMESTAMP_TZ_COL=datetime.datetime(
            2024, 12, 11, 4, 37, tzinfo=pytz.FixedOffset(-420)
        ),
        TIMESTAMP_LTZ_COL=pytz.timezone("America/Los_Angeles").localize(
            datetime.datetime(2024, 4, 19, 6, 28)
        ),
        BLOB_COL=bytearray(b"z\x8cn\xcc\xd9{F\xf1`\xb9J\x0c\x0e\x01\x8d6"),
        RAW_COL=bytearray(b"\x01\xae\xebz\x8apM\xf3\x92:\xf1\xe2\x02t\xdd\xec"),
        GUID_COL=bytearray(b"1\xe6\x92\xc1\x7f\xb8\x1dv\xe0c\x01\x00\x00\x7f\x1c\xb6"),
    ),
]
oracledb_real_data_small = [
    Row(
        ID=1,
        NUMBER_COL=Decimal("8340.41"),
        BINARY_FLOAT_COL=4805.994140625,
        BINARY_DOUBLE_COL=8417.980669484725,
        VARCHAR2_COL="WX3KCfNQEczjtbf4kTnjrO3uVNSQZZzeJdhNbFJUgcgJqzcFDJ",
        CHAR_COL="nDTvvvcOjE",
        CLOB_COL="ftxByY9JjRhOITgsqk8ujyUwfu4PNsBwbdaOm6dOC0QHrrLRlpryCHp7EL2zxvrMde2aVCRTbIRhGk1yhgwxhcJPOInrYgmhmT4WIKaQPZBrIgzzmG6lXyTDj2xW3KH0aJphLrPvFM3ibk9Ir0cxfPkGd4d74V5uLbspfNXlZYE6mIUQKFSXTck7YAF367Brg4ROXbOVrqBLcwnmtdwkwlvYAu84wBCi3AuBLgFA5q4aGldDmwn1ZdYn9uvfEJNVCFsIIdPQRGUdNNfyYrYNGUBPrXrMh6SkcpN1KH5Gi6xZ2G58sQPb681A60vO64iN9pr0cFnDvV8JFw25ijIunwkdTwnaOdc9Pp7GX4FmxoyTOxd7YRqfs3PFTtrWD3yV7pP9QePp0ZFkKONtJe0cHlll8T9tTd7eXQmpza0z8hTTYBBwOCoeAUQjRVYsRLnlnY6diACXkt9R8hp80CqNNnLdjGM2drioF45QtyrdKjSFT4UKmoRc4vEGy9KIaw2U0YJoyNOi5HsnXN1dwoOw6Kop4YQmE6CZ9QmlN3kIJUTDDPhTU2gCEOZAQuWM4RIkq8FzkKPU33WHk24671SxHqiwjRMjXebsfSl2nxrHMpOTzUA5fRWkVT0Gncl0EA1PGj0Aoxgcb6ZymTyJ61dmzsQvLhfrAVI2u84zFsCUl5TAKgn7RZkEtdMj0VjJCEizcYEETHClsvG8Qoxzfkw0kwYqqYopVkNvjmhpQtzjTddC00PRvC1tn8olcLuxXoAk0w3vsKcp0O3VgDlfNFaFOXIU5GpY84b3kKcLq1fNpkBqJL0LCTEPI4yYcGD9mbdBgnYYJSB1bG1yfkXiMZn3XAhJvKd86SrzsRP08BG03UiryXMfRyfu0jacdzaryW8fs4uf73XtYjdCXP0uSUJDrf4Sm7ZIxHTgOdBQ5dY0nE6JYWAsDraAWieHfYdvbkfdtgM7c3n26rbLpDkFBYCICjtrUqxArmBPzrQauson",
        NCHAR_COL="dox8Ve9zBN",
        NVARCHAR2_COL="ob7St9zU6QmRnMeT7zNlxdTJIQYLJ9PPHtMA6EwhfjGXLcWdBa",
        NCLOB_COL="gMAYKxw4X8cEq1zs52LYFSSkaBp1Nhf7OU57COLVfjv0hcRngCJYYCEXx6ub1einS2ObTCDbUHzjOPnKBrBTdy9HjwBt2cZ0rnZ3hEM3X4ulnxfXSNZgZ2lXgafCY5BFZoAjeLF5vvCLPG4C8zmsnGOsuXWGcElCD1aWkNYK5Yqse4VsTI9U0IdUSH3wRHXwofA24zZX5G4ScL60Z2ygzDx27cQuIzxGUf6NvS7Kqv4faowvjC6ADqg8vyHQ42m7o0phUSNKuRjH2qf1W0vAafdOPQmuTwe0Z7iQYLesq198TGokQsbWDxDbPJiTUjnuN3WRuYOb44Fod9kqq7rl3cUygCDtkEpDTzxv7mA9ZLRzDvXkRi3gBSgIWk1tsclaJVhPDEFUi73D5GmAzE92NIs12BoMCG4mXX8woHhRGm3vvVK4st4nJTvPvNQAvAiZnaOzC0frqj619dc3kYIzphdwF3KuUlTyjL9uz9eOsHdFhnsHnizUFG8alvKcDsG6LfKvkVnIK9CuGvR2Ob1jnnij2RndENy6DtpMkzNFwIeJTQPgg0CRuKfLkVj7QYTvSO0kIcbgIbQrz6VMSrDuY3qZep1qWLeKFtBMLV5EqOOF91zIeoQbTD6gpfJ6Hw67fXmXfDkvMVns4IbTNRdChINEWdegZOqBo7MmEYOq9drn2N63HPmF1dKj5gMitfzbOOtkUdtvkEmkuUrORLDJO4vA3tvI982K6cKNByi04iLBTNeQ69cmpYtGgxys3hW2FoP2rp3Ys6PZ255Q2pErUPfozLhgBTnt4uaLmF3ytj0AROqNklZTncjudOje5TfTD6o91cILaSiUOPrDZzJUrwB4iDDf5FPPs8e97d2ydF7AQhSbk8wIq3r43uzh32O9oZQJ6oNTdb7rTmA4RmvDyEr0V7loMmIQSVFQFxnzb7LhqxuuIMtANkP3JLn6DUt5XzCaKi8LRwGrfq4LW5YQ0jJ0690yze2ZeAEoLrX1",
        DATE_COL=datetime.date(2024, 8, 13),
        TIMESTAMP_COL=datetime.datetime(2024, 3, 5, 22, 33),
        TIMESTAMP_TZ_COL=datetime.datetime(
            2024, 7, 18, 12, 56, tzinfo=pytz.FixedOffset(-420)
        ),
        TIMESTAMP_LTZ_COL=pytz.timezone("America/Los_Angeles").localize(
            datetime.datetime(2024, 3, 21, 23, 58)
        ),
        BLOB_COL=bytearray(b"9\xef\xd4g\x8a\xf6j\x9f\xceYv~6\xd0\xd2\xd1"),
        RAW_COL=bytearray(b"Zw\x80k/\x01CM\xbe\xcdk\x93G}`\xf3"),
        GUID_COL=bytearray(b"3\x8d\xcc\x05D\xf2I\x0b\xe0c\x01\x00\x00\x7f\xae\xe7"),
    ),
    Row(
        ID=2,
        NUMBER_COL=Decimal("4162.85"),
        BINARY_FLOAT_COL=1085.7430419921875,
        BINARY_DOUBLE_COL=2759.248254369904,
        VARCHAR2_COL="ejtMYzPEicLJkb6WzKDSNdkJYnavD1zfpFeGZdY8zq5JGYDgNP",
        CHAR_COL="p7XkiUw7g6",
        CLOB_COL="vvi2b6vmpg1wuxURX92EH2nN92ZTyAsXSW2mD2IeVkKC8zlBMVL7uKxJlGbpSKJtgNeKX8qQXYcXEafkWJio2TN0Pcrw1WoTr52kdAVyvjVq28YCnQrYx7bqzK4mC0eg7Aznu6ilOYLUFeqIg16UGs2OQgu1SVhoicjX7fzsnP4z6RIZhDeRHt4U571POMAtSDkQVoqKRaDd8DPlhlI4FzHoSNFs2dr6aQj7g447w7zqSA5SS42h19J8LVqqgfnXwL1A77Z0KNkZ92WI3uUo0S7k5idlmqs6djj8BHBDdjxcjmooB8XvaDl5aP6QFkFM0hvD1MViPDjSHlBIXzBcH4a0THrXu9GZQPZL5fYRtKs7KFfJm80UZH97STG3v2YPw6e9kptwkxfKVqlA3eKC9cO9Ft0nmUWcJGFxTGqTz2bib4U0zOQbKexi04JgPpvYxMSPoMseybXvJSjfjIqe0fNDErRePi4uI2AY0oSFEcg2Fgd68ghggkt7iEpxx1SFsCoxqmxnyGOVne8un4KUMRwae3FsZlbwu81MW6z1CetAECsepqp3nK4lOdSExOykkTjcuxlP9xXKG8CKb57IS2TwGpgrahwiQrRGzuScIle7qt08zU212lOQeSepC6MT2Q3S0j02tSn5oukg7Xrk7HUbRvVVtbzzYFXJsZ8u0QaV5MXSyJvWAKeWB8DNk9UaBJKwdzrQ91YrYga6uTQ5434foX84fdzVn9d3sTjTaq2wAkth0ykzWpJXdgNsQO6ma8szQYAHRc2Cpay0EUDhdOOqQ3ZqknvxFLKdOT1lrxwb9LljYyRTm9dsOgC1wYPenGQJUUnBKlpsEeYZSjRWon5uexbV9gizmLLk6DbMCHquxHNgwNkFkk2p40X1JvUCQFSGHQjGvmSuei08LFeu4QMUvNL8I09JSVhuPsJgdltojtyh96HKizOtB6KxHzHeOwdceUHirCIewVMjo3YuVAzf3AO3NXV5ZJZewpgD",
        NCHAR_COL="MRK7TyJmYz",
        NVARCHAR2_COL="krPqCqwNErTXhnJHAbDBoEq7xpGlLhg1ExWKFUiwtOVs2N5ZCB",
        NCLOB_COL="DWI2Y8H5VoiqdsURTxkKG2803qctrnbO6p1nY2vRE2jpmtXjyIxBRCryg2Ftzssadw6Mavxi3qf91HId3BehpPeaOOc0uVLGxnBjC1tgwk2Mn7nc2hWi1BmkdlJrpkpKsaje4o3rQp5Vm4Wu0vhCuFPVCuVH0ykHfZdbU14wwrAKc5X5mhLYELM1lUGGNKepyfzvLo57X5QoGQlcyV6rjS1ZzdWtkXbJLYkaVTUr3XY2hgXAv1XJPE2pFD3NuLZ4eSMdH1sFfuOH53I0y5pr9Rqd6m2qBStXUm69W3Gryc8OtkOBe9gR8fj5mh9ZanzqrMWj3xZsREZbrLlXYIki8JKf9MCMnZpMYHBc9RsIxq6cHCnu9r8HW0NsE3Bnq4LuFpaciKEsEZWqrk3ID8e7ddoAZ9sEvGc2NBk9HAW17QuSqwFuyca98L7AQQVpsM72nzToBOLKqxpwIKqCXgFY58nOdFReGVXEX4B6ETz4d0yI524mual44nPgAcBJ8ma7WyRhKl607IwGnSf8K7SP9u4uVHpwo7fJokf8I5An274gMH86zCWm4oDXgsUscHktkdLC7yOPswWAAq8howN2WHO825wS19G2pSAJPyZJtRkLTL9HRmwzcVzusA999wBqUO6h8lvHsymnOUFBnPUZvYthTb42OmujvG6PwvhXaxCSamUJZcCtR6OoBsKasSbt1xgKkzUHRlY4cB9diWX4v6l82YgSpW10fHsEOgQqvVbQCobAAhKLsmCNC6prl3CfH9cr9YTPXXizyl1SwB6S0NvGKmlWMGeotEcW6NofVB84i9KJjIHLDm94Vl5LhpbKNgVhU4ALVTRJAaHyayhuI3AuHJ1hFqllYvVcEgX3aPeVyqWFBYGID5Wf0dUrqWJj3Ebcx3iFphdthovLIvWaCFis4CeEQmeIB4ig5DNIArB7N6swIio4l7F6I1wycGa9pjQiJikSdWjZPE1sQ9jxX5jYetuDAFIgz0ypaout",
        DATE_COL=datetime.date(2024, 9, 26),
        TIMESTAMP_COL=datetime.datetime(2024, 11, 21, 1, 12),
        TIMESTAMP_TZ_COL=datetime.datetime(
            2024, 11, 17, 14, 47, tzinfo=pytz.FixedOffset(-420)
        ),
        TIMESTAMP_LTZ_COL=pytz.timezone("America/Los_Angeles").localize(
            datetime.datetime(2024, 3, 26, 23, 17)
        ),
        BLOB_COL=bytearray(b"\xa8%vFm\x9b\xd5\xebud\xe0\x93F\xc3Fl"),
        RAW_COL=bytearray(b"zC\x96\x1a\x08\x9eEF\x87G\xf5O\x10\xcc}\x17"),
        GUID_COL=bytearray(b"3\x8d\xcc\x05D\xf3I\x0b\xe0c\x01\x00\x00\x7f\xae\xe7"),
    ),
    Row(
        ID=3,
        NUMBER_COL=Decimal("9630.71"),
        BINARY_FLOAT_COL=9022.4794921875,
        BINARY_DOUBLE_COL=6980.983732772723,
        VARCHAR2_COL="fsXLmDk32A4QGcZBLafsUiAxsluI9kgNnKlaH6pymluhllwFML",
        CHAR_COL="MA88qqQR1Y",
        CLOB_COL="7T0yRCZGrQ7ukpxIUYNitp2N1ixGNmgZu3vXXH1soc5tAqcwb28yRdgq82QmO4zaVglgKlnTDNrhtS8aV0eet0xlLIp5tlHsi9Il7URmIDUKOR1rro10zR3fTi3SCGvAXhf1WJltkqOchO39b7R4gABeJ7R3z1n18iE6ePruPvqAZ6jjWrLErN23MOTYjlF8mamOTBEyA7e3kNmYumesJyp8JEb9kJEsFU2AYfIW8gBTi20L1etuZtV3JftV9Q5MHe5IIJaGG9NvuOOcYpdycDM7f8OH7XDRsYetpqwor8YFlaPOw6JJBTpa4zDERlFDybmCDHulU1f3f1MLO0euv2A4BK60O2yEwsFZUkgPRLfIqJlete3wGa0OtO05Js0u9QUiRzcMptLJZWj3oSTlb0nzFNnlqI6lXX0Wx8fGQ9NDotL3jkvsryqGuQQApDyTMy2kujOuWN7ADqoDEPCMzfpYIM8Yww6JNs3ZLsQ9rIYihEizrETxScR3sSG75MvSpeIzGh8oyIyRABulqkPDwN0NVjVM0KpNXbBCuegxfxWuqIx4misz83sbgT46zxtuPTyenco0owA851q41QpO4n97PbQcieUKT51pr8eVKeZkHmzPYrSfXDTMYx3SjbHfze3QvsqVGsllSMd5wUxqNE0jfbOeGU9dE13lcXidakMTzb3vpsVhTtp6ZcexZi6wIPWYDbpNXrkBVH85cWsdXP59p1oytS8GmsbIcYs2vaKATAKJ3Fo6EYbpR6mBhaDBEeLtMXzUw1hb1ONIB0oTffJ4HiWZnZG2h3EElrTsRGmiodKzVGSLO90vuItUan3IbJDeXnOkCikXCU2Kdjk7gyDaOcevrJtvEkPcDlfFwAbd1TknBz1KgArb2x9aXQWnr6I8fINqsqM81UOwU2zgYONYuA3LWovJkxlLayC5K2Fh9xITGYG9GBSGP4plNnwIMi8cc2nClJLBR3wkxreA6olgOYNEjR75nW9n82rG",
        NCHAR_COL="b19ki4osyj",
        NVARCHAR2_COL="UzyqRfV48rxEhDZnBZMzAwsyNJGosp8C8aSJwHfgBbKEbm4Exi",
        NCLOB_COL="nShOXgylAAL8nUSURsobJAEuPra2n1uJhW4SkkhVTs1o5yioU7o8Z4Sfwm3fbXbtv8R4DGZFyGPJGTIVcKwCHwUqFLunQeRw9pWtJPCIqhaRmqh8shr22tt8YHxb5hMBANPhuBGSDSqegK0bsTpqz548Hf5363guoVho9aAWTrAM7tYGkD0K60f4BZ6tvfhGXLjPTrRrZXGrSbRy67Z46oGEjjL8IInubZDflBmGzDwu3fbkA4L7ACgf1T2wCXdZBYoomPSNlXYnQ8Avzr2gCKOkmLwSKvhFSSdPmHUBz4eXCDLOta4FrxXHZlLqOfUhHXWveNyuPdFkbzWkTFXCEFgerjyVXDww9KFYRb4QabGLAVTBtd8D4i2xwZoSPPhkzkrFSBRozyyIR9fvbSBVrQCQV6inWafm0Es5pRtuLs4sc8fjZcf8SfkpDR5PxcmO5eWpqgxkEMULbaL5LrbyNmfHUzQ4W3xVjClgzbyUIu3BrApkW5OwVxnqGkUWS7HFJJ3MluufozcUrOxASC2ntLv9QRq4GalOhjsJtpbSXU9XsnA8jHV8nLlPOySwf5BSVsgqeGkQUIIpp5SljHrREVUR7unV67XqVEuLmhVFL7Gd7rzlnwFMdG71xI7dHkHvHI1o1A4Ls9EsoEJFZVZHfEmwR64FaJjkZ8sD7ifZmJQYxSY4bgopNYWOOLFojzafdUGXgD2LgFdcd15I9xkt5zbeKjk17skvBuh8f2mpIx71EC5yRgbP0RxFJnlCBzkZBgZcsjKjgkVsYw16jOvHbgo1yL6VDTMQQEBOAxwhAe0F0Bpt7Sgsd2wlBKg1MiQlSpKKlSBeJQ8DCzMMjFOgfaNOFUA2QYkUflSIUHtyMvfVvC5TsB7oi5I2odrySNRKhiEOWvMFDzEUzFOva0XH5UaUyQwxWaO9h7vzltKBMNoPgH3ExBIZq1CTaIwFVL9OOPg8AMxob7nFprE4dQVv2E93BwTgmQPNqJwrCbpa",
        DATE_COL=datetime.date(2024, 12, 23),
        TIMESTAMP_COL=datetime.datetime(2024, 3, 6, 18, 26),
        TIMESTAMP_TZ_COL=datetime.datetime(
            2024, 3, 21, 10, 36, tzinfo=pytz.FixedOffset(-420)
        ),
        TIMESTAMP_LTZ_COL=pytz.timezone("America/Los_Angeles").localize(
            datetime.datetime(2024, 4, 1, 16, 41)
        ),
        BLOB_COL=bytearray(b"d\xbdWS\xa3\x9e\x98\xfc\xff\xe7\xe3@j7\xa6\x16"),
        RAW_COL=bytearray(b"pGH\xc1|\x17E}\xad\x8f\x06\xb2\xe5\xce{\x85"),
        GUID_COL=bytearray(b"3\x8d\xcc\x05D\xf4I\x0b\xe0c\x01\x00\x00\x7f\xae\xe7"),
    ),
    Row(
        ID=4,
        NUMBER_COL=Decimal("7506.30"),
        BINARY_FLOAT_COL=1671.8011474609375,
        BINARY_DOUBLE_COL=4183.881048469414,
        VARCHAR2_COL="nvURvROAFver9HKGIAPGOESRl8Ez3PYoCQUDg9Gd1sdol0Ptwn",
        CHAR_COL="hVwxroQQOZ",
        CLOB_COL="5eCHWkw7yUmxQFFQmcSq9z0LpHPapfa6SYT2MVWLAJu5viPchjiCqFherSgaFXhHPWQsdyWtGOkDSuaQQGktKoG3dISBtSZVTaent8FzcQO1XsKOddiQi0BGRYnoUbbWhxyXd9jlZPpHUzfeteSX6fcl8BwzTsfIRtGaVaqtLXZc9pWdDd9pmyLqbZ9pBREtDHhMwdu8cmUJqdo1JdyIOSQCKfa9cjn1obuVbhcge43WIhsw2Ukh2XAo6n2jOwMi855qRQxBBlIbORT0fegh5r5ekUpQd51YSdtw7sjfX5QRRpD0yC47csFaOZB9xu3QMGjMn6vNIBRtqGlfk0bUEjfH8qfP1TzgSWVrOBg821x2U1a8PWhGbIypnYBcS4NOdTLH1bkcl73tLnFMFuIlrGjBekYT2CMvVRmBINmEHQ2e1cJl9MD7nVIhoSSZw8SCexk2sPgsU9BTDyelK3QCo0wqiE1uz55hADD8ustiQ38pWJnLOfx6P9KFU3jc2nyzIawuk9Bitn9iYWhPBiaz7oJ9RXqVBws2c7iT4rihEjaPSfTbzYscp579j8cUDkizGCSIU9hmySLBPUr4J3Ss4o5RguVIzkFiibE3Lh0qcpe4zlRyylNGREOplIII8sgXF8QcooHf9G8EZEjMyO2jIprOJs11eiMSJmo37ZtJKkoej5z5UNrmfxEh1ACQM3zkeVNzqj6dwt6iTw10urRuwVzVXzE5xwUslgFebUEqgIgOsVI6bf2VT8ma30rLpgXj18Oir5PD0vJtUpltNRrmVqlSJbw1fHaJOVFV7QbyqNKayNj07Tqq4ACtDymAMIYqc0Ri3pNfsG0Ue9BWHDOTf7Mwb0xwgmaX9pzqhoyXgATotqfiuOqHkx8xuo2AkHQg2gUMP8hYNeLpbTSThKOtgEJGl4leEHTBGz43tUPWbnj03Z0KkvWMEwW0fZ5opaXxA7XfwDmfBr6qYm3ePoPKqRDT796qSwdIv9SGjXfQ",
        NCHAR_COL="L0aVxYepN9",
        NVARCHAR2_COL="M6fUAhVMtvFxOb6KW3TrScFEmIT2IgDHOhYaXiz5IFdxKWmh4A",
        NCLOB_COL="BsINVyITvPbmY5kkPQYSlF2uJ9XLzG56xGsWbEcZ75b01150bF5zAypIhE6Djv8cLXIphn3XyRgXP1DtYX9o1dbdqwigomOtKIQ2ec8YmA8rqInwDKBYvqvgKsqKAaGcwS7ydjoJAoPVZw3cHQrVjBRXQOLrt7HBi5fe1c3UpM4ogRepdMcsreZv2HcGgHUr2IG7vMeELrRDsv7V3KUNOjUVF2in7stgXW9gwdkqUk1KjHzU2RD8cQekZcJpNLcfRN7NySvX4cLVsT5SDw40xz9SBIbpqlGL8cxvPJqCXeiROn2GAgUK37ccRV0FmQBJgTr2APCRE7NiJ729a63Hb1OT4TlX3NWttXjmmWSlqBVnqKpJBvnChjhcArUGrbBqeEGRWHH1AoY19x2nCzSckvwi4ZtuS6crjHD4KT8jRfuLC26rtuUE0daYLkMz9vZpSEmWpfOjZsYlXyE144ShmaWb8zaIBYofd5IuIBepxSuk0aHDykUXeL2erI5Xl9P8NuUCTu2Qg6Gv3qmeWh8a1RErGzpDEJXcXcEeJkdyhIQolqJKglRY8gzh9qHAR9cR375AzVuwKjpfiWpEjke27IYKMthUMvFBCWYrvZbnCokuhDHV0a31dAbB29tf8iqrpJrT7n4YYE5M6baIJZXsDGq5loVe7Ue9ILOjXBBskmBK7yh0VypskvEdOUXu8CLUEUTgZ7f6yVHBer0b8Ktwvi1qNI8wnvdpJd3fgDx21T7ccPR2yqjjMb2RUJa7HHEwy67UROVDEE1il2XNaFpzzvsdgp6HyVV2G56StZ0xftYL8GYNSHcdx1jkbhYYo7IRbjssMu6m3hIpCRrT20EvVxzsYMcjz9GDkha64tMatudbPAgJLUrS2L9hvmFVfPzUbbEjxFsWmGAyHyhOaqdz1bU5rjacL299qrWYtJqvH4txTLn1jbvVhgWt99qNjghPo2wWBtGHY7ilbjT7TcmBKqHXGMRcUS2O0uCGWSeZ",
        DATE_COL=datetime.date(2024, 11, 5),
        TIMESTAMP_COL=datetime.datetime(2024, 2, 27, 4, 48),
        TIMESTAMP_TZ_COL=datetime.datetime(
            2024, 11, 14, 6, 15, tzinfo=pytz.FixedOffset(-420)
        ),
        TIMESTAMP_LTZ_COL=pytz.timezone("America/Los_Angeles").localize(
            datetime.datetime(2024, 8, 16, 6, 10)
        ),
        BLOB_COL=bytearray(b"\x81\xbc\xa9\xf0\xe9,^:\xb6\xd2\xe1\xf7i\x97\xc3I"),
        RAW_COL=bytearray(b"\x9bT\xfd\xbe\xb7\xddJ~\xbe\xdc\x1c86Be\x19"),
        GUID_COL=bytearray(b"3\x8d\xcc\x05D\xf5I\x0b\xe0c\x01\x00\x00\x7f\xae\xe7"),
    ),
    Row(
        ID=5,
        NUMBER_COL=Decimal("3345.80"),
        BINARY_FLOAT_COL=9320.0400390625,
        BINARY_DOUBLE_COL=8472.386081268987,
        VARCHAR2_COL="4bpOSRyf9NnE9uAolPYEm1yw9BqxSED1n5qayQgwI4cKbZ3Lzh",
        CHAR_COL="d2bepqHkKk",
        CLOB_COL="VpVGDnTJoZs3XEtnNYcYOZuzBTPBhvbR6HT8bbDSj9NquOkVUTZZ1Hd8tenEUCP7bPllFl5BzPgPMzCXFM50GxbKJB3vd9cS7LGuauy6RK6wKc8dHZW391z53PmZYnH1F9ZXxpOdowkfTQtND5kViObrWKV7lrOeVR1iefNBkh9YbN4Htn0MyotfDmk0yehlBfkFAAEC7cWTX9k8YXm1n9NA3w9FeyRBfSlUYCjahjOtxKzVVuco2uoxeWJAGjH4rIyI6zsKyAdHR7ByiOw9tb71jPbRtZys5G6eUr0AF7gbLzPmlYrNpK20GaIqhTQIctKcSvpxdIM9TsOU9ZoYAy4GjBEWLJsS6o8VmifCt1cYb9WhWy7rMF1XYmtGgIM7uUK18XhlwHKQ0NyFTPokqajB64lU7QHpQsTLprcJS9Nad3isrDCT7Q8xJYugxT9jtOhFygeUJgNzXT9HOudf0Ai7WShagj5V73S92JZSHoaj65qJm7nLJPW0UUJQ1qqg4UzFIzvzqtK6begmLbGs8GO8rxn7ugoQLpLWfhGVNpQEAwEtBiBoRUgHQcGT2Xk9Jl3RtUYDTw4j70lBi6ih0n9lRj4ADG8UmUjyCJQ7IoPhSnhGbwCPqcCAdTtrRnRbExd60hQAoA6umqaQ1f2WEPYtBqHmIUY1YNwWdBlH22fU0NTueWbKsg16G23Eo6j8qKx4gU76qZwdeimJHpGaexq2VdOYX8P3AGfISSNrCm8swONxYWZZ6FPMaVAi7jZyouzptWIRoh1ibyBJtI270IbdvPCVs2oqM2JZMp22xJTd1jqt3pQLXwAKkRjBYgK8H3GsN1aTmiYmPJR82J2lApsG7cvrQkxc1gjsbPUa548afRF0tUOdMTHzeJJu9HYCpF6K1nPCOSOHFSQ6VNGQ5xOn2jWLbtDY6mH6rzpnFbS2Z3vArIN7emwWirFXklow4SHBwYT626kLBU5SKAOefm73xk01qN05e8aHrJvk",
        NCHAR_COL="2j2PdHoS4c",
        NVARCHAR2_COL="7IKv3USudDI1hZfhgkoOR2URQAOsNFf9zOirzI6jgzLFOrLXah",
        NCLOB_COL="RjfHqcksJJfx3zOHdwaIUyZZNWC5PXqBWmOYrsFSjRmtHiSaDYW8PaqKxF0kMunnZKmFKz35CC26e38UxfU2bRbpfGORqPgbvPmCClyms3AwHs7jDVHqVK6Alen0Wodq80HLlV0H6lQhQBU23Rn28M5pfVYbvACqCUycM1GNOnqG7mp9sVTSGN29azQkeO2mmGnfFugETOYkXroCuIxpSDaQeetgLbMxCIW8FyRYYtO9sW43AT5WQDGQvvIuWpbOFyB5CPMQlW4zHVlLrRwbcu5DhSwyZ5dsp3RYkEXGMe0i0AWrqtDnAdPDpOeDlamMLx0cGFInLb08HIFw6Tdi2rRLLsWaqAjnmO4Wx0KHWOzNeUao6Viaf7WITSzZPhL4GkvMgrrXMUcQeSYeLR72dz92NMxDOruBfuFO60uZm29ykDc3J8uhqmNuPEBhF0CwaZJwCZnzylNV14vmGdZqxjN5gfezO8dHzVw1xnXVGXXVnF0K5ZLriLqiTLCjjXAkWkNqnKi1Z0ok6HAa3dNeqbL00VR7tDz1IqPp4uAhXWvQBnTEvXWn3H4Uo3D5ony1uOkT7HKIBX07dtkZduJIoGltXELCBMK76E07HwsBRp9yv0a73j1Uw4TLsIbt9WboRw6SjOPWEof8FFCXelJZXOU5CK2bvZLy4x4rip8vDUULxuHKAoctmFe6hxNHkYjMVO3996ledb6Ns7C5V38EQsckcA8GhWZoA3S0R9o2gD3T35ZzSUMRNn7LoFjeBy2FCGIEhunfC84Bokr6go1DgreSdBizoKI9J7WnbKI0gZDD92kfqtXnyZ9puCRq1RV9SoND8fMqbzlJ6Oxw14HwERvaP22MtSPkCCirVctjyfejNKHCaIt0nHltVRjKflrWBqJ4TejQf2EDSYbrbUPOw8pU7qf633rrgs14k3F3MUDYVXw8ADoLC5Gat71cQeLzT8kZ4VOAlaEgKW16rQnbJTdII5CPNKRJ8msGvwsE",
        DATE_COL=datetime.date(2024, 12, 18),
        TIMESTAMP_COL=datetime.datetime(2024, 7, 13, 21, 57),
        TIMESTAMP_TZ_COL=datetime.datetime(
            2024, 11, 1, 1, 7, tzinfo=pytz.FixedOffset(-420)
        ),
        TIMESTAMP_LTZ_COL=pytz.timezone("America/Los_Angeles").localize(
            datetime.datetime(2024, 11, 27, 8, 57)
        ),
        BLOB_COL=bytearray(b"\xe1T\xfb\x98\xad\xa8\x1d+\xebxf\x81\x1e\xf5CZ"),
        RAW_COL=bytearray(b"\xed\x9f\x96\xa4\x15\xaaG\xfd\xac\xc2\x0f\xdc\x95oY\xb9"),
        GUID_COL=bytearray(b"3\x8d\xcc\x05D\xf6I\x0b\xe0c\x01\x00\x00\x7f\xae\xe7"),
    ),
]

sql_server_udtf_ingestion_data = [
    Row(
        ID=1,
        SMALLINTCOL=100,
        TINYINTCOL=10,
        BIGINTCOL=100000,
        DECIMALCOL=Decimal("12345.67"),
        FLOATCOL=1.23,
        REALCOL=0.4560000002384186,
        MONEYCOL=Decimal("1234.5600"),
        SMALLMONEYCOL=Decimal("12.3400"),
        CHARCOL="FixedStr1 ",
        VARCHARCOL="VarStr1",
        TEXTCOL="Text1",
        NCHARCOL="UniFix1   ",
        NVARCHARCOL="UniVar1",
        NTEXTCOL="UniText1",
        DATECOL=datetime.date(2023, 1, 1),
        TIMECOL=datetime.time(12, 0),
        DATETIMECOL=datetime.datetime(2023, 1, 1, 12, 0),
        DATETIME2COL=datetime.datetime(2023, 1, 1, 12, 0, 0, 123000),
        SMALLDATETIMECOL=datetime.datetime(2023, 1, 1, 12, 0),
        BINARYCOL=bytearray(b"\x01\x02\x03\x04\x05"),
        VARBINARYCOL=bytearray(b"\x01\x02\x03\x04"),
        BITCOL=True,
        UNIQUEIDENTIFIERCOL=bytearray(b"06D48351-6EA7-4E64-81A2-9921F0EC42A5"),
    )
]


sql_server_all_type_schema = [
    ("Id", int, None, None, 10, 0, False),
    ("SmallIntCol", int, None, None, 5, 0, True),
    ("TinyIntCol", int, None, None, 3, 0, True),
    ("BigIntCol", int, None, None, 19, None, True),
    ("DecimalCol", decimal.Decimal, None, None, 10, 2, True),
    ("FloatCol", float, None, None, 53, None, True),
    ("RealCol", float, None, None, 24, None, True),
    ("MoneyCol", decimal.Decimal, None, None, 19, 4, True),
    ("SmallMoneyCol", decimal.Decimal, None, None, 10, 4, True),
    ("CharCol", str, None, None, None, None, True),
    ("VarCharCol", str, None, None, None, None, True),
    ("TextCol", str, None, None, None, None, True),
    ("NCharCol", str, None, None, None, None, True),
    ("NVarCharCol", str, None, None, None, None, True),
    ("NTextCol", str, None, None, None, None, True),
    ("DateCol", datetime.date, None, None, None, None, True),
    ("TimeCol", datetime.time, None, None, None, None, True),
    ("DateTimeCol", datetime.datetime, None, None, None, None, True),
    ("DateTime2Col", datetime.datetime, None, None, None, None, True),
    ("SmallDateTimeCol", datetime.datetime, None, None, None, None, True),
    ("BinaryCol", bytes, None, None, None, None, True),
    ("VarBinaryCol", bytes, None, None, None, None, True),
    ("BitCol", bool, None, 1, None, None, True),
    ("UniqueIdentifierCol", bytes, None, None, None, None, True),
]

# Define the namedtuple
OracleDBType = namedtuple(
    "OracleDBType", ["name", "type_code", "precision", "scale", "null_ok"]
)


sql_server_all_type_data = [
    (
        0,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
    ),
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
        b"06D48351-6EA7-4E64-81A2-9921F0EC42A5",
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
        b"41B116E8-7D42-420B-A28A-98D53C782C79",
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
        b"F418999E-15F9-4FB0-9161-3383E0BC1B3E",
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
        b"13DF4C45-682A-4C17-81BA-7B00C77E3F9C",
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
        b"16592D8F-D876-4629-B8E5-C9C882A23C9D",
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
        b"16592D8F-D876-4629-B8E5-C9C882A23C9D",
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
        b"26592D8F-D876-4629-B8E5-C9C882A23C9D",
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
        b"36592D8F-D876-4629-B8E5-C9C882A23C9D",
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
        b"46592D8F-D876-4629-B8E5-C9C882A23C9D",
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
        b"56592D8F-D876-4629-B8E5-C9C882A23C9D",
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
        b"66592D8F-D876-4629-B8E5-C9C882A23C9D",
    ),
]

sql_server_unicode_data = [(1, "山田太郎", "日本", "これはUnicodeテストです")]
sql_server_unicode_data_schema = (
    ("编号", int, None, 10, 10, 0, False),
    ("姓名", str, None, 100, 100, 0, True),
    ("国家", str, None, 100, 100, 0, True),
    ("备注", str, None, 255, 255, 0, True),
)

sql_server_double_quoted_data = [(1, "John Doe", "USA", "Fake note")]
sql_server_double_quoted_data_schema = (
    ('"Id"', int, None, 10, 10, 0, False),
    ('"FullName"', str, None, 100, 100, 0, True),
    ('"Country"', str, None, 100, 100, 0, True),
    ('"Notes"', str, None, 255, 255, 0, True),
)
sql_server_all_type_small_data = sql_server_all_type_data[5:]


def sql_server_create_connection_unicode_data():
    return FakeConnection(
        sql_server_unicode_data, sql_server_unicode_data_schema, "pyodbc"
    )


def sql_server_create_connection_double_quoted_data():
    return FakeConnection(
        sql_server_double_quoted_data, sql_server_double_quoted_data_schema, "pyodbc"
    )


def sql_server_create_connection():
    return FakeConnection(
        sql_server_all_type_data, sql_server_all_type_schema, "pyodbc"
    )


def sql_server_create_connection_small_data():
    return FakeConnection(
        sql_server_all_type_small_data, sql_server_all_type_schema, "pyodbc"
    )


def sql_server_create_connection_empty_data():
    return FakeConnection([], sql_server_all_type_schema, "pyodbc")


def sql_server_create_connection_with_exception():
    return FakeConnectionWithException(
        sql_server_all_type_data, sql_server_all_type_schema, "pyodbc"
    )


def unknown_dbms_create_connection():
    return FakeConnection(
        sql_server_all_type_small_data, sql_server_all_type_schema, "unknown"
    )


SQLITE3_DB_CUSTOM_SCHEMA_STRING = "id INTEGER, int_col INTEGER, real_col FLOAT, text_col STRING, blob_col BINARY, null_col STRING, ts_col TIMESTAMP, date_col DATE, time_col TIME, short_col SHORT, long_col LONG, double_col DOUBLE, decimal_col DECIMAL, map_col MAP, array_col ARRAY, var_col VARIANT"
SQLITE3_DB_CUSTOM_SCHEMA_STRUCT_TYPE = StructType(
    [
        StructField("id", IntegerType()),
        StructField("int_col", IntegerType()),
        StructField("real_col", FloatType()),
        StructField("text_col", StringType()),
        StructField("blob_col", BinaryType()),
        StructField("null_col", NullType()),
        StructField("ts_col", TimestampType()),
        StructField("date_col", DateType()),
        StructField("time_col", TimeType()),
        StructField("short_col", ShortType()),
        StructField("long_col", LongType()),
        StructField("double_col", DoubleType()),
        StructField("decimal_col", DecimalType()),
        StructField("map_col", MapType()),
        StructField("array_col", ArrayType()),
        StructField("var_col", VariantType()),
    ]
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
        (
            5,
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
            "5",
        ),
        (
            6,
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
            "6",
        ),
        (
            7,
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
            "7",
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
        (
            5,
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
            '"5"',
        ),
        (
            6,
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
            '"6"',
        ),
        (
            7,
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
            '"7"',
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
