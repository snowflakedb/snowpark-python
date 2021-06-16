from src.snowflake.snowpark.session import Session
from src.snowflake.snowpark.dataframe import DataFrame

from decimal import Decimal


def data1(session: Session) -> DataFrame:
    return session.createDataFrame([(1, True, 'a'), (2, False, 'b')])


def data2(session: Session) -> DataFrame:
    return session.createDataFrame([[1, 1], [1, 2], [2, 1], [2, 2], [3, 1], [3, 2]]).toDF(['a', 'b'])


def data3(session: Session) -> DataFrame:
    # TODO swap order of rows
    return session.createDataFrame([[2, 2], [1, None]]).toDF(['a', 'b'])


def data4(session: Session) -> DataFrame:
    return session.createDataFrame([[i, str(i)] for i in range(1, 100)])


def lower_case_data(session: Session) -> DataFrame:
    return session.createDataFrame([[1, 'a'], [2, 'b'], [3, 'c'], [4, 'd']])


def integer1(session: Session) -> DataFrame:
    return session.sql("select * from values(1),(2),(3) as T(a)")


def decimal_data(session: Session) -> DataFrame:
    return session.createDataFrame([
        [Decimal(1), Decimal(1)],
        [Decimal(1), Decimal(2)],
        [Decimal(2), Decimal(1)],
        [Decimal(2), Decimal(2)],
        [Decimal(3), Decimal(1)],
        [Decimal(3), Decimal(2)],]).toDF(['a', 'b'])