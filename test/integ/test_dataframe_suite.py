from src.snowflake.snowpark.row import Row
from ..utils import Utils

import pytest

def test_null_data_in_tables(session_cnx, db_parameters):
    with session_cnx(db_parameters) as session:
        table_name = Utils.random_name()
        Utils.create_table(session, table_name, "num int")
        session.sql(f"insert into {table_name} values(null),(null),(null)")

@pytest.mark.skip(reason='requires is_null, sort, createDataFrame type inference')
def test_null_data_in_local_relation_with_filters(session_cnx, db_parameters):
    with session_cnx(db_parameters) as session:
        df = session.createDataFrame([[1, None], [2, 'NotNull'], [3, None]]).toDF(['a','b'])
        assert df.collect() == [Row([1, None]), Row([2, 'NotNull']), Row([3, None])]
        df2 = session.createDataFrame([[1, None], [2, 'NotNull'], [3, None]]).toDF(['a','b'])
        assert df.collect() == df2.collect()

        assert df.filter(is_null(col('b'))).collect() == [Row([1,None]), Row([3,None])]
        assert df.filter(!is_null(col('b'))).collect() == [Row([2, 'NotNull'])]
        assert df.sort(col('b').asc_nulls_last).collect() == \
               [Row([2, 'NotNull']), Row([1, None]), Row([3, None])]

