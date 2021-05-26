from src.snowflake.snowpark.functions import builtin, col, call_builtin
from src.snowflake.snowpark.row import Row


def test_builtin_avg_from_range(session_cnx, db_parameters):
    """Tests the builtin functionality, using avg()."""
    with session_cnx(db_parameters, False, False) as session:
        session.sql('use warehouse tests_pysnowpark').collect()

        avg = builtin('avg')

        df = session.range(1, 10, 2).select(avg(col('id')))
        res = df.collect()
        expected = [Row([5.000])]
        assert res == expected

        df = session.range(1, 10, 2).filter('id>2').select(avg(col('id')))
        res = df.collect()
        expected = [Row([6.000])]
        assert res == expected

        # Add extra select on existing column
        df = session.range(1, 10, 2).select('id').filter('id>2').select(avg(col('id')))
        res = df.collect()
        expected = [Row([6.000])]
        assert res == expected

        # Add extra selects on existing column
        df = session.range(1, 10, 2).select('id').select('id').select('id').select('id'). \
            filter('id>2').select(avg(col('id')))
        res = df.collect()
        expected = [Row([6.000])]
        assert res == expected


def test_call_builtin_avg_from_range(session_cnx, db_parameters):
    """Tests the builtin functionality, using avg()."""
    with session_cnx(db_parameters, False, False) as session:
        session.sql('use warehouse tests_pysnowpark').collect()

        df = session.range(1, 10, 2).select(call_builtin('avg', col('id')))
        res = df.collect()
        expected = [Row([5.000])]
        assert res == expected

        df = session.range(1, 10, 2).filter('id>2').select(call_builtin('avg', col('id')))
        res = df.collect()
        expected = [Row([6.000])]
        assert res == expected

        # Add extra select on existing column
        df = session.range(1, 10, 2).select('id').filter('id>2').select(
            call_builtin('avg', col('id')))
        res = df.collect()
        expected = [Row([6.000])]
        assert res == expected

        # Add extra selects on existing column
        df = session.range(1, 10, 2).select('id').select('id').select('id').select('id'). \
            filter('id>2').select(call_builtin('avg', col('id')))
        res = df.collect()
        expected = [Row([6.000])]
        assert res == expected
