#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All right reserved.
#

from test.utils import TestData

from snowflake.snowpark.functions import col, min, sum
from snowflake.snowpark.row import Row


def test_union_all(session_cnx, db_parameters):
    with session_cnx(db_parameters) as session:
        td4 = TestData.test_data4(session)
        union_df = td4.union(td4).union(td4).union(td4).union(td4)

        res = union_df.agg([min(col("key")), sum(col("key"))]).collect()
        assert res == [Row([1, 25250])]

        # unionAll is an alias of union
        union_all_df = td4.unionAll(td4).unionAll(td4).unionAll(td4).unionAll(td4)
        res1 = union_df.collect()
        res2 = union_all_df.collect()
        # don't sort
        assert res1 == res2
