#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All right reserved.
#

import pytest

from snowflake.snowpark.plans.logical.basic_logical_operators import Join
from snowflake.snowpark.snowpark_client_exception import SnowparkClientException
from snowflake.snowpark.types.sp_join_types import JoinType, NaturalJoin, UsingJoin


def test_mix_set_operator(session_cnx):

    with pytest.raises(SnowparkClientException) as ex_info:
        JoinType.from_string("incorrect_join_type")
    assert ex_info.value.error_code == "0116"
    assert ex_info.value.message.startswith(
        "Unsupported join type 'incorrect_join_type'"
    )

    left_semi = JoinType.from_string("left_semi")
    assert left_semi.sql == "LEFT SEMI"

    left_anti = JoinType.from_string("left_anti")
    assert left_anti.sql == "LEFT ANTI"

    with pytest.raises(SnowparkClientException) as ex_info:
        NaturalJoin(JoinType.from_string("left_semi"))
    assert ex_info.value.error_code == "0117"
    assert ex_info.value.message.startswith("Unsupported natural join type 'LeftSemi'.")
    assert NaturalJoin(JoinType.from_string("inner")).sql == "NATURAL INNER"

    with pytest.raises(SnowparkClientException) as ex_info:
        UsingJoin(JoinType.from_string("cross"), ["col1"])
    assert ex_info.value.error_code == "0118"
    assert ex_info.value.message.startswith("Unsupported using join type 'Cross'.")
    assert UsingJoin(JoinType.from_string("inner"), ["col1"]).sql == "USING INNER"

    assert Join(None, None, JoinType.from_string("Inner"), None).sql == "INNER"
