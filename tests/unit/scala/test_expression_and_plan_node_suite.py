#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import pytest

from snowflake.snowpark._internal.analyzer.binary_plan_node import (
    Join,
    LateralJoin,
    NaturalJoin,
    UsingJoin,
    create_join_type,
)
from snowflake.snowpark.exceptions import SnowparkJoinException


def test_mix_set_operator():

    with pytest.raises(SnowparkJoinException) as ex_info:
        create_join_type("incorrect_join_type")
    assert ex_info.value.error_code == "1110"
    assert ex_info.value.message.startswith(
        "Unsupported join type 'incorrect_join_type'"
    )

    left_semi = create_join_type("left_semi")
    assert left_semi.sql == "LEFT SEMI"

    left_anti = create_join_type("left_anti")
    assert left_anti.sql == "LEFT ANTI"

    with pytest.raises(SnowparkJoinException) as ex_info:
        NaturalJoin(create_join_type("left_semi"))
    assert ex_info.value.error_code == "1111"
    assert ex_info.value.message.startswith("Unsupported natural join type 'LeftSemi'.")
    assert NaturalJoin(create_join_type("inner")).sql == "NATURAL INNER"

    with pytest.raises(SnowparkJoinException) as ex_info:
        UsingJoin(create_join_type("cross"), ["col1"])
    assert ex_info.value.error_code == "1112"
    assert ex_info.value.message.startswith("Unsupported using join type 'Cross'.")
    assert UsingJoin(create_join_type("inner"), ["col1"]).sql == "USING INNER"

    assert Join(None, None, create_join_type("Inner"), None, None).sql == "INNER"

    with pytest.raises(SnowparkJoinException) as ex_info:
        LateralJoin(create_join_type("right"))
    assert ex_info.value.error_code == "1116"
    assert ex_info.value.message.startswith(
        "Unsupported lateral join type 'RightOuter'."
    )
    with pytest.raises(SnowparkJoinException) as ex_info:
        LateralJoin(create_join_type("full"))
    assert ex_info.value.error_code == "1116"
    assert ex_info.value.message.startswith(
        "Unsupported lateral join type 'FullOuter'."
    )
    with pytest.raises(SnowparkJoinException) as ex_info:
        LateralJoin(create_join_type("semi"))
    assert ex_info.value.error_code == "1116"
    assert ex_info.value.message.startswith("Unsupported lateral join type 'LeftSemi'.")
    with pytest.raises(SnowparkJoinException) as ex_info:
        LateralJoin(create_join_type("anti"))
    assert ex_info.value.error_code == "1116"
    assert ex_info.value.message.startswith("Unsupported lateral join type 'LeftAnti'.")
    with pytest.raises(SnowparkJoinException) as ex_info:
        LateralJoin(create_join_type("asof"))
    assert ex_info.value.error_code == "1116"
    assert ex_info.value.message.startswith("Unsupported lateral join type 'AsOf'.")

    assert LateralJoin(create_join_type("inner")).sql == "INNER JOIN LATERAL"
    assert LateralJoin(create_join_type("left")).sql == "LEFT OUTER JOIN LATERAL"
    assert LateralJoin(create_join_type("cross")).sql == "CROSS JOIN LATERAL"
