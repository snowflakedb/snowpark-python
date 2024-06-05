#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import sys
from enum import Enum

# collections.Counter does not pass type checker. Changes with appropriate type hints were made in 3.9+
if sys.version_info < (3, 9):
    import collections
    import typing

    KT = typing.TypeVar("KT")

    class Counter(collections.Counter, typing.Counter[KT]):
        pass

else:
    from collections import Counter  # noqa


class ComplexityStat(Enum):
    FILTER = "filter"
    ORDER_BY = "order_by"
    JOIN = "join"
    SET_OPERATION = "set_operation"  # UNION, EXCEPT, INTERSECT, UNION ALL
    SAMPLE = "sample"
    PIVOT = "pivot"
    UNPIVOT = "unpivot"
    WINDOW = "window"
    GROUP_BY = "group_by"
    PARTITION_BY = "partition_by"
    CASE_WHEN = "case_when"
    LITERAL = "literal"
    COLUMN = "column"
    FUNCTION = "function"
    IN = "in"
    LOW_IMPACT = "low_impact"
