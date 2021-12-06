from typing import Any, List

class JoinType:
    sql: Any
    @classmethod
    def from_string(cls, join_type: str) -> JoinType: ...

class InnerLike(JoinType): ...

class Inner(InnerLike):
    sql: str

class Cross(InnerLike):
    sql: str

class LefOuter(JoinType):
    sql: str

class RightOuter(JoinType):
    sql: str

class FullOuter(JoinType):
    sql: str

class LeftSemi(JoinType):
    sql: str

class LeftAnti(JoinType):
    sql: str

class NaturalJoin(JoinType):
    tpe: Any
    sql: Any
    def __init__(self, tpe: JoinType) -> None: ...

class UsingJoin(JoinType):
    sql: Any
    tpe: Any
    using_columns: Any
    def __init__(self, tpe: JoinType, using_columns: List[str]) -> None: ...
