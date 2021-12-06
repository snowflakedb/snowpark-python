from typing import Any, Optional

class LogicalPlan:
    children: Any
    def __init__(self) -> None: ...

class LeafNode(LogicalPlan): ...

class BinaryNode(LogicalPlan):
    left: LogicalPlan
    right: LogicalPlan

class NamedRelation(LogicalPlan): ...

class UnresolvedRelation(LeafNode, NamedRelation):
    multipart_identifier: Any
    def __init__(self, multipart_identifier) -> None: ...

class UnaryNode(LogicalPlan): ...
class OrderPreservingUnaryNode(UnaryNode): ...

class Project(OrderPreservingUnaryNode):
    project_list: Any
    child: Any
    resolved: bool
    def __init__(self, project_list, child) -> None: ...

class Filter(OrderPreservingUnaryNode):
    condition: Any
    child: Any
    def __init__(self, condition, child) -> None: ...

class Sample(UnaryNode):
    probability_fraction: Any
    row_count: Any
    child: Any
    def __init__(
        self,
        child: LogicalPlan,
        probability_fraction: Optional[float] = ...,
        row_count: Optional[int] = ...,
    ) -> None: ...
