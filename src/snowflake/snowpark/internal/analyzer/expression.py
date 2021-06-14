from ..sp_expressions import Expression as SPExpression, UnaryExpression as SPUnaryExpression
from ...types.sf_types import DataType

from typing import Optional, List


class Expression(SPExpression):
    def __init__(self, child: Optional[SPExpression]):
        self.child = child
        self.children = [child]

    def nullable(self):
        return True

    def datatype(self):
        return self.child.datatype


class Like(Expression):
    def __init__(self, expr: SPExpression, pattern: SPExpression):
        super().__init__(expr)
        self.expr = expr
        self.pattern = pattern


class RegExp(Expression):
    def __init__(self, expr: SPExpression, pattern: SPExpression):
        super().__init__(expr)
        self.expr = expr
        self.pattern = pattern


class Collate(Expression):
    def __init__(self, expr: SPExpression, collationSpec: str):
        super().__init__(expr)
        self.expr = expr
        self.collationSpec = collationSpec


class SubfieldString(Expression):
    def __init__(self, expr: SPExpression, field: str):
        super().__init__(expr)
        self.expr = expr
        self.field = field


class SubfieldInt(Expression):
    def __init__(self, expr: SPExpression, field: int):
        super().__init__(expr)
        self.expr = expr
        self.field = field


class TableFunctionExpression(Expression):
    def __init__(self):
        super().__init__(None)
        self.datatype = None


class Cast(SPUnaryExpression):
    def __init__(self, child: SPExpression, to: DataType):
        self.child = child
        self.children = [child]
        self.to = to
        self.datatype = child.datatype


class UnaryMinus(SPUnaryExpression):
    def __init__(self, child: SPExpression):
        self.child = child
        self.children = [child]
        self.datatype = child.datatype


class FlattenFunction(TableFunctionExpression):
    def __init__(self, input: SPExpression, path: str, outer: bool, recursive: bool, mode: str):
        super.__init__(child=None)
        self.input = input
        self.path = path
        self.outer = outer
        self.recursive = recursive
        self.mode = mode


class TableFunction(TableFunctionExpression):
    def __init__(self, func_name: str, args: List[SPExpression]):
        super.__init__(child=None)
        self.func_name = func_name
        self.args = args


class NamedArgumentsTableFunction(TableFunctionExpression):
    def __init__(self, func_name: str, args: List[SPExpression]):
        super.__init__(child=None)
        self.func_name = func_name
        self.args = args


class GroupingSets(Expression):
    def __init__(self, args: List[SPExpression]):
        super.__init__(child=None)
        self.args = args
        self.datatype = None


class WithinGroup(Expression):
    def __init__(self, expr: SPExpression, order_by_cols: List[SPExpression]):
        super.__init__(child=expr)
        self.expr = expr
        self.order_by_cols = order_by_cols

