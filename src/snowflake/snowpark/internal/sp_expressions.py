#  File containing the Expression definitions for ASTs (Spark).
from src.snowflake.snowpark.types.types_package import _infer_type

import uuid


class Expression:
    pass


class NamedExpression(Expression):
    def __init__(self, name):
        self.name = name
        self.expr_id = uuid.uuid4()


class LeafExpression(Expression):
    pass


class Star(LeafExpression, NamedExpression):
    pass


class UnaryExpression(Expression):
    pass


class BinaryExpression(Expression):
    def __init__(self):
        self.left = None
        self.right = None


class UnresolvedFunction(Expression):
    def __init__(self, name, arguments, is_distinct=False):
        self.name = name
        self.children = arguments
        self.is_distinct = is_distinct


# Stars
class UnresolvedStar(Star):
    def __init__(self, target):
        super().__init__('UnresolvedStar')
        self.target = target


class ResolvedStar(Star):
    def __init__(self, expressions):
        super().__init__('ResolvedStar')
        self.expressions = expressions


# Named Expressions
class Alias(UnaryExpression, NamedExpression):
    def __init__(self, child, name, expr_id=None):
        super().__init__(name=name)
        self.child = child
        self.expr_id = expr_id if expr_id else uuid.uuid4()


class Attribute(LeafExpression, NamedExpression):
    def __init__(self, name):
        super().__init__(name=name)

    @classmethod
    def with_name(cls, name):
        return Attribute(name)


class UnresolvedAlias(UnaryExpression, NamedExpression):
    def __init__(self, child, alias_func):
        super().__init__(child.name)
        self.child = child
        self.alias_func = alias_func


# Leaf Expressions
class Literal(LeafExpression):
    def __init__(self, value, datatype):
        self.value = value
        self.datatype = datatype

    @classmethod
    def create(cls, value):
        return cls(value, _infer_type(value))


# Binary Expressions
class BinaryOperator(BinaryExpression):
    pass


# also inherits from Predicate, omitted
class And(BinaryOperator):
    symbol = sql_operator = 'AND'

    def __init__(self, left: Expression, right: Expression):
        assert isinstance(left, Expression)
        assert isinstance(right, Expression)
        super().__init__()
        self.left = left
        self.right = right


class BinaryComparison(BinaryOperator):
    # Note for BinaryComparison expressions we have to assign their symbol values.
    pass


class EqualTo(BinaryComparison):
    symbol = '='

    def __init__(self, left, right):
        assert isinstance(left, Expression)
        assert isinstance(right, Expression)
        super().__init__()
        self.left = left
        self.right = right


class GreaterThan(BinaryComparison):
    symbol = '>'

    def __init__(self, left, right):
        assert isinstance(left, Expression)
        assert isinstance(right, Expression)
        super().__init__()
        self.left = left
        self.right = right


class GreaterThanOrEqual(BinaryComparison):
    symbol = '>='

    def __init__(self, left, right):
        assert isinstance(left, Expression)
        assert isinstance(right, Expression)
        super().__init__()
        self.left = left
        self.right = right


# Attributes
class AttributeReference(Attribute):
    def __init__(self, name: str, data_type, nullable: bool):
        super().__init__(name)
        self.data_type = data_type
        self.nullable = nullable

    def with_name(self, new_name):
        if self.name == new_name:
            return self
        else:
            return AttributeReference(self.name, self.data_type, self.nullable)


class UnresolvedAttribute(Attribute):

    def __init__(self, name_parts):
        super().__init__(name_parts if type(name_parts) == str else name_parts[-1])
        self.name_parts = [name_parts] if type(name_parts) == str else name_parts

    #@property
    #def expr_id(self) -> None:
    #    raise Exception("UnresolvedException - expr_id")

    @classmethod
    def quoted(cls, name):
        # TODO revisit
        return cls(name)

    @classmethod
    def quoted_string(cls, name):
        # TODO revisit
        return cls(UnresolvedAttribute.parse_attribute_name(name))

    @staticmethod
    def parse_attribute_name(name):
        # TODO
        return name
