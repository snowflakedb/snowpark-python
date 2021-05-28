#  File containing the Expression definitions for ASTs (Spark).
from src.snowflake.snowpark.types.types_package import _infer_type


class Expression:
    pass

class Star(Expression):
    pass

class NamedExpression(Expression):
    def __init__(self, name):
        self.name = name


class UnaryExpression(Expression):
    pass


class LeafExpression(Expression):
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
    def __init__(self, name):
        self.name = name


# Named Expressions
class Alias(UnaryExpression, NamedExpression):
    def __init__(self, child, name):
        super().__init__(name=name)
        self.child = child


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


class BinaryComparison(BinaryOperator):
    #Note for BinaryComparison expression we have to assign its symbol value.
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


class UnresolvedAttribute(Attribute):

    def __init__(self, name_parts):
        super().__init__(name_parts if type(name_parts) == str else name_parts[-1])
        self.name_parts = [name_parts] if type(name_parts) == str else name_parts

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
