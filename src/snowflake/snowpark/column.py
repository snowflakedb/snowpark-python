from src.snowflake.snowpark.internal.sp_expressions import Expression, UnresolvedAttribute, \
    UnresolvedStar, UnresolvedAlias, NamedExpression


class Column:

    def __init__(self, expr):
        if type(expr) == str:
            self.expression = self.__get_expr(expr)
        elif isinstance(expr, Expression):
            self.expression = expr
        else:
            raise Exception("Column ctor takes only str or expression.")

    def named(self):
        if isinstance(self.expression, NamedExpression):
            return self.expression
        else:
            return UnresolvedAlias(self.expression, None)

    # TODO make subscriptable

    @classmethod
    def expr(cls, e):
        return cls(UnresolvedAttribute.quoted(e))

    @staticmethod
    def __get_expr(name):
        if name == "*":
            return UnresolvedStar(None)
        elif name.endswith('.*'):
            parts = UnresolvedAttribute.parse_attribute_name(name[0:-2])
            return UnresolvedStar(parts)
        else:
            return UnresolvedAttribute.quoted(name)

    @classmethod
    def with_expr(cls, new_expr):
        return cls(new_expr)

# TODO
# class CaseExp(Column):
#
