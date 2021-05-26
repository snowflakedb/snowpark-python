from .column import Column
from .internal.sp_expressions import Expression as SPExpression, Literal as SPLiteral, \
    UnresolvedFunction as SPUnresolvedFunction


def col(col_name):
    """Returns the [[Column]] with the specified name. """
    return Column(col_name)


def column(col_name):
    """Returns a [[Column]] with the specified name. Alias for col. """
    return Column(col_name)


def call_builtin(function_name, *args):
    """Invokes a built-in snowflake function with the specified name and arguments.
        Arguments can be of two types
        a. [[Column]], or
        b. Basic types such as Int, Long, Double, Decimal etc. which are converted to Snowpark
        literals.
    """

    sp_expressions = []
    for arg in args:
        if type(arg) == Column:
            sp_expressions.append(arg.expression)
        elif isinstance(arg, SPExpression):
            sp_expressions.append(arg)
        else:
            sp_expressions.append(SPLiteral.create(arg))

    return Column(SPUnresolvedFunction(function_name, sp_expressions, is_distinct=False))


def builtin(function_name, *args):
    """
    Function object to invoke a Snowflake builtin. Use this to invoke
   * any builtins not explicitly listed in this object.
   *
   * Example
   * {{{
   *    val repeat = functions.builtin("repeat")
   *    df.select(repeat(col("col_1"), 3))
   * }}}
    :param function_name: Name of built-in Snowflake function
    :param args: arguments to function_name
    :return: Column
    """
    return lambda args: call_builtin(function_name, args)
