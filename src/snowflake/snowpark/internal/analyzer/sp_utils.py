# See https://github.com/apache/spark/blob/1dd0ca23f64acfc7a3dc697e19627a1b74012a2d/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/util/package.scala#L128

from ..sp_expressions import Expression as SPExpression, Attribute as SPAttribute, \
    PrettyAttribute as SPPrettyAttribute, Literal as SPLiteral
from ...types.sp_data_types import StringType as SPStringType, NumericType as SPNumericType


def use_pretty_expression(e: SPExpression) -> SPExpression:
    if isinstance(e, SPAttribute):
        return SPPrettyAttribute.this(e)
    if isinstance(e, SPLiteral):
        if type(e.datatype) == SPStringType:
            return SPPrettyAttribute(str(e.value), SPStringType())
        if isinstance(e.datatype, SPNumericType):
            if e.value:
                return SPPrettyAttribute(str(e.value), SPStringType())
        if not e.value:
            return SPPrettyAttribute("NULL", e.datatype)

    # TODO
    # GetStructField
    # GetArrayStructFields
    # RuntimeReplaceable
    # CastBase


def to_pretty_sql(e: SPExpression) -> str:
    try:
        return use_pretty_expression(e).sql()
    except:
        try:
            return e.to_string()
        except:
            return e.name
