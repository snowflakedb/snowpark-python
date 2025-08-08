#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

"""
Handler functions for XPath UDFs.

These functions are registered as UDFs using register_from_file to evaluate
XPath expressions against XML strings.
"""

from typing import Optional, Any

# lxml is required for XPath evaluation
try:
    from lxml import etree
except ImportError:
    etree = None


def _xpath_evaluate_internal(
    xml_str: Optional[str], xpath_expr: Optional[str], return_type: str
) -> Any:
    """
    Internal XPath evaluation function.

    Args:
        xml_str: The XML string to evaluate against
        xpath_expr: The XPath expression to evaluate
        return_type: The type of result to return ('array', 'string', 'boolean', 'int', 'float')

    Returns:
        Result based on return_type
    """
    if etree is None:
        raise ImportError("lxml is required for XPath evaluation")

    # Handle NULL inputs according to Spark semantics
    if xml_str is None or xpath_expr is None:
        if return_type == "array":
            return []  # Return Python list for Snowflake ARRAY type
        elif return_type == "boolean":
            return False
        else:  # string, int, float
            return None

    try:
        # Parse XML with error recovery
        parser = etree.XMLParser(recover=True, encoding="utf-8")
        doc = etree.fromstring(xml_str.encode("utf-8"), parser=parser)

        # Evaluate XPath
        result = doc.xpath(xpath_expr)

        # Process results based on return type
        if return_type == "array":
            # Return all matches as array of strings
            values = []
            for r in result:
                if isinstance(r, etree._Element):
                    # For elements, return text content
                    text = "".join(r.itertext())
                    values.append(text)
                else:
                    values.append(str(r))
            return values

        elif return_type == "string":
            # Return first match as string
            if not result:
                return None
            r = result[0] if isinstance(result, list) else result
            if isinstance(r, etree._Element):
                return "".join(r.itertext())
            return str(r)

        elif return_type == "boolean":
            # XPath boolean coercion rules
            if isinstance(result, bool):
                return result
            elif isinstance(result, list):
                return len(result) > 0
            elif isinstance(result, (int, float)):
                return result != 0
            elif isinstance(result, str):
                return len(result) > 0
            return bool(result)

        elif return_type in ("int", "float"):
            # Return first match as number
            if not result:
                return None
            val = result[0] if isinstance(result, list) else result

            try:
                if isinstance(val, etree._Element):
                    text = "".join(val.itertext()).strip()
                    if return_type == "int":
                        # Try to parse as float first, then convert to int
                        # This handles cases like "1.0" which should become 1
                        return int(float(text))
                    else:
                        return float(text)
                else:
                    if return_type == "int":
                        return int(float(str(val)))
                    else:
                        return float(str(val))
            except (ValueError, TypeError):
                return None

    except Exception:
        # Handle parsing/evaluation errors
        if return_type == "array":
            return []
        elif return_type == "boolean":
            return False
        else:
            return None


# Handler functions for each return type
def xpath_array_handler(xml_str: Optional[str], xpath_expr: Optional[str]) -> list:
    """Handler function for xpath() returning array of strings."""
    return _xpath_evaluate_internal(xml_str, xpath_expr, "array")


def xpath_string_handler(
    xml_str: Optional[str], xpath_expr: Optional[str]
) -> Optional[str]:
    """Handler function for xpath_string() returning first match as string."""
    return _xpath_evaluate_internal(xml_str, xpath_expr, "string")


def xpath_boolean_handler(xml_str: Optional[str], xpath_expr: Optional[str]) -> bool:
    """Handler function for xpath_boolean() returning boolean result."""
    return _xpath_evaluate_internal(xml_str, xpath_expr, "boolean")


def xpath_int_handler(
    xml_str: Optional[str], xpath_expr: Optional[str]
) -> Optional[int]:
    """Handler function for xpath_int() returning integer result."""
    return _xpath_evaluate_internal(xml_str, xpath_expr, "int")


def xpath_float_handler(
    xml_str: Optional[str], xpath_expr: Optional[str]
) -> Optional[float]:
    """Handler function for xpath_number() and xpath_float() returning float result."""
    return _xpath_evaluate_internal(xml_str, xpath_expr, "float")
