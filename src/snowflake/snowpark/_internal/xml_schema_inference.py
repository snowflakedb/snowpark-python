#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import re
import random
from datetime import datetime, date
from typing import Optional, Dict, List

from snowflake.snowpark._internal.xml_reader import (
    DEFAULT_CHUNK_SIZE,
    find_next_opening_tag_pos,
    tag_is_self_closing,
    find_next_closing_tag_pos,
    strip_xml_namespaces,
    replace_entity,
)
from snowflake.snowpark.files import SnowflakeFile
from snowflake.snowpark.types import (
    StructType,
    ArrayType,
    DataType,
    NullType,
    StringType,
    BooleanType,
    LongType,
    DoubleType,
    DecimalType,
    DateType,
    TimestampType,
    StructField,
)

# lxml is only a dev dependency so use try/except to import it if available
try:
    import lxml.etree as ET

    lxml_installed = True
except ImportError:
    import xml.etree.ElementTree as ET

    lxml_installed = False


# ---------------------------------------------------------------------------
# Stage 1 – Per-record type inference
# ---------------------------------------------------------------------------


def _normalize_text(
    text: Optional[str], ignore_surrounding_whitespace: bool
) -> Optional[str]:
    """Normalize text by stripping whitespace if configured."""
    if text is None:
        return None
    return text.strip() if ignore_surrounding_whitespace else text


def _infer_primitive_type(text: str) -> DataType:
    """
    Infer the DataType from a single string value with below priority order:
    - null/empty -> NullType
    - parseable as Long -> LongType
    - parseable as Double -> DoubleType
    - "true"/"false" -> BooleanType
    - parseable as Date -> DateType (ISO format: yyyy-mm-dd)
    - parseable as Timestamp -> TimestampType (ISO format)
    - anything else -> StringType
    """
    # Long (matching Spark: infers integers as LongType directly)
    try:
        sign_safe = text.lstrip("+-")
        if sign_safe and sign_safe[0].isdigit() and "." not in text:
            val = int(text)
            # Spark's Long is 64-bit; Python's int is unbounded
            if -(2**63) <= val <= 2**63 - 1:
                return LongType()
            # Numbers outside Long range fall through to Double
    except (ValueError, OverflowError):
        pass

    # Double
    try:
        sign_safe = text.lstrip("+-")
        is_numeric_start = sign_safe and (sign_safe[0].isdigit() or sign_safe[0] == ".")
        is_special_float = text.lower() in (
            "nan",
            "infinity",
            "+infinity",
            "-infinity",
            "inf",
            "+inf",
            "-inf",
        )
        if is_numeric_start or is_special_float:
            # Reject strings ending in d/D/f/F
            if text[-1] not in ("d", "D", "f", "F"):
                float(text)
                return DoubleType()
    except (ValueError, OverflowError):
        pass

    # Boolean
    if text.lower() in ("true", "false"):
        return BooleanType()

    # Date (Spark default pattern: yyyy-MM-dd via DateFormatter)
    try:
        date.fromisoformat(text)
        return DateType()
    except (ValueError, TypeError):
        pass

    # Timestamp (Spark default: TimestampFormatter with CAST logic)
    # Backward compatibility: Python 3.9 fromisoformat doesn't support 'Z' suffix; replace with +00:00
    try:
        ts_text = text.replace("Z", "+00:00") if text.endswith("Z") else text
        datetime.fromisoformat(ts_text)
        return TimestampType()
    except (ValueError, TypeError):
        pass

    return StringType()


def infer_type(
    value: Optional[str],
    ignore_surrounding_whitespace: bool = False,
    null_value: str = "",
) -> DataType:
    """
    Infer the DataType from a single string value.
    Normalizes *value*, checks for null / empty / null_value, then delegates
    to :func:`_infer_primitive_type`.
    """
    text = _normalize_text(value, ignore_surrounding_whitespace)
    if text is None or text == null_value or text == "":
        return NullType()
    return _infer_primitive_type(text)


def infer_element_schema(
    element: ET.Element,
    attribute_prefix: str = "_",
    exclude_attributes: bool = False,
    value_tag: str = "_VALUE",
    null_value: str = "",
    ignore_surrounding_whitespace: bool = False,
    ignore_namespace: bool = False,
    is_root: bool = True,
) -> DataType:
    """
    Infer the schema (DataType) from a parsed XML Element.
      - Elements with no children and no attributes (or excluded) -> infer from text (primitive/NullType)
      - Elements with children -> StructType with child field types
      - Elements with attributes -> StructType with attribute fields
      - Mixed content (text + children/attributes) -> StructType with _VALUE field
      - Repeated child tags -> ArrayType detection via add_or_update_type

    is_root: True for the row-tag element (top-level), False for child elements.
      Spark treats these differently: at root level, self-closing attribute-only
      elements do NOT get _VALUE. At child level, they always get _VALUE
      (NullType -> StringType after canonicalization).
    """
    children = list(element)
    has_attributes = bool(element.attrib) and not exclude_attributes

    # Case: leaf element with no attributes -> infer from text content
    if not children and not has_attributes:
        return infer_type(element.text, ignore_surrounding_whitespace, null_value)

    # This element will become a StructType
    # Use a dict to track field names and types (for array detection)
    name_to_type: Dict[str, DataType] = {}
    field_order: List[str] = []

    # Process attributes first
    if has_attributes:
        for attr_name, attr_value in element.attrib.items():
            prefixed_name = f"{attribute_prefix}{attr_name}"
            attr_type = infer_type(
                attr_value, ignore_surrounding_whitespace, null_value
            )
            if prefixed_name not in name_to_type:
                field_order.append(prefixed_name)
            add_or_update_type(name_to_type, prefixed_name, attr_type, value_tag)

    if children:
        for child in children:
            child_tag = child.tag
            # Ignore namespace in tag if configured (both Clark and prefix notation)
            if ignore_namespace:
                if "}" in child_tag:
                    child_tag = child_tag.split("}", 1)[1]
                elif ":" in child_tag:
                    child_tag = child_tag.split(":", 1)[1]

            # Check if child has attributes
            child_has_attrs = bool(child.attrib) and not exclude_attributes

            # inferField dispatch
            child_children = list(child)
            if not child_children and not child_has_attrs:
                # Leaf element
                child_type = infer_type(
                    child.text, ignore_surrounding_whitespace, null_value
                )
            else:
                # Non-leaf element: recurse into inferObject
                child_type = infer_element_schema(
                    child,
                    attribute_prefix=attribute_prefix,
                    exclude_attributes=exclude_attributes,
                    value_tag=value_tag,
                    null_value=null_value,
                    ignore_surrounding_whitespace=ignore_surrounding_whitespace,
                    ignore_namespace=ignore_namespace,
                    is_root=False,
                )

            # When child_has_attrs is True, the recursive infer_element_schema call
            # above already processes child attributes and includes them in the
            # returned StructType. No additional attribute processing needed here.
            if child_tag not in name_to_type:
                field_order.append(child_tag)
            add_or_update_type(name_to_type, child_tag, child_type, value_tag)

        # Handle mixed content: text + child elements
        text = _normalize_text(element.text, ignore_surrounding_whitespace)
        if text is not None and text != null_value and text.strip() != "":
            text_type = _infer_primitive_type(text)
            if value_tag not in name_to_type:
                field_order.append(value_tag)
            add_or_update_type(name_to_type, value_tag, text_type, value_tag)
    else:
        # No children but has attributes -> conditionally include _VALUE.
        # [SPARK PARITY] Spark's behavior differs by element level:
        #   - Root/row-tag level: _VALUE only added when actual text exists
        #   - Child level: _VALUE always added (NullType if no text, canonicalized
        #     to StringType later). This covers cases like self-closing
        #     <edition year="2023" format="Hardcover"/>.
        text = _normalize_text(element.text, ignore_surrounding_whitespace)
        if text is not None and text != null_value and text != "":
            text_type = _infer_primitive_type(text)
            if value_tag not in name_to_type:
                field_order.append(value_tag)
            add_or_update_type(name_to_type, value_tag, text_type, value_tag)
        elif not is_root:
            # Child-level attribute-only element: add _VALUE as NullType
            if value_tag not in name_to_type:
                field_order.append(value_tag)
            add_or_update_type(name_to_type, value_tag, NullType(), value_tag)

    # Build the StructType with sorted fields to match Spark behavior.
    result_fields = sorted(
        (StructField(name, name_to_type[name], nullable=True) for name in field_order),
        key=lambda f: f.name,
    )
    return StructType(result_fields)


# ---------------------------------------------------------------------------
# Stage 2 – Cross-partition merge
# ---------------------------------------------------------------------------


def compatible_type(t1: DataType, t2: DataType, value_tag: str = "_VALUE") -> DataType:
    """
    Returns the most general data type for two given data types.
    """
    # Same type
    if type(t1) == type(t2):
        if isinstance(t1, StructType):
            return merge_struct_types(t1, t2, value_tag)
        if isinstance(t1, ArrayType):
            return ArrayType(
                compatible_type(t1.element_type, t2.element_type, value_tag),
            )
        if isinstance(t1, DecimalType):
            # Widen decimal
            scale = max(t1.scale, t2.scale)
            range_ = max(t1.precision - t1.scale, t2.precision - t2.scale)
            if range_ + scale > 38:
                return DoubleType()
            return DecimalType(range_ + scale, scale)
        return t1

    # NullType + T -> T
    if isinstance(t1, NullType):
        return t2
    if isinstance(t2, NullType):
        return t1

    # Numeric widening: Long < Double
    if (isinstance(t1, LongType) and isinstance(t2, DoubleType)) or (
        isinstance(t1, DoubleType) and isinstance(t2, LongType)
    ):
        return DoubleType()

    # Double + Decimal -> Double
    if (isinstance(t1, DoubleType) and isinstance(t2, DecimalType)) or (
        isinstance(t1, DecimalType) and isinstance(t2, DoubleType)
    ):
        return DoubleType()

    # Long + Decimal -> Decimal (widened)
    if isinstance(t1, LongType) and isinstance(t2, DecimalType):
        # DecimalType.forType(LongType) in Spark is Decimal(20, 0)
        return compatible_type(DecimalType(20, 0), t2, value_tag)
    if isinstance(t1, DecimalType) and isinstance(t2, LongType):
        return compatible_type(t1, DecimalType(20, 0), value_tag)

    # Timestamp + Date -> Timestamp
    if (isinstance(t1, TimestampType) and isinstance(t2, DateType)) or (
        isinstance(t1, DateType) and isinstance(t2, TimestampType)
    ):
        return TimestampType()

    # Array + non-Array -> ArrayType(compatible)
    if isinstance(t1, ArrayType):
        return ArrayType(compatible_type(t1.element_type, t2, value_tag))
    if isinstance(t2, ArrayType):
        return ArrayType(compatible_type(t1, t2.element_type, value_tag))

    # Struct with _VALUE tag + Primitive -> widen _VALUE field
    if isinstance(t1, StructType) and _struct_has_value_tag(t1, value_tag):
        return _merge_struct_with_primitive(t1, t2, value_tag)
    if isinstance(t2, StructType) and _struct_has_value_tag(t2, value_tag):
        return _merge_struct_with_primitive(t2, t1, value_tag)

    # Fallback: anything else -> StringType
    return StringType()


def _struct_has_value_tag(st: StructType, value_tag: str) -> bool:
    """Check if a StructType has a field with the given value_tag name."""
    return any(f.name == value_tag for f in st.fields)


def _merge_struct_with_primitive(
    st: StructType, primitive: DataType, value_tag: str
) -> StructType:
    """
    Merge a StructType containing a value_tag field with a primitive type.
    The value_tag field's type is widened to be compatible with the primitive.
    """
    new_fields = []
    for f in st.fields:
        if f.name == value_tag:
            new_type = compatible_type(f.datatype, primitive, value_tag)
            new_fields.append(StructField(f.name, new_type, nullable=True))
        else:
            new_fields.append(f)
    return StructType(new_fields)


def merge_struct_types(
    a: StructType, b: StructType, value_tag: str = "_VALUE"
) -> StructType:
    """
    Merge two StructTypes field-by-field (case-sensitive).
    Fields present in both are merged via compatible_type.
    Fields present in only one are included as-is (nullable).

    Uses f._name (original case from XML) rather than f.name (uppercased by ColumnIdentifier).
    """
    field_map: Dict[str, DataType] = {}
    field_order: List[str] = []

    for f in a.fields:
        field_map[f._name] = f.datatype
        field_order.append(f._name)

    for f in b.fields:
        if f._name in field_map:
            field_map[f._name] = compatible_type(
                field_map[f._name], f.datatype, value_tag
            )
        else:
            field_map[f._name] = f.datatype
            field_order.append(f._name)

    # Sort fields by name to match Spark behavior
    return StructType(
        sorted(
            (StructField(name, field_map[name], nullable=True) for name in field_order),
            key=lambda f: f._name,
        ),
    )


def add_or_update_type(
    name_to_type: Dict[str, DataType],
    field_name: str,
    new_type: DataType,
    value_tag: str = "_VALUE",
) -> None:
    """
    Array detection logic:
    - 1st occurrence of field_name -> store the type as-is
    - 2nd occurrence -> wrap into ArrayType(compatible(old, new))
    - Nth occurrence -> the existing type is already ArrayType, merge via compatible_type
    """
    if field_name in name_to_type:
        old_type = name_to_type[field_name]
        if not isinstance(old_type, ArrayType):
            # 2nd occurrence: promote to ArrayType
            name_to_type[field_name] = ArrayType(
                compatible_type(old_type, new_type, value_tag),
            )
        else:
            # Already an ArrayType: merge element types
            name_to_type[field_name] = compatible_type(old_type, new_type, value_tag)
    else:
        name_to_type[field_name] = new_type


# ---------------------------------------------------------------------------
# Stage 3 – Canonicalization
# ---------------------------------------------------------------------------


def canonicalize_type(dt: DataType) -> Optional[DataType]:
    """
    Convert NullType to StringType and remove StructTypes with no fields:
      - NullType -> StringType
      - Empty StructType -> None
      - ArrayType -> recurse on element type
      - StructType -> recurse, remove empty-name fields
      - Other -> kept as-is
    """
    if isinstance(dt, NullType):
        return StringType()

    if isinstance(dt, ArrayType):
        canonical_element = canonicalize_type(dt.element_type)
        if canonical_element is not None:
            return ArrayType(canonical_element)
        return None

    if isinstance(dt, StructType):
        canonical_fields = []
        for f in dt.fields:
            if f._name == "":
                continue
            canonical_child = canonicalize_type(f.datatype)
            if canonical_child is not None:
                canonical_fields.append(
                    StructField(f._name, canonical_child, nullable=True)
                )
        if canonical_fields:
            return StructType(canonical_fields)
        # empty structs should be deleted
        return None

    return dt


# ---------------------------------------------------------------------------
# Schema string serialization
# ---------------------------------------------------------------------------


def _case_preserving_simple_string(dt: DataType) -> str:
    """
    Serialize a DataType to a simple string, preserving original field name case.
    Wrap field names with colons in double quotes so that the schema string parser
    can correctly find the top-level colon separating name from type.
    The consumer of these strings must strip the outer quotes after parsing.
    """
    if isinstance(dt, StructType):
        parts = []
        for f in dt.fields:
            name = f._name
            # Wrap names with colons in double quotes so the parser can
            # distinguish the name:type separator from colons in the name.
            if ":" in name:
                name = f'"{name}"'
            parts.append(f"{name}:{_case_preserving_simple_string(f.datatype)}")
        return f"struct<{','.join(parts)}>"
    elif isinstance(dt, ArrayType):
        return f"array<{_case_preserving_simple_string(dt.element_type)}>"
    else:
        return dt.simple_string()


# ---------------------------------------------------------------------------
# XMLSchemaInference UDTF
# ---------------------------------------------------------------------------


def infer_schema_for_xml_range(
    file_path: str,
    row_tag: str,
    approx_start: int,
    approx_end: int,
    sampling_ratio: float,
    ignore_namespace: bool,
    attribute_prefix: str,
    exclude_attributes: bool,
    value_tag: str,
    null_value: str,
    charset: str,
    ignore_surrounding_whitespace: bool,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
) -> Optional[StructType]:
    """
    Infer the merged XML schema for all records within a byte range.

    Scans the file from *approx_start* to *approx_end*, parses each
    XML record delimited by *row_tag*, infers per-record schemas, and
    merges them into a single StructType.

    Returns:
        The merged StructType, or None if no records were found.
    """
    tag_start_1 = f"<{row_tag}>".encode()
    tag_start_2 = f"<{row_tag} ".encode()
    closing_tag = f"</{row_tag}>".encode()

    merged_schema: Optional[StructType] = None

    with SnowflakeFile.open(file_path, "rb", require_scoped_url=False) as f:
        f.seek(approx_start)

        while True:
            try:
                open_pos = find_next_opening_tag_pos(
                    f, tag_start_1, tag_start_2, approx_end, chunk_size
                )
            except EOFError:
                break

            if open_pos >= approx_end:
                break

            record_start = open_pos
            f.seek(record_start)

            try:
                is_self_close, tag_end = tag_is_self_closing(f, chunk_size)
                if is_self_close:
                    record_end = tag_end
                else:
                    f.seek(tag_end)
                    record_end = find_next_closing_tag_pos(f, closing_tag, chunk_size)
            except Exception:
                try:
                    f.seek(min(record_start + 1, approx_end))
                except Exception:
                    break
                continue

            if sampling_ratio < 1.0 and random.random() > sampling_ratio:
                if record_end > approx_end:
                    break
                try:
                    f.seek(min(record_end, approx_end))
                except Exception:
                    break
                continue

            try:
                f.seek(record_start)
                record_bytes = f.read(record_end - record_start)
                record_str = record_bytes.decode(charset, errors="replace")
                record_str = re.sub(r"&(\w+);", replace_entity, record_str)

                if lxml_installed:
                    recover = bool(":" in row_tag)
                    parser = ET.XMLParser(recover=recover, ns_clean=True)
                    try:
                        element = ET.fromstring(record_str, parser)
                    except ET.XMLSyntaxError:
                        if ignore_namespace:
                            cleaned = re.sub(r"\s+(\w+):(\w+)=", r" \2=", record_str)
                            element = ET.fromstring(cleaned, parser)
                        else:
                            raise
                else:
                    element = ET.fromstring(record_str)

                if ignore_namespace:
                    element = strip_xml_namespaces(element)
            except Exception:
                if record_end > approx_end:
                    break
                try:
                    f.seek(min(record_end, approx_end))
                except Exception:
                    break
                continue

            record_schema = infer_element_schema(
                element,
                attribute_prefix=attribute_prefix,
                exclude_attributes=exclude_attributes,
                value_tag=value_tag,
                null_value=null_value,
                ignore_surrounding_whitespace=ignore_surrounding_whitespace,
                ignore_namespace=ignore_namespace,
            )

            if not isinstance(record_schema, StructType):
                record_schema = StructType(
                    [StructField(value_tag, record_schema, nullable=True)],
                )

            if merged_schema is None:
                merged_schema = record_schema
            else:
                merged_schema = merge_struct_types(
                    merged_schema, record_schema, value_tag
                )

            if record_end > approx_end:
                break
            try:
                f.seek(min(record_end, approx_end))
            except Exception:
                break

    return merged_schema


class XMLSchemaInference:
    """
    UDTF handler for parallelized XML schema inference.

    Each worker reads its assigned byte range of the XML file, parses
    XML records, infers a per-record schema, and merges all schemas
    within its partition. The merged schema is yielded as a serialized
    string (StructType.simple_string()).

    The parallelization pattern mirrors XMLReader.
    """

    def process(
        self,
        filename: str,
        num_workers: int,
        row_tag: str,
        i: int,
        sampling_ratio: float,
        ignore_namespace: bool,
        attribute_prefix: str,
        exclude_attributes: bool,
        value_tag: str,
        null_value: str,
        charset: str,
        ignore_surrounding_whitespace: bool,
        file_size: int,
    ):
        """
        Infer XML schema for a byte-range partition of the file.

        Args:
            filename: Path to the XML file.
            num_workers: Total number of workers.
            row_tag: The tag that delimits records.
            i: This worker's ID (0-based).
            sampling_ratio: Fraction of records to sample (0.0-1.0).
            ignore_namespace: Whether to strip namespaces.
            attribute_prefix: Prefix for attribute names.
            exclude_attributes: Whether to exclude attributes.
            value_tag: Tag name for the value column.
            null_value: Value to treat as null.
            charset: Character encoding of the XML file.
            ignore_surrounding_whitespace: Whether to strip whitespace from values.
            file_size: Size of the file in bytes (provided by the client via LS).
        """
        if not file_size or file_size <= 0:
            yield ("",)
            return

        if num_workers is None or num_workers <= 0:
            num_workers = 1
        if i is None or i < 0:
            i = 0
        if i >= num_workers:
            yield ("",)
            return

        approx_chunk_size = file_size // num_workers
        approx_start = approx_chunk_size * i
        approx_end = approx_chunk_size * (i + 1) if i < num_workers - 1 else file_size

        # Deterministic per-worker seed for Bernoulli sampling:
        if sampling_ratio < 1.0:
            random.seed(1 + i)

        merged_schema = infer_schema_for_xml_range(
            file_path=filename,
            row_tag=row_tag,
            approx_start=approx_start,
            approx_end=approx_end,
            sampling_ratio=sampling_ratio,
            ignore_namespace=ignore_namespace,
            attribute_prefix=attribute_prefix,
            exclude_attributes=exclude_attributes,
            value_tag=value_tag,
            null_value=null_value,
            charset=charset,
            ignore_surrounding_whitespace=ignore_surrounding_whitespace,
        )

        yield (
            _case_preserving_simple_string(merged_schema)
            if merged_schema is not None
            else "",
        )
