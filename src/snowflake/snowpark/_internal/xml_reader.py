#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import os
import re
import html.entities
import struct
import copy
import random

from typing import Optional, Dict, Any, Iterator, BinaryIO, Union, Tuple
from datetime import datetime, date, time
from snowflake.snowpark._internal.analyzer.analyzer_utils import unquote_if_quoted
from snowflake.snowpark._internal.type_utils import type_string_to_type_object
from snowflake.snowpark.files import SnowflakeFile
from snowflake.snowpark.types import (
    StructType,
    ArrayType,
    DataType,
    MapType,
    NullType,
    StringType,
    BooleanType,
    IntegerType,
    LongType,
    DoubleType,
    DecimalType,
    DateType,
    TimestampType,
    TimeType,
    StructField,
)


_DECIMAL_RE = re.compile(r"^[+-]?(?:\d+(?:\.\d*)?|\.\d+)$")

# lxml is only a dev dependency so use try/except to import it if available
try:
    import lxml.etree as ET

    lxml_installed = True
except ImportError:
    import xml.etree.ElementTree as ET

    lxml_installed = False


DEFAULT_CHUNK_SIZE: int = 1024
VARIANT_COLUMN_SIZE_LIMIT: int = 16 * 1024 * 1024


def replace_entity(match: re.Match) -> str:
    """
    Replaces an HTML entity with its corresponding character, except for predefined XML entities.

    Args:
        match (re.Match): A match object containing the entity to be replaced.

    Returns:
        str: The corresponding character if the entity is recognized, otherwise the original entity string.
    """
    entity = match.group(1)
    # XML predefined entities that must remain as is.
    predefined = {"amp", "lt", "gt", "apos", "quot"}

    if entity in predefined:
        # Leave these untouched.
        return match.group(0)
    elif entity == "xxe":
        # Replace 'xxe' with an empty string.
        return ""
    elif entity in html.entities.name2codepoint:
        # Replace with the actual character.
        return chr(html.entities.name2codepoint[entity])
    else:
        # For any entity we don't recognize, leave it unchanged.
        return match.group(0)


def schema_string_to_result_dict_and_struct_type(schema_string: str) -> Optional[dict]:
    if schema_string == "":
        return None
    schema = type_string_to_type_object(schema_string)

    return struct_type_to_result_template(schema)


def struct_type_to_result_template(dt: DataType) -> Optional[dict]:
    if isinstance(dt, StructType):
        out: Dict[str, Any] = {}
        for f in dt.fields:
            out[unquote_if_quoted(f.name)] = struct_type_to_result_template(f.datatype)
        return out

    if isinstance(dt, ArrayType) and dt.element_type is not None:
        return struct_type_to_result_template(dt.element_type)

    if isinstance(dt, MapType) and dt.value_type is not None:
        return struct_type_to_result_template(dt.value_type)

    return None


def get_file_size(filename: str) -> Optional[int]:
    """
    Get the size of a file using a file object without reading its content.
    """
    with SnowflakeFile.open(filename, "rb", require_scoped_url=False) as file_obj:
        file_obj.seek(0, os.SEEK_END)
        return file_obj.tell()


def tag_is_self_closing(
    file_obj: Union[BinaryIO, SnowflakeFile],
    chunk_size: int = DEFAULT_CHUNK_SIZE,
) -> Tuple[bool, int]:
    """
    Return ``(is_self_closing, end_pos)`` after searching the terminating ``>`` .
    ``end_pos`` is the byte offset one past the terminating ``>`` of that
    same tag.
    This method is quote-aware and will not consider a ``>`` inside a quote, e.g.,
    ``<book title="a<b>c">``.
    Note that There is no back‑slash escaping (\") inside XML attribute values.
    If we want to embed a double‑quote inside a double‑quoted attribute, we must use the entity ``&quot;``.

    Note that this function will change the position of file pointer, which is expected for next processing
    operation in ``process_xml_range``.
    """
    in_quote = False
    quote_char = None
    last_byte = b""

    while True:
        chunk_start_pos = file_obj.tell()
        chunk = file_obj.read(chunk_size)
        if not chunk:
            raise EOFError("Reached end of file but the tag is not closed")

        for idx, b in enumerate(struct.unpack(f"{len(chunk)}c", chunk)):
            # '>' inside quote should not be considered as the end of the tag
            if not in_quote and b in [b'"', b"'"]:
                in_quote = True
                quote_char = b
            elif in_quote and b == quote_char:
                in_quote = False
                quote_char = None
            # '>' outside quotes
            elif b == b">" and not in_quote:
                is_self = last_byte == b"/"
                absolute_pos = chunk_start_pos + idx + 1
                return is_self, absolute_pos
            last_byte = b


def find_next_closing_tag_pos(
    file_obj: Union[BinaryIO, SnowflakeFile],
    closing_tag: bytes,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
) -> int:
    """
    Efficiently finds the next closing tag position by reading chunks of data.
    It searches for both self-closing tags (b"/>") and normal closing tags,
    and returns the position immediately after the earliest occurrence.

    Note that this function will change the position of file pointer, which is expected for next processing
    operation in ``process_xml_range``.

    Args:
        file_obj (BinaryIO): Binary file object to read from.
        closing_tag (bytes): The closing tag to search for (e.g., b"</book>").
        chunk_size (int): Size of chunks to read.

    Returns:
        int: The byte position immediately after the found tag.

    Raises:
        EOFError: If end of file is reached before finding a closing tag.
    """
    overlap_size = len(closing_tag)
    # Ensure chunk_size is at least the overlap size + 2, which ensures no infinite loop.
    chunk_size = max(overlap_size + 2, chunk_size)

    while True:
        pos_before = file_obj.tell()
        chunk = file_obj.read(chunk_size)
        if not chunk:
            raise EOFError("Reached end of file before finding tag end")

        # If the chunk is smaller than the requested chunk_size, we may be at the file end.
        if len(chunk) < chunk_size:
            # Check if the tag exists in this last chunk.
            if chunk.find(closing_tag) == -1:
                raise EOFError("Reached end of file before finding tag end")
        data = chunk

        idx = data.find(closing_tag)
        if idx != -1:
            absolute_pos = file_obj.tell() - len(data) + idx + overlap_size
            file_obj.seek(absolute_pos)
            return absolute_pos

        # Prepare overlap for the next iteration.
        overlap = data[-overlap_size:] if len(data) >= overlap_size else data

        # Rewind file pointer to ensure we do not skip data that may contain a split tag.
        file_obj.seek(-len(overlap), 1)

        # Check that progress is being made to avoid infinite loops.
        if file_obj.tell() <= pos_before:
            raise EOFError("No progress made while searching for closing tag")


def find_next_opening_tag_pos(
    file_obj: Union[BinaryIO, SnowflakeFile],
    tag_start_1: bytes,
    tag_start_2: bytes,
    end_limit: int,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
) -> int:
    """
    Efficiently finds the next opening tag position by reading chunks of data.
    Stops searching if the file pointer reaches or exceeds end_limit.

    Note that this function will change the position of file pointer, which is expected for next processing
    operation in ``process_xml_range``.

    Args:
        file_obj (BinaryIO or SnowflakeFile): Binary file object to read from.
        tag_start_1 (bytes): The first variant of the opening tag to search for (e.g., b"<row>").
        tag_start_2 (bytes): The second variant of the opening tag to search for (e.g., b"<row ").
        end_limit (int): The byte position up to which the search should be performed.
        chunk_size (int): Size of chunks to read.

    Returns:
        int: The byte position immediately where an opening tag is found.

    Raises:
        EOFError: If end of file is reached or no opening tag is found before reaching end_limit.
    """
    overlap_size = max(len(tag_start_1), len(tag_start_2))
    # Ensure chunk_size is at least the overlap size + 2, which ensures no infinite loop.
    chunk_size = max(overlap_size + 2, chunk_size)
    overlap = b""

    while True:
        current_pos = file_obj.tell()
        if current_pos >= end_limit:
            raise EOFError("Exceeded end limit before finding opening tag")

        remaining = end_limit - current_pos
        # Read enough so that the new chunk plus our overlap covers possible tag splits.
        current_chunk_size = min(chunk_size, remaining + overlap_size)
        pos_before = file_obj.tell()
        chunk = file_obj.read(current_chunk_size)
        if not chunk:
            raise EOFError("Reached end of file before finding opening tag")

        # Combine leftover from previous read with the new chunk.
        data = overlap + chunk

        # Search for both possible opening tag variants.
        pos1 = data.find(tag_start_1)
        pos2 = data.find(tag_start_2)
        if pos1 != -1 or pos2 != -1:
            pos = (
                min(pos1, pos2)
                if pos1 != -1 and pos2 != -1
                else (pos1 if pos1 != -1 else pos2)
            )
            # Calculate the absolute position. Note that `data` starts at (current_pos - len(overlap)).
            absolute_pos = current_pos + pos - len(overlap)
            if absolute_pos >= end_limit:
                raise EOFError("Exceeded end limit before finding opening tag")
            file_obj.seek(absolute_pos)
            return absolute_pos

        # No tag was found in this block.
        # Update the overlap from the end of the combined data.
        overlap = data[-overlap_size:] if len(data) >= overlap_size else data

        # Check that progress is being made to avoid infinite loops.
        if file_obj.tell() <= pos_before:
            raise EOFError("No progress made while searching for opening tag")


def strip_xml_namespaces(elem: ET.Element) -> ET.Element:
    """
    Recursively strip XML namespace information from an ElementTree element and its children.

    This function removes the namespace portion (e.g. 'xmlns') from the element's tag and attribute keys.
    After processing, all element tags and attribute keys will contain only their local names.
    """
    # Remove namespace from the element tag, if present
    if "}" in elem.tag:
        elem.tag = elem.tag.split("}", 1)[1]

    # Process element attributes: remove namespace from keys, if any
    # Create a list of namespace-prefixed keys to avoid modifying during iteration
    prefixed_keys = [key for key in elem.attrib.keys() if "}" in key]

    # Update attributes in place (compatible with lxml.etree)
    for key in prefixed_keys:
        value = elem.attrib[key]
        new_key = key.split("}", 1)[1]
        # Remove old key and add with new key
        del elem.attrib[key]
        elem.attrib[new_key] = value

    # Recursively strip namespaces in child elements
    for child in elem:
        strip_xml_namespaces(child)
    return elem


def element_to_dict_or_str(
    element: ET.Element,
    attribute_prefix: str = "_",
    exclude_attributes: bool = False,
    value_tag: str = "_VALUE",
    null_value: str = "",
    ignore_surrounding_whitespace: bool = False,
    result_template: Optional[dict] = None,
) -> Optional[Union[Dict[str, Any], str]]:
    """
    Recursively converts an XML Element to a dictionary.
    """
    norm_name_to_ori_name = (
        {key.lower(): key for key in result_template.keys()}
        if result_template is not None
        else None
    )

    def get_text(element: ET.Element) -> Optional[str]:
        """Do not strip the text"""
        if element.text is None:
            return None
        text = element.text.strip() if ignore_surrounding_whitespace else element.text
        if text == null_value:
            return None
        return text

    children = list(element)
    if not children and (not element.attrib or exclude_attributes):
        # it's a value element with no attributes or excluded attributes, so return the text
        return get_text(element)

    result = copy.deepcopy(result_template) if result_template is not None else {}

    if not exclude_attributes:
        for attr_name, attr_value in element.attrib.items():
            if ignore_surrounding_whitespace:
                attr_value = attr_value.strip()
            attribute_name = f"{attribute_prefix}{attr_name}"
            # when custom_schema exists, only exact mathc is allowed
            if result_template is None:
                result[attribute_name] = (
                    None if attr_value == null_value else attr_value
                )
            elif attribute_name.lower() in norm_name_to_ori_name:
                result[norm_name_to_ori_name[attribute_name.lower()]] = (
                    None if attr_value == null_value else attr_value
                )

    if children:
        temp_dict = {}
        for child in children:
            tag = child.tag
            child_result_template = None
            if result_template is not None:
                # skip if not in custom schema
                if tag.lower() not in norm_name_to_ori_name:
                    continue
                tag = norm_name_to_ori_name[tag.lower()]
                child_result_template = result_template[tag]
            child_dict = element_to_dict_or_str(
                child,
                attribute_prefix=attribute_prefix,
                exclude_attributes=exclude_attributes,
                value_tag=value_tag,
                null_value=null_value,
                ignore_surrounding_whitespace=ignore_surrounding_whitespace,
                result_template=child_result_template,
            )
            if tag in temp_dict:
                if not isinstance(temp_dict[tag], list):
                    temp_dict[tag] = [temp_dict[tag]]
                temp_dict[tag].append(child_dict)
            else:
                temp_dict[tag] = child_dict
        result.update(temp_dict)
    else:
        # it's a value element with attributes, so return the dict
        text = get_text(element)
        if text is not None:
            result[value_tag] = text
    return result


def process_xml_range(
    file_path: str,
    tag_name: str,
    approx_start: int,
    approx_end: int,
    mode: str,
    column_name_of_corrupt_record: str,
    ignore_namespace: bool,
    attribute_prefix: str,
    exclude_attributes: bool,
    value_tag: str,
    null_value: str,
    charset: str,
    ignore_surrounding_whitespace: bool,
    row_validation_xsd_path: str,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    result_template: Optional[dict] = None,
) -> Iterator[Optional[Dict[str, Any]]]:
    """
    Processes an XML file within a given approximate byte range.
    It locates complete records by:

      1. Starting at approx_start, finding the first opening tag.
      2. If the opening tag is beyond approx_end, the job is done.
      3. Otherwise, it finds the corresponding closing tag,
         reads the complete record, parses it via ElementTree,
         converts it to a dictionary, and yields the result.
      4. The process repeats from the end of the current record.

    Args:
        file_path (str): Path to the XML file.
        tag_name (str): The tag that delimits records (e.g., "row").
        approx_start (int): Approximate start byte position.
        approx_end (int): Approximate end byte position.
        mode (str): The mode for dealing with corrupt records.
            "PERMISSIVE", "DROPMALFORMED" and "FAILFAST" are supported.
        column_name_of_corrupt_record (str): The name of the column for corrupt records.
        ignore_namespace (bool): Whether to strip namespaces from the XML element.
        attribute_prefix (str): The prefix to add to the attribute names.
        exclude_attributes (bool): Whether to exclude attributes from the XML element.
        value_tag (str): The tag name for the value column.
        null_value (str): The value to treat as a null value.
        charset (str): The character encoding of the XML file.
        ignore_surrounding_whitespace (bool): Whether or not whitespaces surrounding values should be skipped.
        row_validation_xsd_path (str): Path to XSD file for row validation.
        chunk_size (int): Size of chunks to read.
        result_template(dict): a result template generate from user input schema

    Yields:
        Optional[Dict[str, Any]]: Dictionary representation of the parsed XML element.
                                  Yields None if parsing fails.
    """
    tag_start_1 = f"<{tag_name}>".encode()
    tag_start_2 = f"<{tag_name} ".encode()
    closing_tag = f"</{tag_name}>".encode()

    # Load XSD schema if validation is required
    xsd_schema = None
    if row_validation_xsd_path and lxml_installed:
        with SnowflakeFile.open(
            row_validation_xsd_path, "r", require_scoped_url=False
        ) as xsd_file:
            xsd_doc = ET.parse(xsd_file)
            xsd_schema = ET.XMLSchema(xsd_doc)

    # We perform raw byte‑level scanning here because we must split the file into independent
    # chunks by byte ranges for parallel processing. A streaming parser like xml.etree.ElementTree.iterparse
    # only yields element events (not byte offsets), requires well‑formed XML over the full stream,
    # and incurs extra overhead parsing every element sequentially—none of which allow us to locate
    # matching tag positions as raw byte ranges for chunking.
    with SnowflakeFile.open(file_path, "rb", require_scoped_url=False) as f:
        f.seek(approx_start)
        while True:
            try:
                open_pos = find_next_opening_tag_pos(
                    f, tag_start_1, tag_start_2, approx_end, chunk_size
                )
            except EOFError:
                # No further opening tag found within the range.
                break

            if open_pos >= approx_end:
                break

            record_start = open_pos
            f.seek(record_start)

            # decide whether the row element is self‑closing
            try:
                is_self_close, tag_end = tag_is_self_closing(f)
            # encountering an EOFError means the XML record isn't self-closing or
            # doesn't have a closing tag after reaching the end of the file
            except EOFError as e:
                if mode == "PERMISSIVE":
                    # read util the end of file or util variant column size limit
                    record_bytes = f.read(VARIANT_COLUMN_SIZE_LIMIT)
                    record_str = record_bytes.decode(charset, errors="replace")
                    record_str = re.sub(r"&(\w+);", replace_entity, record_str)
                    yield {column_name_of_corrupt_record: record_str}
                elif mode == "FAILFAST":
                    raise EOFError(
                        f"Malformed XML record at bytes {record_start}-EOF: {e}"
                    ) from e
                break

            if is_self_close:
                record_end = tag_end
            else:
                f.seek(tag_end)
                try:
                    record_end = find_next_closing_tag_pos(f, closing_tag, chunk_size)
                # encountering an EOFError means the XML record isn't self-closing or
                # doesn't have a closing tag after reaching the end of the file
                except EOFError as e:
                    if mode == "PERMISSIVE":
                        # read util the end of file or util variant column size limit
                        record_bytes = f.read(VARIANT_COLUMN_SIZE_LIMIT)
                        record_str = record_bytes.decode(charset, errors="replace")
                        record_str = re.sub(r"&(\w+);", replace_entity, record_str)
                        yield {column_name_of_corrupt_record: record_str}
                    elif mode == "FAILFAST":
                        raise EOFError(
                            f"Malformed XML record at bytes {record_start}-EOF: {e}"
                        ) from e
                    break

            # Read the complete XML record.
            f.seek(record_start)
            record_bytes = f.read(record_end - record_start)
            record_str = record_bytes.decode(charset, errors="replace")
            record_str = re.sub(r"&(\w+);", replace_entity, record_str)

            try:
                if lxml_installed:
                    # to parse undeclared namespaces, we have to use recover mode
                    recover = bool(":" in tag_name)
                    parser = ET.XMLParser(recover=recover, ns_clean=True)
                    try:
                        element = ET.fromstring(record_str, parser)
                    except ET.XMLSyntaxError:
                        # when ignoring namespaces, strip attribute prefixes
                        # like xyz:id -> id so records with undeclared prefixes can still parse.
                        if ignore_namespace:
                            try:
                                cleaned_record = re.sub(
                                    r"\s+(\w+):(\w+)=", r" \2=", record_str
                                )
                                element = ET.fromstring(cleaned_record, parser)
                            except Exception as inner_ex:
                                # avoid chained exceptions
                                raise inner_ex from None
                        else:
                            raise
                else:
                    element = ET.fromstring(record_str)

                # Perform XSD validation if schema is available
                if xsd_schema:
                    if not xsd_schema.validate(element):
                        validation_error = (
                            str(xsd_schema.error_log.last_error)
                            if xsd_schema.error_log.last_error
                            else "XSD validation failed"
                        )
                        raise ET.ParseError(
                            f"XSD validation failed: {validation_error}", None, 0, 0
                        )

                if ignore_namespace:
                    element = strip_xml_namespaces(element)
                result = element_to_dict_or_str(
                    element,
                    attribute_prefix=attribute_prefix,
                    exclude_attributes=exclude_attributes,
                    value_tag=value_tag,
                    null_value=null_value,
                    ignore_surrounding_whitespace=ignore_surrounding_whitespace,
                    result_template=copy.deepcopy(result_template),
                )
                if isinstance(result, dict):
                    yield result
                else:
                    yield {value_tag: result}
            except ET.ParseError as e:
                if mode == "PERMISSIVE":
                    yield {column_name_of_corrupt_record: record_str}
                elif mode == "FAILFAST":
                    raise RuntimeError(
                        f"Malformed XML record at bytes {record_start}-{record_end}: {e}\n"
                        f"XML record string: {record_str}"
                    )

            if record_end > approx_end:
                break

            # Move the file pointer to the end of the record to continue.
            f.seek(record_end)


class XMLReader:
    def process(
        self,
        filename: str,
        num_workers: int,
        row_tag: str,
        i: int,
        mode: str,
        column_name_of_corrupt_record: str,
        ignore_namespace: bool,
        attribute_prefix: str,
        exclude_attributes: bool,
        value_tag: str,
        null_value: str,
        charset: str,
        ignore_surrounding_whitespace: bool,
        row_validation_xsd_path: str,
        custom_schema: str,
    ):
        """
        Splits the file into byte ranges—one per worker—by starting with an even
        file size division and then moving each boundary to the end of a record,
        as indicated by the closing tag.

        Args:
            filename (str): Path to the XML file.
            num_workers (int): Number of workers/chunks.
            row_tag (str): The tag name that delimits records (e.g., "row").
            i (int): The worker id.
            mode (str): The mode for dealing with corrupt records.
                "PERMISSIVE", "DROPMALFORMED" and "FAILFAST" are supported.
            column_name_of_corrupt_record (str): The name of the column for corrupt records.
            ignore_namespace (bool): Whether to strip namespaces from the XML element.
            attribute_prefix (str): The prefix to add to the attribute names.
            exclude_attributes (bool): Whether to exclude attributes from the XML element.
            value_tag (str): The tag name for the value column.
            null_value (str): The value to treat as a null value.
            charset (str): The character encoding of the XML file.
            ignore_surrounding_whitespace (bool): Whether or not whitespaces surrounding values should be skipped.
            row_validation_xsd_path (str): Path to XSD file for row validation.
            custom_schema: User input schema for xml, must be used together with row tag.
        """
        file_size = get_file_size(filename)
        approx_chunk_size = file_size // num_workers
        approx_start = approx_chunk_size * i
        approx_end = approx_chunk_size * (i + 1) if i < num_workers - 1 else file_size
        result_template = schema_string_to_result_dict_and_struct_type(custom_schema)
        for element in process_xml_range(
            filename,
            row_tag,
            approx_start,
            approx_end,
            mode,
            column_name_of_corrupt_record,
            ignore_namespace,
            attribute_prefix,
            exclude_attributes,
            value_tag,
            null_value,
            charset,
            ignore_surrounding_whitespace,
            row_validation_xsd_path=row_validation_xsd_path,
            result_template=result_template,
        ):
            yield (element,)


def norm_text(
    ignore_surrounding_whitespace: bool, text: Optional[str]
) -> Optional[str]:
    if text is None:
        return None
    return text.strip() if ignore_surrounding_whitespace else text


def infer_type(
    text: str, ignore_surrounding_whitespace: bool, null_value: Optional[str]
) -> DataType:
    t = norm_text(ignore_surrounding_whitespace, text)
    if t is None:
        return NullType()

    # Apply null_value rule consistent with element_to_dict_or_str/get_text
    if t == null_value:
        return NullType()

    # Keep empty string as String unless user explicitly chose null_value=""
    # In ElementTree, empty tags often yield text=None; but if we do get "", honor null_value behavior above.
    if t == "":
        return StringType()

    low = t.lower()

    # boolean
    if low in ("true", "false"):
        return BooleanType()

    # integer / long (no underscores, no decimals)
    try:
        # reject things like "01.0"
        if all(c.isdigit() for c in (t[1:] if t.startswith(("+", "-")) else t)):
            int(t, 10)
            return IntegerType()
    except Exception:
        pass

    # decimal
    if _DECIMAL_RE.match(t):
        if t[0] in "+-":
            t = t[1:]

        if "." in t:
            left, right = t.split(".", 1)
            scale = len(right)
            precision = len(left) + len(right)
            if not (0 <= scale <= precision):
                scale = 0
            if not (0 <= precision <= 38):
                precision = 38
            return DecimalType(precision, scale)

    # time
    try:
        time.fromisoformat(t)
        return TimeType()
    except Exception:
        pass

    # date
    try:
        date.fromisoformat(t)
        return DateType()
    except Exception:
        pass

    # timestamp
    try:
        datetime.fromisoformat(t)
        return TimestampType()
    except Exception:
        pass

    return StringType()


def merge_decimal(a: DecimalType, b: DecimalType) -> DecimalType:
    # Merge by taking max precision and max scale (clamped).
    precision = max(a.precision, b.precision)
    scale = max(a.scale, b.scale)
    precision = min(38, max(precision, scale))
    scale = min(38, scale)
    return DecimalType(precision, scale)


def rank(dt: DataType) -> int:
    # Lower rank = "narrower"/preferred; higher = "wider"/more general
    if isinstance(dt, NullType):
        return 0
    if isinstance(dt, BooleanType):
        return 1
    if isinstance(dt, IntegerType):
        return 2
    if isinstance(dt, LongType):
        return 3
    if isinstance(dt, DecimalType):
        return 4
    if isinstance(dt, DoubleType):
        return 5
    if isinstance(dt, DateType):
        return 6
    if isinstance(dt, TimestampType):
        return 7
    if isinstance(dt, StringType):
        return 100
    if isinstance(dt, StructType):
        return 200
    if isinstance(dt, ArrayType):
        return 300
    return 1000


def merge_struct(a: StructType, b: StructType) -> StructType:
    if a is None:
        return b
    # Merge fields by name (case-sensitive), preserving first-seen order.
    a_fields = {f.name: f.datatype for f in a.fields}
    out_order = [f.name for f in a.fields]

    for f in b.fields:
        if f.name not in a_fields:
            a_fields[f.name] = f.datatype
            out_order.append(f.name)
        else:
            a_fields[f.name] = merge_types(a_fields[f.name], f.datatype)

    return StructType([StructField(name, a_fields[name], True) for name in out_order])


def merge_types(a: DataType, b: DataType) -> DataType:
    # Handle arrays first
    if isinstance(a, ArrayType) and isinstance(b, ArrayType):
        return ArrayType(merge_types(a.element_type, b.element_type))
    if isinstance(a, ArrayType):
        return ArrayType(merge_types(a.element_type, b))
    if isinstance(b, ArrayType):
        return ArrayType(merge_types(a, b.element_type))

    # Structs
    if isinstance(a, StructType) and isinstance(b, StructType):
        return merge_struct(a, b)

    # Nulls
    if isinstance(a, NullType):
        return b
    if isinstance(b, NullType):
        return a

    # Date/timestamp promotion
    if isinstance(a, TimestampType) and isinstance(b, DateType):
        return a
    if isinstance(a, DateType) and isinstance(b, TimestampType):
        return b

    # Numeric merging
    if isinstance(a, DecimalType) and isinstance(b, DecimalType):
        return merge_decimal(a, b)
    if isinstance(a, DecimalType) and isinstance(b, (IntegerType, LongType)):
        return a
    if isinstance(b, DecimalType) and isinstance(a, (IntegerType, LongType)):
        return b
    if isinstance(a, DoubleType) and isinstance(
        b, (IntegerType, LongType, DecimalType)
    ):
        return a
    if isinstance(b, DoubleType) and isinstance(
        a, (IntegerType, LongType, DecimalType)
    ):
        return b
    if isinstance(a, LongType) and isinstance(b, IntegerType):
        return a
    if isinstance(b, LongType) and isinstance(a, IntegerType):
        return b

    # If types are identical, keep one
    if type(a) is type(b):
        return a

    # Otherwise choose the "wider" by rank; anything conflicting often ends up as string.
    # (e.g., boolean + number, date + number, etc.)
    if rank(a) == 100 or rank(b) == 100:
        return StringType()
    return a if rank(a) >= rank(b) else b


def infer_schema(
    element: ET.Element,
    exclude_attributes: bool,
    attribute_prefix: str,
    null_value: str,
    value_tag: str,
    ignore_surrounding_whitespace: bool,
):
    children = list(element)

    # Case: no children and (no attributes OR attributes excluded) -> scalar
    if not children and (not element.attrib or exclude_attributes):
        return infer_type(element.text, ignore_surrounding_whitespace, null_value)

    fields = []

    # Attributes (same rule as element_to_dict_or_str)
    if not exclude_attributes:
        for attr_name, attr_value in element.attrib.items():
            fields.append(
                StructField(
                    f"{attribute_prefix}{attr_name}",
                    infer_type(attr_value, ignore_surrounding_whitespace, null_value),
                    True,
                )
            )

    # Children
    if children:
        by_tag = {}
        for c in children:
            by_tag.setdefault(c.tag, []).append(c)

        for tag, elems in by_tag.items():
            dt: Optional[DataType] = None
            for child_elem in elems:
                child_dt = infer_schema(
                    child_elem,
                    exclude_attributes,
                    attribute_prefix,
                    null_value,
                    value_tag,
                    ignore_surrounding_whitespace,
                )
                dt = child_dt if dt is None else merge_types(dt, child_dt)

            assert dt is not None
            if len(elems) > 1:
                dt = ArrayType(dt)
            fields.append(StructField(tag, dt, True))
    else:
        # No children, but has attributes -> also include the value_tag for text if present and not null
        # (matches element_to_dict_or_str behavior)
        t = norm_text(ignore_surrounding_whitespace, element.text)
        if t is not None and t != null_value:
            fields.append(
                StructField(
                    value_tag,
                    infer_type(t, ignore_surrounding_whitespace, null_value),
                    True,
                )
            )

    return StructType(fields)


class XMLSchemaInference:
    def process(
        self,
        file_path: str,
        row_tag: str,
        sampling_ratio: float,
        charset: str,
        ignore_namespace: bool,
        attribute_prefix: str,
        null_value: str,
        value_tag: str,
        ignore_surrounding_whitespace: bool,
        exclude_attributes: bool,
    ):
        chunk_size = int(1024)
        result = None

        tag_start_1 = f"<{row_tag}>".encode()
        tag_start_2 = f"<{row_tag} ".encode()
        closing_tag = f"</{row_tag}>".encode()

        file_size = get_file_size(file_path) or 0
        if file_size == 0:
            return None

        with SnowflakeFile.open(file_path, "rb", require_scoped_url=False) as f:
            f.seek(0)

            while True:
                # 1) Find next opening <row_tag ...> within the file
                try:
                    open_pos = find_next_opening_tag_pos(
                        f, tag_start_1, tag_start_2, file_size, chunk_size
                    )
                except EOFError:
                    break

                record_start = open_pos
                f.seek(record_start)

                # 2) Find record_end (self-closing vs closing tag)
                try:
                    is_self_close, tag_end = tag_is_self_closing(
                        f, chunk_size=chunk_size
                    )
                    if is_self_close:
                        record_end = tag_end
                    else:
                        f.seek(tag_end)
                        record_end = find_next_closing_tag_pos(
                            f, closing_tag, chunk_size=chunk_size
                        )
                except Exception:
                    # Malformed tag boundaries -> skip and keep scanning forward
                    try:
                        f.seek(min(record_start + 1, file_size))
                    except Exception:
                        break
                    continue

                # 3) Read full record bytes and parse using the SAME logic as process_xml_range
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
                                cleaned_record = re.sub(
                                    r"\s+(\w+):(\w+)=", r" \2=", record_str
                                )
                                element = ET.fromstring(cleaned_record, parser)
                            else:
                                raise
                    else:
                        element = ET.fromstring(record_str)

                    if ignore_namespace:
                        element = strip_xml_namespaces(element)

                except Exception:
                    # Malformed record -> ALWAYS skip it
                    try:
                        f.seek(min(record_end, file_size))
                    except Exception:
                        break
                    continue

                # 4) Sampling
                if random.random() < sampling_ratio:
                    schema = infer_schema(
                        element,
                        exclude_attributes,
                        attribute_prefix,
                        null_value,
                        value_tag,
                        ignore_surrounding_whitespace,
                    )
                    result = merge_struct(result, schema)

                # 5) Move to end of record and continue
                try:
                    f.seek(min(record_end, file_size))
                except Exception:
                    break

            yield (result.simple_string(),)
