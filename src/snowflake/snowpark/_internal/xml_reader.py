#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import os
import re
import html.entities
import struct
from typing import Optional, Dict, Any, Iterator, BinaryIO, Union, Tuple
from snowflake.snowpark.files import SnowflakeFile

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
        # If the chunk is smaller than expected, we are near the end.
        if len(chunk) < current_chunk_size:
            if chunk.find(tag_start_1) == -1 and chunk.find(tag_start_2) == -1:
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

        # Otherwise, rewind by the length of the overlap so that a tag spanning the boundary isn't missed.
        file_obj.seek(-len(overlap), 1)

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
) -> Optional[Union[Dict[str, Any], str]]:
    """
    Recursively converts an XML Element to a dictionary.
    """

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

    result = {}

    if not exclude_attributes:
        for attr_name, attr_value in element.attrib.items():
            if ignore_surrounding_whitespace:
                attr_value = attr_value.strip()
            result[f"{attribute_prefix}{attr_name}"] = (
                None if attr_value == null_value else attr_value
            )

    if children:
        temp_dict = {}
        for child in children:
            child_dict = element_to_dict_or_str(
                child,
                attribute_prefix=attribute_prefix,
                exclude_attributes=exclude_attributes,
                value_tag=value_tag,
                null_value=null_value,
                ignore_surrounding_whitespace=ignore_surrounding_whitespace,
            )
            tag = child.tag
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
                    element = ET.fromstring(record_str, parser)
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
                        f"Malformed XML record at bytes {record_start}-{record_end}: {e}"
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
        """
        file_size = get_file_size(filename)
        approx_chunk_size = file_size // num_workers
        approx_start = approx_chunk_size * i
        approx_end = approx_chunk_size * (i + 1) if i < num_workers - 1 else file_size
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
        ):
            yield (element,)
