#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import os
import re
import html.entities
import logging
import xml.etree.ElementTree as ET
from typing import Optional, Dict, Any, Iterator, BinaryIO, Union
from snowflake.snowpark.files import SnowflakeFile


SELF_CLOSING_TAG: bytes = b"/>"
DEFAULT_CHUNK_SIZE: int = 1024


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


def find_next_closing_tag_pos(
    file_obj: Union[BinaryIO, SnowflakeFile],
    closing_tag: bytes,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
) -> int:
    """
    Efficiently finds the next closing tag position by reading chunks of data.
    It searches for both self-closing tags (b"/>") and normal closing tags,
    and returns the position immediately after the earliest occurrence.

    Args:
        file_obj (BinaryIO): Binary file object to read from.
        closing_tag (bytes): The closing tag to search for (e.g., b"</book>").
        chunk_size (int): Size of chunks to read.

    Returns:
        int: The byte position immediately after the found tag.

    Raises:
        EOFError: If end of file is reached before finding a closing tag.
    """
    # We'll use the maximum length among our tags as our overlap.
    overlap_size = max(len(closing_tag), len(SELF_CLOSING_TAG))
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
            if chunk.find(SELF_CLOSING_TAG) == -1 and chunk.find(closing_tag) == -1:
                raise EOFError("Reached end of file before finding tag end")
        data = chunk

        # Look for both self-closing and normal closing tag occurrences.
        self_close_pos = data.find(SELF_CLOSING_TAG)
        close_pos = data.find(closing_tag)

        # Determine the earliest occurrence (if any).
        chosen = None
        tag_len = 0
        if self_close_pos != -1 and close_pos != -1:
            if self_close_pos < close_pos:
                chosen = self_close_pos
                tag_len = len(SELF_CLOSING_TAG)
            else:
                chosen = close_pos
                tag_len = len(closing_tag)
        elif self_close_pos != -1:
            chosen = self_close_pos
            tag_len = len(SELF_CLOSING_TAG)
        elif close_pos != -1:
            chosen = close_pos
            tag_len = len(closing_tag)

        if chosen is not None:
            absolute_pos = file_obj.tell() - len(data) + chosen + tag_len
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

    This revised version avoids the infinite loop issue in the last chunk.

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
                raise EOFError("Found tag beyond end limit")
            file_obj.seek(absolute_pos)
            return absolute_pos

        # No tag was found in this block.
        # Update the overlap from the end of the combined data.
        overlap = data[-overlap_size:] if len(data) >= overlap_size else data

        # Otherwise, rewind by the length of the overlap so that a tag spanning the boundary isn’t missed.
        file_obj.seek(-len(overlap), 1)

        # Check that progress is being made to avoid infinite loops.
        if file_obj.tell() <= pos_before:
            raise EOFError("No progress made while searching for opening tag")


def strip_namespaces(elem):
    """
    Recursively strip XML namespace information from an ElementTree element and its children.

    This function removes the namespace portion (e.g. 'xmlns') from the element's tag and attribute keys.
    After processing, all element tags and attribute keys will contain only their local names.
    """
    # Remove namespace from the element tag, if present
    if "}" in elem.tag:
        elem.tag = elem.tag.split("}", 1)[1]

    # Process element attributes: remove namespace from keys, if any
    new_attrib = {}
    for key, value in elem.attrib.items():
        if "}" in key:
            new_key = key.split("}", 1)[1]
        else:
            new_key = key
        new_attrib[new_key] = value
    elem.attrib = new_attrib

    # Recursively strip namespaces in child elements
    for child in elem:
        strip_namespaces(child)
    return elem


def element_to_dict(
    element: ET.Element, attribute_prefix: str = "_"
) -> Optional[Union[Dict[str, Any], str]]:
    """
    Recursively converts an XML Element to a dictionary.
    """
    if not list(element) and not element.attrib:
        return element.text.strip() if element.text and element.text.strip() else None

    result: Dict[str, Any] = {}

    for attr_name, attr_value in element.attrib.items():
        result[f"{attribute_prefix}{attr_name}"] = attr_value

    children = list(element)
    if children:
        temp_dict: Dict[str, Any] = {}
        for child in children:
            child_dict = element_to_dict(child, attribute_prefix)
            tag = child.tag
            if tag in temp_dict:
                if not isinstance(temp_dict[tag], list):
                    temp_dict[tag] = [temp_dict[tag]]
                temp_dict[tag].append(child_dict)
            else:
                temp_dict[tag] = child_dict
        result.update(temp_dict)
    else:
        if element.text and element.text.strip():
            return element.text.strip()

    return result


def process_xml_range(
    file_path: str,
    tag_name: str,
    approx_start: int,
    approx_end: int,
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
        chunk_size (int): Size of chunks to read.

    Yields:
        Optional[Dict[str, Any]]: Dictionary representation of the parsed XML element.
                                  Yields None if parsing fails.
    """
    tag_start_1 = f"<{tag_name}>".encode()
    tag_start_2 = f"<{tag_name} ".encode()
    closing_tag = f"</{tag_name}>".encode()

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
            try:
                record_end = find_next_closing_tag_pos(f, closing_tag, chunk_size)
            except EOFError:
                break

            # Read the complete XML record.
            f.seek(record_start)
            record_bytes = f.read(record_end - record_start)
            try:
                record_str = record_bytes.decode("utf-8")
                record_str = re.sub(r"&(\w+);", replace_entity, record_str)
            except UnicodeDecodeError as e:
                logging.warning(
                    f"Unicode decode error at bytes {record_start}-{record_end}: {e}"
                )
                f.seek(record_end)
                continue

            try:
                element = ET.fromstring(record_str)
                yield element_to_dict(strip_namespaces(element))
            except ET.ParseError as e:
                logging.warning(
                    f"XML parse error at bytes {record_start}-{record_end}: {e}"
                )
                logging.warning(f"Record content: {record_str}")

            if record_end > approx_end:
                break

            # Move the file pointer to the end of the record to continue.
            f.seek(record_end)


class XMLReader:
    def process(self, filename: str, num_workers: int, row_tag: str, i: int):
        """
        Splits the file into byte ranges—one per worker—by starting with an even
        file size division and then moving each boundary to the end of a record,
        as indicated by the closing tag.

        Args:
            filename (str): Path to the XML file.
            num_workers (int): Number of workers/chunks.
            row_tag (str): The tag name that delimits records (e.g., "row").
            i (int): The worker id.
        """
        file_size = get_file_size(filename)
        approx_chunk_size = file_size // num_workers
        approx_start = approx_chunk_size * i
        approx_end = approx_chunk_size * (i + 1) if i < num_workers - 1 else file_size
        for element in process_xml_range(filename, row_tag, approx_start, approx_end):
            yield (element,)
