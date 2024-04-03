#!/usr/bin/env python3
#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import argparse
import os
import shutil
import subprocess
from collections import namedtuple
from typing import Iterable, Optional, Union
import tempfile
import itertools

Class = namedtuple(
    "Class",
    ["module", "methods", "attributes"]
)
Module = namedtuple(
    "Module", ["name", "attributes", "functions", "classes", "exceptions"]
)
mapping = {
    "Methods": "methods",
    "Attributes": "attributes",
    "Module Attributes": "attributes",
    "Functions": "functions",
    "Classes": "classes",
    "Exceptions": "exceptions",
}


# STRING LITERALS
TAB = "    "
NEWLINE_TAB = f"\n{TAB}"
RUBRIC_HEADER = ".. rubric::"
AUTOSUMMARY_HEADER=".. autosummary::"


def autogen_and_parse_for_info(module_name: str, class_name: Optional[str] = None) -> Union[Module, Class]:
    if class_name:
        res = Class(module_name, [], [])
        name = f"{module_name}.{class_name}"
    else:
        res = Module(module_name, [], [], [], [])
        name = module_name

    with tempfile.TemporaryDirectory() as tmpdir:
        
        rst_content = f"""

.. currentmodule:: snowflake.snowpark

.. autosummary::
    :members:
    :inherited-members:
    :toctree: api/

    {name}

    """
        fname = os.path.join(tmpdir, f"{name}_temp.rst")
        with open(fname, "w") as fp:
            fp.write(rst_content)

        output_dir = os.path.join(tmpdir, 'output')
        subprocess.run(["sphinx-autogen", fname, "-o", output_dir, "-t", "_templates"])

        section = ""


        with open(f"{output_dir}/{name}.rst") as fp:
            for line in fp:
                line = line.strip()
                if line.startswith(AUTOSUMMARY_HEADER):
                    continue
                if line.startswith(RUBRIC_HEADER):
                    line = line[len(RUBRIC_HEADER):].strip()
                    section = mapping.get(line)
                    continue
                if not section:
                    continue
                if line and not line.isspace():
                    getattr(res, section).append(line.rstrip())
                elif getattr(res, section):  # End of a section
                    section = ""

    return res


def generate_autosummary_section(section: str, content: str) -> str:
    if content:
        return f"""
.. rubric:: {section}

.. autosummary::
    :toctree: api/

    {content}

"""
    else:
        return ""


def generate_module_header(title:str, module:str) -> str:
    automodule_text = "" if module=="snowflake.snowpark" else f"""
.. automodule:: {module}
  :noindex:
"""
    return f"""
{'='*(len(title)+5)}
{title}
{'='*(len(title)+5)}
{automodule_text}

.. currentmodule:: {module}

"""


def generate_classes(title:str, module:str, classes: Iterable[str]) -> str:
    results = [autogen_and_parse_for_info(module, c) for c in classes]
    names = NEWLINE_TAB.join(classes)
    methods = NEWLINE_TAB.join(itertools.chain.from_iterable(c.methods for c in results))
    attributes = NEWLINE_TAB.join(
        itertools.chain.from_iterable(c.attributes for c in results)
    )

    return f"""
{generate_module_header(title, module)}
{generate_autosummary_section("Classes", names)}
{generate_autosummary_section("Methods", methods)}
{generate_autosummary_section("Attributes", attributes)}

"""


def generate_module(title:str, module: str) -> str:
    mod = autogen_and_parse_for_info(module)
    attributes = NEWLINE_TAB.join(mod.attributes)
    functions = NEWLINE_TAB.join(mod.functions)
    exceptions = NEWLINE_TAB.join(mod.exceptions)
    classes = NEWLINE_TAB.join(mod.classes)
    return f"""
{generate_module_header(title, module)}
{generate_autosummary_section("Module Attributes", attributes)}
{generate_autosummary_section("Functions", functions)}
{generate_autosummary_section("Classes", classes)}
{generate_autosummary_section("Exceptions", exceptions)}

"""


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="generate_doc",
        description="Generate documentation rst for a module or classes inside a module",
    )

    parser.add_argument(
        "module", help="The module or the parent module of the classes to be documented"
    )
    parser.add_argument("-c", "--classes", nargs="*", help="Classes to be documented")
    parser.add_argument("-t", "--title", help="Title of the rst file generated", default="PLACEHOLDER")
    parser.add_argument(
        "-f", "--filename", help="File to write the generated content to")

    args = parser.parse_args()
    if args.classes:
        content = generate_classes(args.title, args.module, args.classes)
    else:
        content = generate_module(args.title, args.module)

    if args.filename:
        with open(args.filename, "w") as fp:
            fp.write(content)
    else:
        print(content)

