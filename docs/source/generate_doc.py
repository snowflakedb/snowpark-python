#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#

import argparse
import os
import shutil
import subprocess
from collections import namedtuple
from typing import Iterable, Optional

Class = namedtuple(
    "Class",
    ["module", "methods", "attributes"],
    defaults=["snowflake.snowpark", [], []],
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


def autogen_and_parse_for_info(module_name: str, class_name: Optional[str] = None):
    if class_name:
        res = Class(module_name, [], [])
        name = f"{module_name}.{class_name}"
    else:
        res = Module(module_name, [], [], [], [])
        name = module_name

    if not os.path.exists("temp"):
        os.mkdir("temp")

    rst_content = f"""

.. currentmodule:: snowflake.snowpark

.. autosummary::
    :members:
    :inherited-members:
    :toctree: api/

    {name}

"""
    fname = f"temp/{name}_temp.rst"
    with open(fname, "w") as fp:
        fp.write(rst_content)

    subprocess.run(["sphinx-autogen", fname, "-o", "output", "-t", "source/_templates"])

    section = ""

    with open(f"output/{name}.rst") as fp:
        for line in fp:
            line = line.strip()
            if line.startswith(".. autosummary::"):
                continue
            if line.startswith(".. rubric:: "):
                line = line[12:]
                for title in mapping.keys():
                    if line.startswith(title):
                        section = mapping[title]
                        break
                continue
            if not section:
                continue
            if line and not line.isspace():
                getattr(res, section).append(line.rstrip())
            elif getattr(res, section):  # End of a section
                section = ""

    return res


def generate_section(section: str, content: str):
    if content:
        return f"""
.. rubric:: {section}

.. autosummary::
    :toctree: api/

    {content}

"""
    else:
        return ""


def generate(title: str, module: str, classes: Iterable[str]) -> str:
    import itertools

    if classes:
        results = [autogen_and_parse_for_info(module, c) for c in classes]
        names = "\n\t".join(classes)
        methods = "\n\t".join(itertools.chain.from_iterable(c.methods for c in results))
        attributes = "\n\t".join(
            itertools.chain.from_iterable(c.attributes for c in results)
        )
        return f"""
{'='*(len(title)+5)}
{title}
{'='*(len(title)+5)}

.. currentmodule:: {module}

.. rubric:: Classes

.. autosummary::
    :recursive:
    :toctree: api/

    {names}

{generate_section("Methods", methods)}
{generate_section("Attributes", attributes)}

"""
    else:
        mod = autogen_and_parse_for_info(module)
        attributes = "\n\t".join(mod.attributes)
        functions = "\n\t".join(mod.functions)
        exceptions = "\n\t".join(mod.exceptions)
        classes = "\n\t".join(mod.classes)
        return f"""
{'='*(len(title)+5)}
{title}
{'='*(len(title)+5)}

.. automodule:: {module}
  :noindex:

.. currentmodule:: {module}

{generate_section("Attributes", attributes)}
{generate_section("Functions", functions)}
{generate_section("Classes", classes)}
{generate_section("Exceptions", exceptions)}

"""


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="generate_doc",
        description="Generate documentation rst for a module or classes inside a module",
    )

    parser.add_argument("title", help="Title of the rst")

    parser.add_argument(
        "module", help="The module of parent module of the classes to be documented"
    )
    parser.add_argument("-c", "--classes", nargs="*", help="Classes to be documented")
    parser.add_argument(
        "-f", "--filename", help="File to write the generated content to"
    )
    parser.add_argument("--cleanup", action="store_true")

    args = parser.parse_args()
    content = generate(args.title, args.module, args.classes)
    if args.cleanup and os.path.exists("output"):
        shutil.rmtree("temp")
        shutil.rmtree("output")
    if args.filename:
        with open(args.filename, "w") as fp:
            fp.write(content)
    else:
        print(content)
