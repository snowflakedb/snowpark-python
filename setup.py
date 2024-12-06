#!/usr/bin/env python3
#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#
import os
import shutil
import subprocess
import sys
from codecs import open

from setuptools import setup
from setuptools.command.build_py import build_py as _build_py

THIS_DIR = os.path.dirname(os.path.realpath(__file__))
SRC_DIR = os.path.join(THIS_DIR, "src")
SNOWPARK_SRC_DIR = os.path.join(SRC_DIR, "snowflake", "snowpark")
MODIN_DEPENDENCY_VERSION = "==0.30.1"  # Snowpark pandas requires modin 0.30.1, which is compatible with pandas 2.2.x
CONNECTOR_DEPENDENCY_VERSION = ">=3.12.0, <4.0.0"
CONNECTOR_DEPENDENCY = f"snowflake-connector-python{CONNECTOR_DEPENDENCY_VERSION}"
INSTALL_REQ_LIST = [
    "setuptools>=40.6.0",
    "wheel",
    CONNECTOR_DEPENDENCY,
    # snowpark directly depends on typing-extension, so we should not remove it even if connector also depends on it.
    "typing-extensions>=4.1.0, <5.0.0",
    "pyyaml",
    "cloudpickle>=1.6.0,<=2.2.1,!=2.1.0,!=2.2.0",
    # `protoc` < 3.20 is not able to generate protobuf code compatible with protobuf >= 3.20.
    "protobuf>=3.20, <6",  # Snowpark IR
    "python-dateutil",  # Snowpark IR
    "tzlocal",  # Snowpark IR
]
REQUIRED_PYTHON_VERSION = ">=3.8, <3.13"

if os.getenv("SNOWFLAKE_IS_PYTHON_RUNTIME_TEST", False):
    REQUIRED_PYTHON_VERSION = ">=3.8"

PANDAS_REQUIREMENTS = [
    f"snowflake-connector-python[pandas]{CONNECTOR_DEPENDENCY_VERSION}",
]
MODIN_REQUIREMENTS = [
    *PANDAS_REQUIREMENTS,
    f"modin{MODIN_DEPENDENCY_VERSION}",
]
DEVELOPMENT_REQUIREMENTS = [
    "pytest<8.0.0",  # check SNOW-1022240 for more details on the pin here
    "pytest-cov",
    "wrapt",  # used for functools.wraps(...) in testing.
    "coverage",
    "sphinx==5.0.2",
    "cachetools",  # used in UDF doctest
    "pytest-timeout",
    "pytest-xdist",
    "openpyxl",  # used in read_excel test, not a requirement for distribution
    "matplotlib",  # used in plot tests
    "pre-commit",
    "graphviz",  # used in plot tests
    "pytest-assume",  # sql counter check
    "decorator",  # sql counter check
    "protoc-wheel-0==21.1",  # Protocol buffer compiler, for Snowpark IR
    "mypy-protobuf",  # used in generating typed Python code from protobuf for Snowpark IR
    "lxml",  # used in read_xml tests
]

# read the version
VERSION = ()
with open(os.path.join(SNOWPARK_SRC_DIR, "version.py"), encoding="utf-8") as f:
    exec(f.read())
if not VERSION:
    raise ValueError("version can't be read")
version = ".".join([str(v) for v in VERSION if v is not None])

with open(os.path.join(THIS_DIR, "README.md"), encoding="utf-8") as f:
    readme = f.read()
with open(os.path.join(THIS_DIR, "CHANGELOG.md"), encoding="utf-8") as f:
    changelog = f.read()

# Find the Protocol Compiler.
if "PROTOC" in os.environ and os.path.exists(os.environ["PROTOC"]):
    protoc = os.environ["PROTOC"]
else:
    protoc = shutil.which("protoc")

if protoc is None:
    sys.stderr.write(
        "protoc is not installed nor found. Please install the binary package, e.g., `pip install protoc-wheel-0==21.1 mypy-protobuf`\n"
    )
    sys.exit(-1)

protoc_gen_mypy = shutil.which("protoc-gen-mypy")
if protoc_gen_mypy is None:
    sys.stderr.write(
        "protoc-gen-mypy is not installed nor found. Please install the binary package, e.g., `pip install mypy-protobuf`\n"
    )
    sys.exit(-1)

# Protobuf files need to compile
PROTOS = ("src/snowflake/snowpark/_internal/proto/ast.proto",)

_ROOT_DIR = os.path.dirname(os.path.abspath(__file__))


def generate_proto(source):
    """Invokes the Protocol Compiler to generate a _pb2.py from the given
    .proto file.  Does nothing if the output already exists and is newer than
    the input."""
    output = source.replace(".proto", "_pb2.py").replace("proto/", "proto/generated/")
    if not os.path.exists(output) or (
        os.path.exists(source) and os.path.getmtime(source) > os.path.getmtime(output)
    ):
        print(f"Generating {output} from {source}...")  # noqa: T201

        if os.path.isfile(output):
            print(f"Removing existing file {output}...")  # noqa: T201
            os.remove(output)

        if not os.path.exists(source):
            sys.stderr.write("Can't find required file: %s\n" % source)
            sys.exit(-1)

        output_dir = os.path.dirname(output)
        proto_dir = os.path.dirname(source)
        protoc_command = [
            protoc,
            f"--proto_path={proto_dir}",
            f"--python_out={output_dir}",
            f"--mypy_out={output_dir}",
            source,
        ]
        if subprocess.call(protoc_command) != 0:
            sys.exit(-1)


class build_py(_build_py):
    """Generate protobuf bindings in build_py stage."""

    def run(self):
        for proto_path in PROTOS:
            proto_file = os.path.join(_ROOT_DIR, proto_path)
            generate_proto(proto_file)
        _build_py.run(self)


setup(
    name="snowflake-snowpark-python",
    version=version,
    description="Snowflake Snowpark for Python",
    long_description=readme + "\n\n" + changelog,
    long_description_content_type="text/markdown",
    author="Snowflake, Inc",
    author_email="snowflake-python-libraries-dl@snowflake.com",
    license="Apache License, Version 2.0",
    keywords="Snowflake db database cloud analytics warehouse",
    url="https://www.snowflake.com/",
    project_urls={
        "Documentation": "https://docs.snowflake.com/en/developer-guide/snowpark/python/index.html",
        "Source": "https://github.com/snowflakedb/snowpark-python",
        "Issues": "https://github.com/snowflakedb/snowpark-python/issues",
        "Changelog": "https://github.com/snowflakedb/snowpark-python/blob/main/CHANGELOG.md",
    },
    python_requires=REQUIRED_PYTHON_VERSION,
    install_requires=INSTALL_REQ_LIST,
    namespace_packages=["snowflake"],
    # When a new package (directory) is added, we should also add it here
    packages=[
        "snowflake.snowpark",
        "snowflake.snowpark._internal",
        "snowflake.snowpark._internal.analyzer",
        "snowflake.snowpark._internal.ast",
        "snowflake.snowpark._internal.compiler",
        "snowflake.snowpark._internal.proto.generated",
        "snowflake.snowpark.mock",
        "snowflake.snowpark.modin",
        "snowflake.snowpark.modin.config",
        "snowflake.snowpark.modin.plugin",
        "snowflake.snowpark.modin.plugin._internal",
        "snowflake.snowpark.modin.plugin.compiler",
        "snowflake.snowpark.modin.plugin.docstrings",
        "snowflake.snowpark.modin.plugin.extensions",
        "snowflake.snowpark.modin.plugin.io",
        "snowflake.snowpark.modin.plugin.utils",
    ],
    cmdclass={"build_py": build_py},
    package_dir={
        "": "src",
    },
    package_data={
        "snowflake.snowpark": ["LICENSE.txt", "py.typed"],
    },
    extras_require={
        "pandas": PANDAS_REQUIREMENTS,
        "modin": MODIN_REQUIREMENTS,
        "secure-local-storage": [
            f"snowflake-connector-python[secure-local-storage]{CONNECTOR_DEPENDENCY_VERSION}",
        ],
        "development": DEVELOPMENT_REQUIREMENTS,
        "modin-development": [
            *MODIN_REQUIREMENTS,
            *DEVELOPMENT_REQUIREMENTS,
            "scipy",  # Snowpark pandas 3rd party library testing
            "statsmodels",  # Snowpark pandas 3rd party library testing
            "scikit-learn==1.5.2",  # Snowpark pandas scikit-learn tests
        ],
        "localtest": [
            "pandas",
            "requests",
        ],
        "opentelemetry": [
            "opentelemetry-api>=1.0.0, <2.0.0",
            "opentelemetry-sdk>=1.0.0, <2.0.0",
        ],
    },
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Environment :: Console",
        "Environment :: Other Environment",
        "Intended Audience :: Developers",
        "Intended Audience :: Education",
        "Intended Audience :: Information Technology",
        "Intended Audience :: System Administrators",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
        "Programming Language :: SQL",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Topic :: Database",
        "Topic :: Software Development",
        "Topic :: Software Development :: Libraries",
        "Topic :: Software Development :: Libraries :: Application Frameworks",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: Scientific/Engineering :: Information Analysis",
    ],
    zip_safe=False,
)
