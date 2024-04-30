#!/usr/bin/env python3
#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#
import os
from codecs import open

from setuptools import setup

THIS_DIR = os.path.dirname(os.path.realpath(__file__))
SRC_DIR = os.path.join(THIS_DIR, "src")
SNOWPARK_SRC_DIR = os.path.join(SRC_DIR, "snowflake", "snowpark")
CONNECTOR_DEPENDENCY_VERSION = ">=3.10.0, <4.0.0"
INSTALL_REQ_LIST = [
    "setuptools>=40.6.0",
    "wheel",
    f"snowflake-connector-python{CONNECTOR_DEPENDENCY_VERSION}",
    # snowpark directly depends on typing-extension, so we should not remove it even if connector also depends on it.
    "typing-extensions>=4.1.0, <5.0.0",
    "pyyaml",
    "cloudpickle>=1.6.0,<=2.2.1,!=2.1.0,!=2.2.0;python_version<'3.11'",
    "cloudpickle==2.2.1;python_version~='3.11'",  # backend only supports cloudpickle 2.2.1 + python 3.11 at the moment
]
REQUIRED_PYTHON_VERSION = ">=3.8, <3.12"

if os.getenv("SNOWFLAKE_IS_PYTHON_RUNTIME_TEST", False):
    REQUIRED_PYTHON_VERSION = ">=3.8"

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
        "snowflake.snowpark.mock",
    ],
    package_dir={
        "": "src",
    },
    package_data={
        "snowflake.snowpark": ["LICENSE.txt", "py.typed"],
    },
    extras_require={
        "pandas": [
            f"snowflake-connector-python[pandas]{CONNECTOR_DEPENDENCY_VERSION}",
        ],
        "secure-local-storage": [
            f"snowflake-connector-python[secure-local-storage]{CONNECTOR_DEPENDENCY_VERSION}",
        ],
        "development": [
            "pytest<8.0.0",  # check SNOW-1022240 for more details on the pin here
            "pytest-cov",
            "coverage",
            "sphinx==5.0.2",
            "cachetools",  # used in UDF doctest
            "pytest-timeout",
            "pre-commit",
        ],
        "localtest": [
            "pandas",
            "pyarrow",
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
