#!/usr/bin/env python3
#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#
import os
from codecs import open

from setuptools import setup

THIS_DIR = os.path.dirname(os.path.realpath(__file__))
SRC_DIR = os.path.join(THIS_DIR, "src")
SNOWPARK_SRC_DIR = os.path.join(SRC_DIR, "snowflake", "snowpark")
VERSION = (1, 1, 1, None)  # Default, needed so code will compile
CONNECTOR_DEPENDENCY_VERSION = "2.7.4"
with open(os.path.join(SNOWPARK_SRC_DIR, "version.py"), encoding="utf-8") as f:
    exec(f.read())
version = ".".join([str(v) for v in VERSION if v is not None])

setup(
    name="snowflake-snowpark-python",
    version=version,
    description="Snowflake Snowpark for Python",
    author="Snowflake, Inc",
    author_email="support@snowflake.com",
    license="Apache License, Version 2.0",
    keywords="Snowflake db database cloud analytics warehouse",
    url="https://www.snowflake.com/",
    project_urls={
        "Documentation": "https://docs.snowflake.com/",
        # TODO: update it when we have documentation
        # "Code": "https://github.com/snowflakedb/snowflake-connector-python",
        # "Issue tracker": "https://github.com/snowflakedb/snowflake-connector-python/issues",
    },
    python_requires="==3.8.*",
    install_requires=[
        "setuptools>=40.6.0",
        "wheel",
        "cloudpickle>=1.6.0,<2.1.0",
        f"snowflake-connector-python>={CONNECTOR_DEPENDENCY_VERSION}",
        "typing-extensions>=4.1.0,<4.2.0",
    ],
    namespace_packages=["snowflake"],
    # When a new package (directory) is added, we should also add it here
    packages=[
        "snowflake.snowpark",
        "snowflake.snowpark._internal",
        "snowflake.snowpark._internal.analyzer",
    ],
    package_dir={
        "": "src",
    },
    package_data={
        "snowflake.snowpark": ["*.pem", "*.json", "*.rst", "LICENSE.txt"],
    },
    extras_require={
        "pandas": [
            f"snowflake-connector-python[pandas]>={CONNECTOR_DEPENDENCY_VERSION}",
        ],
        "secure-local-storage": [
            f"snowflake-connector-python[secure-local-storage]>={CONNECTOR_DEPENDENCY_VERSION}",
        ],
        "development": [
            "pytest",
            "pytest-cov",
            "coverage",
            "sphinx",
            "cachetools",  # used in UDF doctest
        ],
    },
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Environment :: Console",
        "Environment :: Other Environment",
        "Intended Audience :: Developers",
        "Intended Audience :: Education",
        "Intended Audience :: Information Technology",
        "Intended Audience :: System Administrators",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
        "Programming Language :: SQL",
        "Programming Language :: Python :: 3.8",
        "Topic :: Database",
        "Topic :: Software Development",
        "Topic :: Software Development :: Libraries",
        "Topic :: Software Development :: Libraries :: Application Frameworks",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: Scientific/Engineering :: Information Analysis",
    ],
    zip_safe=False,
)
