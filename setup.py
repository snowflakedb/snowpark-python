#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
#
import os
from codecs import open

from setuptools import setup

THIS_DIR = os.path.dirname(os.path.realpath(__file__))
SRC_DIR = os.path.join(THIS_DIR, "src")
SNOWPARK_SRC_DIR = os.path.join(SRC_DIR, "snowflake", "snowpark")
VERSION = (1, 1, 1, None)  # Default
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
    python_requires=">=3.6",
    install_requires=[
        "setuptools>=40.6.0",
        "wheel",
        "cloudpickle>=1.6.0",
        "snowflake-connector-python>=2.6.2",
    ],
    namespace_packages=["snowflake"],
    # When a new package (directory) is added, we should also add it here
    packages=[
        "snowflake.snowpark",
        "snowflake.snowpark._internal",
        "snowflake.snowpark._internal.analyzer",
        "snowflake.snowpark._internal.plans",
        "snowflake.snowpark._internal.plans.logical",
        "snowflake.snowpark._internal.sp_types",
    ],
    package_dir={
        "": "src",
    },
    package_data={
        "snowflake.snowpark": ["*.pem", "*.json", "*.rst", "LICENSE.txt"],
    },
    extras_require={
        "pandas": "snowflake-connector-python[pandas]>=2.6.2",
        "development": [
            "pytest",
            "pytest-cov",
            "coverage",
            "sphinx",
            "sphinx_autodoc_typehints",
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
