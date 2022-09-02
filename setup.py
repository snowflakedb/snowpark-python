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
CONNECTOR_DEPENDENCY_VERSION = "2.7.12"

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
    author_email="triage-snowpark-python-api-dl@snowflake.com",
    license="Apache License, Version 2.0",
    keywords="Snowflake db database cloud analytics warehouse",
    url="https://www.snowflake.com/",
    project_urls={
        "Documentation": "https://docs.snowflake.com/en/developer-guide/snowpark/reference/python",
        "Source": "https://github.com/snowflakedb/snowpark-python",
        "Issues": "https://github.com/snowflakedb/snowpark-python/issues",
        "Changelog": "https://github.com/snowflakedb/snowpark-python/blob/main/CHANGELOG.md",
    },
    python_requires="==3.8.*",
    install_requires=[
        "setuptools>=40.6.0",
        "wheel",
        "cloudpickle>=1.6.0,<=2.0.0",
        f"snowflake-connector-python>={CONNECTOR_DEPENDENCY_VERSION}",
        "typing-extensions>=4.1.0",
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
        "snowflake.snowpark": ["LICENSE.txt"],
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
            "sphinx==5.0.2",
            "cachetools",  # used in UDF doctest
        ],
    },
    classifiers=[
        "Development Status :: 4 - Beta",
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
        "Topic :: Database",
        "Topic :: Software Development",
        "Topic :: Software Development :: Libraries",
        "Topic :: Software Development :: Libraries :: Application Frameworks",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: Scientific/Engineering :: Information Analysis",
    ],
    zip_safe=False,
)
