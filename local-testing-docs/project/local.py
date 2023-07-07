"""
This module provides utilities for local development and testing.
"""

from pathlib import Path
from os import environ

import configparser
import toml


def get_env_var_config() -> dict:
    """
    Returns a dictionary of the connection parameters using the SnowSQL CLI
    environment variables.
    """
    try:
        return {
            "user": environ["SNOWSQL_USER"],
            "password": environ["SNOWSQL_PWD"],
            "account": environ["SNOWSQL_ACCOUNT"],
            # "role": environ["SNOWSQL_ROLE"],
            "warehouse": environ["SNOWSQL_WAREHOUSE"],
            "database": environ["SNOWSQL_DATABASE"],
            "schema": environ["SNOWSQL_SCHEMA"],
        }
    except KeyError as exc:
        raise KeyError(
            "ERROR: Environment variable for Snowflake Connection not found. "
            + "Please set the SNOWSQL_* environment variables"
        ) from exc


def get_dev_config(
    environment: str = "dev",
    app_config_path: Path = Path.cwd().joinpath("app.toml"),
) -> dict:
    """
    Returns a dictionary of the connection parameters using the app.toml
    in the project root.
    """
    try:
        app_config = toml.load(app_config_path)
        config = configparser.ConfigParser(inline_comment_prefixes="#")
        config.read(app_config["snowsql_config_path"])
        session_config = config["connections." + app_config["snowsql_connection_name"]]
        session_config_dict = {
            k.replace("name", ""): v.strip('"') for k, v in session_config.items()
        }
        session_config_dict.update(app_config.get(environment))  # type: ignore
        return session_config_dict
    except Exception as exc:
        raise EnvironmentError(
            "Error creating snowpark session - be sure you've logged into "
            "the SnowCLI and have a valid app.toml file",
        ) from exc
