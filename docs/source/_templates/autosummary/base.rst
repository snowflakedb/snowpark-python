{% if "modin.pandas" in fullname %}
    {% set fullname = "modin.pandas." + objname %}
{% endif %}
{% if "modin.pandas.general" in module or ("modin.pandas" in module and name in ["read_csv", "read_json", "read_parquet", "read_snowflake", "to_snowpark", "to_pandas"])%}
    {% set module = "modin.pandas" %}
{% endif %}
{% extends "!autosummary/base.rst" %}