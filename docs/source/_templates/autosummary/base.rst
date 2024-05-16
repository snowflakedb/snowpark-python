{% if "modin.pandas" in fullname %}
    {% set fullname = "modin.pandas." + objname %}
{% endif %}
{% if "modin.pandas.general" in module %}
    {% set module = "modin.pandas" %}
{% endif %}
{% extends "!autosummary/base.rst" %}