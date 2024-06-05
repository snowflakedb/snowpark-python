{% if "modin.pandas" in fullname %}
    {% set fullname = "modin.pandas." + objname %}
{% endif %}
{% extends "!autosummary/base.rst" %}