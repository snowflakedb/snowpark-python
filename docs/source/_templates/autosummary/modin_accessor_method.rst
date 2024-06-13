{% if "series_utils.StringMethods" in fullname %}
    {% set fullname = "modin.pandas.Series.str" + objname %}
{% endif %}
{{ fullname }}
{{ underline }}

.. currentmodule:: {{ module }}

.. automodinaccessormethod:: {{ (module.split('.')[2:] + [objname]) | join('.') }}