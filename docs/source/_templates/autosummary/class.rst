{% if "modin.pandas" in fullname %}
    {% set fullname = "modin.pandas." + objname %}
{% endif %}
{{ fullname | escape | underline}}

{% if "modin.pandas" in module %}
    {% set module = "modin.pandas" %}
{% endif %}

.. currentmodule:: {{ module }}

.. autoclass:: {{ objname }}

{% set methods =(methods| reject("equalto", "__init__") |list) %}

{% block methods %}

{% if methods %}
   .. rubric:: Methods

   .. autosummary::
   {% for item in methods %}
   {%- if item not in inherited_members %}
      ~{{ name }}.{{ item }}
   {%- endif %}
   {%- endfor %}
{% endif %}
{% endblock %}

{% block attributes %}

{% if attributes %}
   .. rubric:: Attributes

   .. autosummary::
   {% for item in attributes %}
   {%- if item not in inherited_members %}
      ~{{ name }}.{{ item }}
   {%- endif %}
   {%- endfor %}
{% endif %}
{% endblock %}