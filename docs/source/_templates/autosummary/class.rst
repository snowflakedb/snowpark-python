{% extends "!autosummary/class.rst" %}

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