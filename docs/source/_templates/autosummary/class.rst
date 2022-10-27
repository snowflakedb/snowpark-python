{% extends "!autosummary/class.rst" %}

{% set methods =(methods| reject("equalto", "__init__") |list) %}

{% block methods %}

{% if methods %}
   .. rubric:: Methods

   .. autosummary::
   {% for item in methods %}
      ~{{ name }}.{{ item }}
   {%- endfor %}
{% endif %}
{% endblock %}