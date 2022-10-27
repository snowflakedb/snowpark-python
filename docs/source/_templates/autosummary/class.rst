{% extends "!autosummary/class.rst" %}

{% set methods =(methods| reject("equalto", "__init__") |list) %}

{% block methods %}

{% if methods and methods|length > 0 %}
   .. rubric:: Methods

   .. autosummary::
   {% for item in methods %}
      ~{{ name }}.{{ item }}
   {%- endfor %}
{% endif %}
{% endblock %}