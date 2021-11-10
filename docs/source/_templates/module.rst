{{ fullname | escape | underline}}
.. automodule:: {{ fullname }}

{% if fullname == 'snowflake.snowpark' %}

    .. rubric:: {{ _('Classes') }}

    .. autosummary::
        {% for item in ['CaseExpr', 'Column', 'DataFrame', 'DataFrameReader', 'DataFrameWriter',
            'RelationalGroupedDataFrame', 'Row', 'Session', 'FileOperation', 'PutResult', 'GetResult', 'Window', 'WindowSpec']
        %}
            {{ item }}
        {% endfor %}

    .. rubric:: {{ _('Submodules') }}

    .. autosummary::

        {% for item in ['snowflake.snowpark.functions', 'snowflake.snowpark.types',
            'snowflake.snowpark.udf', 'snowflake.snowpark.exceptions']
        %}
            {{ item }}
        {% endfor %}
{% endif %}

   {% block attributes %}
   {% if attributes %}
   .. rubric:: {{ _('Module Attributes') }}

   .. autosummary::
   {% for item in attributes %}
      {{ item }}
   {%- endfor %}
   {% endif %}
   {% endblock %}

   {% block functions %}
   {% if functions %}
   .. rubric:: {{ _('Functions') }}

   .. autosummary::
   {% for item in functions %}
      {{ item }}
   {%- endfor %}
   {% endif %}
   {% endblock %}

   {% block classes %}
   {% if classes %}
   .. rubric:: {{ _('Classes') }}

   .. autosummary::
   {% for item in classes %}
      {{ item }}
   {%- endfor %}
   {% endif %}
   {% endblock %}

   {% block exceptions %}
   {% if exceptions %}
   .. rubric:: {{ _('Exceptions') }}

   .. autosummary::
   {% for item in exceptions %}
      {{ item }}
   {%- endfor %}
   {% endif %}
   {% endblock %}

{% block modules %}
{% if modules %}
.. rubric:: Modules

.. autosummary::
   :toctree:
   :recursive:
{% for item in modules %}
   {{ item }}
{%- endfor %}
{% endif %}
{% endblock %}