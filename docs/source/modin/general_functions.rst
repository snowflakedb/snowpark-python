=============================
General functions
=============================

.. currentmodule:: modin.pandas
.. rubric:: :doc:`All supported general functions <supported/general_supported>`

.. rubric:: Data manipulations

.. autosummary::
    :toctree: pandas_api/

    melt
    crosstab
    pivot
    pivot_table
    cut
    qcut
    concat
    get_dummies
    merge
    merge_asof
    unique

.. rubric:: Top-level missing data

.. autosummary::
    :toctree: pandas_api/

    isna
    isnull
    notna
    notnull


.. rubric:: Top-level dealing with numeric data

.. autosummary::
    :toctree: pandas_api/

    to_numeric

.. rubric:: Top-level dealing with datetimelike data

.. autosummary::
    :toctree: pandas_api/

    date_range
    bdate_range
    to_datetime

.. rubric:: Hybrid Execution functions

.. autosummary::
    :toctree: pandas_api/

    explain_switch
