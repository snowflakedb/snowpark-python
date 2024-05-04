=============================
Resampling
=============================

.. currentmodule:: snowflake.snowpark.modin.pandas.resample
.. rubric:: :doc:`All supported resampling APIs <supported/resampling_supported>`

.. rubric:: Indexing, iteration

.. autosummary::
    :toctree: pandas_api/

    Resampler.groups
    Resampler.indices
    Resampler.get_group


.. rubric:: Function application

.. autosummary::
    :toctree: pandas_api/

    Resampler.apply
    Resampler.aggregate
    Resampler.transform


.. rubric:: Upsampling

.. autosummary::
    :toctree: pandas_api/

    Resampler.ffill
    Resampler.bfill
    Resampler.nearest
    Resampler.fillna
    Resampler.asfreq

.. rubric:: Computations / descriptive stats

.. autosummary::
    :toctree: pandas_api/

    Resampler.count
    Resampler.nunique
    Resampler.first
    Resampler.last
    Resampler.max
    Resampler.mean
    Resampler.median
    Resampler.min
    Resampler.interpolate
    Resampler.ohlc
    Resampler.pad
    Resampler.pipe
    Resampler.prod
    Resampler.quantile
    Resampler.sem
    Resampler.size
    Resampler.std
    Resampler.sum
    Resampler.var
