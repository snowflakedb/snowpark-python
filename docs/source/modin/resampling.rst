=============================
Resampling
=============================

.. currentmodule:: snowflake.snowpark.modin.plugin.extensions.resample_overrides
.. rubric:: :doc:`All supported resampling APIs <supported/resampling_supported>`

.. rubric:: Indexing, iteration

.. autosummary::
    :toctree: pandas_api/

    Resampler.get_group
    Resampler.groups
    Resampler.indices

.. rubric:: Function application

.. autosummary::
    :toctree: pandas_api/

    Resampler.aggregate
    Resampler.apply
    Resampler.pipe
    Resampler.transform

.. rubric:: Upsampling

.. autosummary::
    :toctree: pandas_api/

    Resampler.asfreq
    Resampler.backfill
    Resampler.bfill
    Resampler.ffill
    Resampler.fillna
    Resampler.interpolate
    Resampler.nearest

.. rubric:: Computations / descriptive stats

.. autosummary::
    :toctree: pandas_api/

    Resampler.count
    Resampler.first
    Resampler.last
    Resampler.max
    Resampler.mean
    Resampler.median
    Resampler.min
    Resampler.nunique
    Resampler.ohlc
    Resampler.pad
    Resampler.prod
    Resampler.quantile
    Resampler.sem
    Resampler.std
    Resampler.size
    Resampler.sum
    Resampler.var
