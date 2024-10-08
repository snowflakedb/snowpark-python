Resampler supported APIs
========================

The following table is structured as follows: The first column contains the method name.
The second column is a flag for whether or not there is an implementation in Snowpark for
the method in the left column.

.. note::
    ``Y`` stands for yes, i.e., supports distributed implementation, ``N`` stands for no and API simply errors out,
    ``P`` stands for partial (meaning some parameters may not be supported yet), and ``D`` stands for defaults to single
    node pandas execution via UDF/Sproc.
    ``engine`` and ``engine_kwargs`` are always ignored in Snowpark pandas. The execution engine will always be Snowflake.

Indexing, iteration

+-----------------------------+---------------------------------+----------------------------------------------------+
| Resampler method            | Snowpark implemented? (Y/N/P/D) | Notes for current implementation                   |
+-----------------------------+---------------------------------+----------------------------------------------------+
| ``get_group``               | N                               |                                                    |
+-----------------------------+---------------------------------+----------------------------------------------------+
| ``groups``                  | N                               |                                                    |
+-----------------------------+---------------------------------+----------------------------------------------------+
| ``indices``                 | Y                               |                                                    |
+-----------------------------+---------------------------------+----------------------------------------------------+
| ``__iter__``                | N                               |                                                    |
+-----------------------------+---------------------------------+----------------------------------------------------+

Function application

+-----------------------------+---------------------------------+----------------------------------------------------+
| Resampler method            | Snowpark implemented? (Y/N/P/D) | Notes for current implementation                   |
+-----------------------------+---------------------------------+----------------------------------------------------+
| ``aggregate``               | N                               |                                                    |
+-----------------------------+---------------------------------+----------------------------------------------------+
| ``apply``                   | N                               |                                                    |
+-----------------------------+---------------------------------+----------------------------------------------------+
| ``pipe``                    | N                               |                                                    |
+-----------------------------+---------------------------------+----------------------------------------------------+
| ``transform``               | N                               |                                                    |
+-----------------------------+---------------------------------+----------------------------------------------------+

Upsampling

+-----------------------------+---------------------------------+----------------------------------+----------------------------------------------------+
| Resampler method            | Snowpark implemented? (Y/N/P/D) | Missing parameters               | Notes for current implementation                   |
+-----------------------------+---------------------------------+----------------------------------+----------------------------------------------------+
| ``asfreq``                  | P                               | ``fill_value``                   |                                                    |
+-----------------------------+---------------------------------+----------------------------------+----------------------------------------------------+
| ``bfill``                   | P                               | ``limit``                        |                                                    |
+-----------------------------+---------------------------------+----------------------------------+----------------------------------------------------+
| ``ffill``                   | P                               | ``limit``                        |                                                    |
+-----------------------------+---------------------------------+----------------------------------+----------------------------------------------------+
| ``fillna``                  | P                               | ``limit``                        | Method ``nearest`` is not supported.               |
+-----------------------------+---------------------------------+----------------------------------+----------------------------------------------------+
| ``interpolate``             | N                               |                                  |                                                    |
+-----------------------------+---------------------------------+----------------------------------+----------------------------------------------------+
| ``nearest``                 | N                               |                                  |                                                    |
+-----------------------------+---------------------------------+----------------------------------+----------------------------------------------------+

Computations / descriptive stats

+-----------------------------+---------------------------------+----------------------------------------------------+
| Resampler method            | Snowpark implemented? (Y/N/P/D) | Notes for current implementation                   |
+-----------------------------+---------------------------------+----------------------------------------------------+
| ``count``                   | Y                               |                                                    |
+-----------------------------+---------------------------------+----------------------------------------------------+
| ``first``                   | P                               | Does not support ``min_count`` parameter           |
+-----------------------------+---------------------------------+----------------------------------------------------+
| ``last``                    | P                               | Does not support ``min_count`` parameter           |
+-----------------------------+---------------------------------+----------------------------------------------------+
| ``max``                     | Y                               |                                                    |
+-----------------------------+---------------------------------+----------------------------------------------------+
| ``mean``                    | Y                               |                                                    |
+-----------------------------+---------------------------------+----------------------------------------------------+
| ``median``                  | Y                               |                                                    |
+-----------------------------+---------------------------------+----------------------------------------------------+
| ``min``                     | Y                               |                                                    |
+-----------------------------+---------------------------------+----------------------------------------------------+
| ``nunique``                 | Y                               |                                                    |
+-----------------------------+---------------------------------+----------------------------------------------------+
| ``ohlc``                    | N                               |                                                    |
+-----------------------------+---------------------------------+----------------------------------------------------+
| ``prod``                    | N                               |                                                    |
+-----------------------------+---------------------------------+----------------------------------------------------+
| ``quantile``                | P                               | ``N`` for list-like ``q``                          |
+-----------------------------+---------------------------------+----------------------------------------------------+
| ``sem``                     | N                               |                                                    |
+-----------------------------+---------------------------------+----------------------------------------------------+
| ``std``                     | P                               | ``N`` if ``ddof`` is not 0 or 1                    |
+-----------------------------+---------------------------------+----------------------------------------------------+
| ``size``                    | Y                               |                                                    |
+-----------------------------+---------------------------------+----------------------------------------------------+
| ``sum``                     | Y                               |                                                    |
+-----------------------------+---------------------------------+----------------------------------------------------+
| ``var``                     | P                               | ``N`` if ``ddof`` is not 0 or 1                    |
+-----------------------------+---------------------------------+----------------------------------------------------+

