===========================================
Hybrid Execution (Public Preview)
===========================================

Snowpark pandas supports workloads on mixed underlying execution engines and will automatically
move data to the most appropriate engine for a given dataset size and operation. Currently you
can use either Snowflake or local pandas to back a DataFrame object. Decisions on when to move
data are dominated by dataset size.

For Snowflake, specific API calls will trigger hybrid backend evaluation. These are registered 
as either a pre-operation switch point or a post-operation switch point. These switch points
may change over time as the feature matures and as APIs are updated.

Only Dataframes with data types that are compatible with Snowflake can be moved to the Snowflake
backend, and if a Dataframe backed by pandas contains such a type ('categorical' for example)
then astype should be used to coerce that type before the backend can be changed.

Example Pre-Operation Switchpoints:
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
apply, iterrows, itertuples, items, plot, quantile, __init__, plot, quantile, T, read_csv, read_json, concat, merge 

Many methods that are not yet implemented in Snowpark pandas are also registered as
pre-operation switch points, and will automatically move data to local pandas for execution when
called. This includes most methods that are ordinarily completely unsupported by Snowpark pandas,
and have `N` in their implemented status in the :doc:`DataFrame <supported/dataframe_supported>` and
:doc:`Series <supported/series_supported>` supported API lists.

Post-Operation Switchpoints:
~~~~~~~~~~~~~~~~~~~~~~~~~~~~
read_snowflake, value_counts, tail, var, std, sum, sem, max, min, mean, agg, aggregate, count, nunique, cummax, cummin, cumprod, cumsum


Examples
========

Enabling Hybrid Execution
~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    import modin.pandas as pd
    import snowflake.snowpark.modin.plugin

    # Import the configuration variable
    from modin.config import AutoSwitchBackend
    from snowflake.snowpark import Session
    
    Session.builder.create()
    df = pd.DataFrame([1, 2, 3])
    print(df.get_backend()) # 'Snowflake'

   # Enable hybrid execution
    AutoSwitchBackend().enable()
    df = pd.DataFrame([4, 5, 6])
    # DataFrame should use local execution backend, 'Pandas'
    # because the data frame is already small and in memory
    print(df.get_backend()) # 'Pandas'

    # Using a configuration context to change behavior
    # within a specific code block
    from modin.config import context as config_context
    with config_context(AutoSwitchBackend=False):
        # perform operations with no switching
        df = pd.DataFrame([[1, 2], [3, 4]])

    # Disable hybrid execution ( All DataFrames stay on existing engine )
    AutoSwitchBackend().disable()

Manually Changing DataFrame Backends
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    # Move a DataFrame to the local machine
    df_local = df.move_to('Pandas')
    # Move a DataFrame to be backed by Snowflake
    df_snow = df_local.move_to('Snowflake')
    # Move a DataFrame to the local machine, without changing the 'df' reference
    df.move_to('Pandas', inplace=True)
    # "pin" the current backend, preventing data movement
    df.pin_backend(inplace=True)
    # "unpin" the current backend, preventing data movement
    df.unpin_backend(inplace=True)

    from modin.config import context as config_context
    with config_context(Backend="Pandas"):
        # Operations only performed using the Pandas backend
        df = pd.DataFrame([4, 5, 6])

    with config_context(Backend="Snowflake"):
        # Operations only performed using the Snowflake backend
        df = pd.DataFrame([4, 5, 6])

Configuring Local Pandas Backend
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Currently the auto switching behavior is dominated by dataset size, with some exceptions
for specific operations. The default limit for running workloads on the local pandas 
backend is 10M rows. This can be configured through the modin environment variables:

.. code-block:: python

    # Change row threshold to 500k
    from modin.config.envvars import NativePandasMaxRows
    from modin.config import context as config_context

    NativePandasMaxRows.put(500_000)

    # Use a config context to set the Pandas backend parameters
    with config_context(NativePandasMaxRows=1234):
        # Operations only performed using the Pandas backend
        df = pd.DataFrame([4, 5, 6])


Debugging Hybrid Execution
~~~~~~~~~~~~~~~~~~~~~~~~~~

`pd.explain_switch()` provides information on how execution engine decisions
are made. This method prints a simplified version of the command unless `simple=False` is
passed as an argument.