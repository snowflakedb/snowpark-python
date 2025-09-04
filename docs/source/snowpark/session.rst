=============================
Snowpark Session
=============================

.. currentmodule:: snowflake.snowpark

.. rubric:: Session

.. autosummary::
    :toctree: api/

    Session


.. rubric:: SessionBuilder

.. autosummary::
    :toctree: api/
    :template: autosummary/accessor_method.rst

    Session.SessionBuilder.app_name
    Session.SessionBuilder.config
    Session.SessionBuilder.configs
    Session.SessionBuilder.create
    Session.SessionBuilder.getOrCreate

.. rubric:: Methods

..
    TODO: Investigate how to add documentation for Session.SessionBuilder

.. autosummary::
    :toctree: api/

      Session.add_import
      Session.add_packages
      Session.add_requirements
      Session.append_query_tag
      Session.call
      Session.call_nowait
      Session.cancel_all
      Session.catalog
      Session.clear_imports
      Session.clear_packages
      Session.close
      Session.createDataFrame
      Session.create_async_job
      Session.create_dataframe
      Session.directory
      Session.flatten
      Session.generator
      Session.getActiveSession
      Session.get_active_session
      Session.get_current_account
      Session.get_current_database
      Session.get_current_role
      Session.get_current_schema
      Session.get_current_user
      Session.get_current_warehouse
      Session.get_fully_qualified_current_schema
      Session.get_fully_qualified_name_if_possible
      Session.get_imports
      Session.get_packages
      Session.get_session_stage
      Session.query_history
      Session.stored_procedure_profiler
      Session.range
      Session.remove_import
      Session.remove_package
      Session.replicate_local_environment
      Session.sql
      Session.table
      Session.table_function
      Session.update_query_tag
      Session.use_database
      Session.use_role
      Session.use_schema
      Session.use_secondary_roles
      Session.use_warehouse
      Session.write_pandas

.. rubric:: Attributes

.. autosummary::
    :toctree: api/

    Session.builder
    Session.custom_package_usage_config
    Session.file
    Session.query_tag
    Session.lineage
    Session.read
    Session.sproc
    Session.sql_simplifier_enabled
    Session.telemetry_enabled
    Session.udaf
    Session.udf
    Session.udtf
    Session.session_id
    Session.connection
