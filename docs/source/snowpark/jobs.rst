=============
Jobs
=============
Experimental API to submit, deploy and schedule Snowpark Python programs, available as :attr:`Session.jobs <snowflake.snowpark.Session.jobs>`.

.. note::

    A submitted run lasts only as long as the session that submitted it: closing the
    session (``session.close()``) cancels runs that are still going. To keep work going
    after the process ends, wait for the run before closing, or deploy and schedule it,
    because scheduled tasks run on their own. ``session.jobs.get()`` can re-attach to a
    run from another process while the submitting session is still open.

.. note::

    ``schedule()`` creates a task on the code bundle target's ``warehouse``, otherwise on
    the session's current warehouse at the time of the call; ``serverless=True`` creates a
    serverless task instead. ``Program.stage()`` and ``Program.workspace()`` programs
    run with the ``code_bundle.yml`` in the staged code, which defines the compute and the
    environment.

.. currentmodule:: snowflake.snowpark.jobs

.. rubric:: Job manager

.. autosummary::
    :toctree: api/

    JobManager
    JobManager.submit
    JobManager.deploy
    JobManager.get
    JobManager.list
    JobManager.get_deployment
    JobManager.cleanup

.. rubric:: Programs, environments and targets

.. autosummary::
    :toctree: api/

    Program
    Program.function
    Program.file
    Program.directory
    Program.stage
    Program.workspace
    Program.ref
    Environment
    StoredProcedure
    CodeBundle

.. rubric:: Jobs

.. autosummary::
    :toctree: api/

    Job
    JobStatus
    QueryJob
    BundleRun
    TaskRun
    JobNotSupportedError
    JobNotFoundError

.. rubric:: Deployments and schedules

.. autosummary::
    :toctree: api/

    Deployment
    ProcedureDeployment
    CodeBundleDeployment
    CodeBundleDeployment.add_version
    Schedule
    Schedule.suspend
    Schedule.resume
    Schedule.run_now
    Schedule.runs
    Schedule.drop
