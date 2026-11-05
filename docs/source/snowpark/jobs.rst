=============
Jobs
=============
Experimental API to submit, deploy and schedule Snowpark Python programs, available as :attr:`Session.jobs <snowflake.snowpark.Session.jobs>`.

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
