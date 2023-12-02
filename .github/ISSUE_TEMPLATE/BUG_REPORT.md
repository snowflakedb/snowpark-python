---
name: "Bug Report \U0001F41E"
about: Something isn't working as expected? Here is the right place to report.
title: ''
labels: bug, needs triage
assignees: ''

---

Please answer these questions before submitting your issue. Thanks!

1. What version of Python are you using?

   Replace with the output of `python --version --version`

2. What operating system and processor architecture are you using?

   Replace with the output of `python -c 'import platform; print(platform.platform())'`

3. What are the component versions in the environment (`pip freeze`)?

   Replace with the output of `python -m pip freeze`

4. What did you do?

   If possible, provide a recipe for reproducing the error.
   A complete runnable program is good.

5. What did you expect to see?

   What should have happened and what happened instead?

6. Can you set logging to DEBUG and collect the logs?

   ```
   import logging

   for logger_name in ('snowflake.snowpark', 'snowflake.connector'):
       logger = logging.getLogger(logger_name)
       logger.setLevel(logging.DEBUG)
       ch = logging.StreamHandler()
       ch.setLevel(logging.DEBUG)
       ch.setFormatter(logging.Formatter('%(asctime)s - %(threadName)s %(filename)s:%(lineno)d - %(funcName)s() - %(levelname)s - %(message)s'))
       logger.addHandler(ch)
   ```

<!--
If you need urgent assistance reach out to support for escalated issue processing https://community.snowflake.com/s/article/How-To-Submit-a-Support-Case-in-Snowflake-Lodge
-->
