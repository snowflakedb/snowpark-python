<!---
Please answer these questions before creating your pull request. Thanks!
--->

1. Which Jira issue is this PR addressing? Make sure that there is an accompanying issue to your PR.

   <!---
   In this section, please add a Snowflake Jira issue number.

   Note that if a corresponding GitHub issue exists, you should still include
   the Snowflake Jira issue number. For example, for GitHub issue
   https://github.com/snowflakedb/snowpark-python/issues/1400, you should
   add "SNOW-1335071" here.
    --->

   Fixes SNOW-NNNNNNN

2. Fill out the following pre-review checklist:

   - [ ] I am adding a new automated test(s) to verify correctness of my new code
      - [ ] If this test skips Local Testing mode, I'm requesting review from @snowflakedb/local-testing
   - [ ] I am adding new logging messages
   - [ ] I am adding a new telemetry message
   - [ ] I am adding new credentials
   - [ ] I am adding a new dependency
   - [ ] If this is a new feature/behavior, I'm adding the Local Testing parity changes.
   - [ ] I acknowledge that I have ensured my changes to be thread-safe. Follow the link for more information: [Thread-safe Developer Guidelines](https://github.com/snowflakedb/snowpark-python/blob/main/CONTRIBUTING.md#thread-safe-development)
   - [ ] If adding any arguments to public Snowpark APIs or creating new public Snowpark APIs, I acknowledge that I have ensured my changes include AST support. Follow the link for more information: [AST Support Guidelines](https://github.com/snowflakedb/snowpark-python/blob/main/CONTRIBUTING.md#ast-abstract-syntax-tree-support-in-snowpark)

3. Please describe how your code solves the related issue.

   Please write a short description of how your code change solves the related issue.
