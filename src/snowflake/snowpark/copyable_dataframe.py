from snowflake.snowpark.dataframe import DataFrame
from snowflake.snowpark.internal.analyzer.snowflake_plan import SnowflakePlan
from snowflake.snowpark.internal.analyzer.staged_file_reader import StagedFileReader
from snowflake.snowpark.internal.utils import Utils
from snowflake.snowpark.plans.logical.logical_plan import CopyIntoNode as SPCopyIntoNode
from snowflake.snowpark.session import Session


class CopyableDataFrame(DataFrame):
    def __init__(self, session: Session, plan: SnowflakePlan, reader=StagedFileReader):
        super().__init__(session=session, plan=plan)
        self.staged_file_reader = reader

    def copyInto(self, table_name:str):
        """
        Executes a `COPY INTO <table>` command to load data from files in a stage into
        a specified table.

        Note that you don't need to call the :func:`collect` method before calling this
        method.

        For example, the following code loads data from the path specified by
        `my_file_stage` to the table `T`::

            df = session.read.schema(user_schema).csv(my_file_stage)
            df.copyInto("T")

        :param table_name:
        :return:
        """
        Utils.validate_object_name(table_name)
        self._DataFrame__with_plan(SPCopyIntoNode(table_name, [], {}, StagedFileReader.from_staged_file_reade(self.staged_file_reader))).collect()

