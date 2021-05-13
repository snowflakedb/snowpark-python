class SnowflakePlan:
    # for read_file()
    __copy_option = {"ON_ERROR", "SIZE_LIMIT", "PURGE", "RETURN_FAILED_ONLY",
                     "MATCH_BY_COLUMN_NAME", "ENFORCE_LENGTH", "TRUNCATECOLUMNS", "FORCE",
                     "LOAD_UNCERTAIN_FILES"}

    def __init__(self, queries, schema_query, session, source_plan):
        self.queries = queries
        self.schema_query = schema_query
        self.session = session
        self.source_plan = source_plan

    # TODO
    def wrap_exception(self):
        pass


class SnowflakePlanBuilder:

    def __init__(self, session):
        self.__session = session

    def query(self, sql, source_plan):
        """
        :rtype: SnowflakePlan
        """
        return SnowflakePlan([Query(sql, None)], sql, session=self.__session, source_plan=source_plan)


class Query:

    def __init__(self, query_string, query_placeholder):
        self.sql = query_string
        self.place_holder = query_placeholder
