#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import json
from enum import Enum
from typing import List, Optional, Tuple, Union

import snowflake.snowpark
from snowflake.snowpark._internal.error_message import SnowparkClientExceptionMessages
from snowflake.snowpark.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    VariantType,
)


class LineageDirection(Enum):
    """
    Directions for tracing the lineage.

    Attributes:
        DOWNSTREAM (str): Represents the downstream direction in lineage tracing.
        UPSTREAM (str): Represents the upstream direction in lineage tracing.
        BOTH (str): Represents both upstream and downstream direction in lineage tracing.
    """

    DOWNSTREAM = "downstream"
    UPSTREAM = "upstream"
    BOTH = "both"

    def __str__(self):
        return self.value

    @classmethod
    def values(cls):
        return [member.value for member in cls]

    @classmethod
    def value_of(cls, value):
        for member in cls:
            if member.value == value:
                return member
        else:
            raise ValueError(f"'{cls.__name__}' enum not found for '{value}'")


class _EdgeType(Enum):
    """
    Types of edges for lineage tracing.
    """

    DATA_LINEAGE = "DATA_LINEAGE"
    OBJECT_DEPENDENCY = "OBJECT_DEPENDENCY"

    @classmethod
    def values(cls):
        return [member.value for member in cls]


class _ObjectField:
    """
    Defines static fields used to reference object properties in DGQL query and response.
    """

    DOMAIN = "domain"
    REFINED_DOMAIN = "refinedDomain"
    USER_DOMAIN = "userDomain"
    NAME = "name"
    PROPERTIES = "properties"
    SCHEMA = "schema"
    DB = "db"
    STATUS = "status"
    CREATED_ON = "createdOn"
    PARENT_NAME = "ParentName"
    VERSION = "version"

    # A list of fileds queried on each object in the lineage.
    GRAPH_ENTITY_PROPERTIES = [
        DOMAIN,
        REFINED_DOMAIN,
        USER_DOMAIN,
        NAME,
        PROPERTIES,
        SCHEMA,
        DB,
        STATUS,
        CREATED_ON,
    ]


class _DGQLFields:
    """
    Contains static definitions of field names used in DGQL queries and responses.
    """

    DATA = "data"
    NODE = "V"
    EDGE = "E"
    SOURCE = "S"
    TARGET = "T"
    OUT = "OUT"
    IN = "IN"


class _UserDomain:
    """
    Domains in the user context that logically maps to different snowflake objects.
    """

    FEATURE_VIEW = "FEATURE_VIEW"
    MODEL = "MODULE"


class _SnowflakeDomain:
    """
    Snowflake object domains relevant for querying lineage.
    Note: This is a subset and does not include all possible domains.
    """

    TABLE = "TABLE"
    MODULE = "MODULE"
    DATASET = "DATASET"


class Lineage:
    """
    Provides methods for exploring lineage of Snowflake objects.
    To access an object of this class, use :attr:`Session.lineage`.
    """

    def __init__(self, session: "snowflake.snowpark.session.Session") -> None:
        self._session = session
        self._user_to_system_domain_map = {
            _UserDomain.FEATURE_VIEW: _SnowflakeDomain.TABLE,
            _UserDomain.MODEL: _SnowflakeDomain.MODULE,
        }
        self._versioned_object_domains = {
            _UserDomain.FEATURE_VIEW,
            _UserDomain.MODEL,
            _SnowflakeDomain.DATASET,
        }

    def _build_graphql_query(
        self,
        object_name: str,
        object_domain: str,
        edge_directions: list,
        object_version: Optional[str] = None,
    ) -> str:
        """
        Constructs a GraphQL query for lineage tracing based on the specified parameters.
        """
        properties_string = ", ".join(_ObjectField.GRAPH_ENTITY_PROPERTIES)
        edge_types_formatted = ", ".join(_EdgeType.values())

        parts = []
        edge_template = "{direction}: {edge_key}(edgeType:[{edge_types}],direction:{dir}){{{source_key} {{{properties}}}, {target_key} {{{properties}}}}}"
        for direction in edge_directions:
            dir_key = (
                _DGQLFields.OUT
                if direction == LineageDirection.DOWNSTREAM
                else _DGQLFields.IN
            )
            parts.append(
                edge_template.format(
                    edge_key=_DGQLFields.EDGE,
                    source_key=_DGQLFields.SOURCE,
                    target_key=_DGQLFields.TARGET,
                    direction=direction,
                    dir=dir_key,
                    edge_types=edge_types_formatted,
                    properties=properties_string,
                )
            )

        object_domain = self._user_to_system_domain_map.get(
            object_domain.lower(), object_domain
        )

        parent_param = ""
        query_object_name = object_name
        if object_version:
            parent_param = f', {_ObjectField.PARENT_NAME}:"{object_name}"'
            query_object_name = object_version

        query_template = '{{{nodeKey}({domainKey}: {domain}, {nameKey}:"{name}"{parent_param}) {{{edges}}}}}'
        query = query_template.format(
            nodeKey=_DGQLFields.NODE,
            domainKey=_ObjectField.DOMAIN,
            nameKey=_ObjectField.NAME,
            domain=object_domain.upper(),
            name=query_object_name,
            parent_param=parent_param,
            edges="".join(parts),
        )

        return f"select SYSTEM$DGQL('{query}')"

    def _get_lineage(
        self,
        object_name: str,
        object_domain: str,
        directions: list,
        object_version: Optional[str] = None,
        current_depth=1,
    ) -> List[Tuple[VariantType, VariantType, StringType, int]]:
        """
        Constructs and executes a query to trace the lineage of a given entity at a single depth level.
        """
        query_string = self._build_graphql_query(
            object_name, object_domain, directions, object_version
        )
        response = self._session.sql(query_string)
        json_response = json.loads(response.collect()[0][0])

        rows = []
        for direction in directions:
            edges = (
                json_response.get(_DGQLFields.DATA, {})
                .get(_DGQLFields.NODE, {})
                .get(direction.value, [])
            )

            for edge in edges:
                if _DGQLFields.SOURCE in edge and _DGQLFields.TARGET in edge:
                    rows.append(
                        (
                            edge[_DGQLFields.SOURCE],
                            edge[_DGQLFields.TARGET],
                            direction,
                            current_depth,
                        )
                    )

        return rows

    def _recursive_trace(
        self,
        object_name: str,
        object_domain: str,
        direction: str,
        current_depth: int,
        total_depth: int,
        object_version: Optional[str] = None,
    ) -> List[Tuple[VariantType, VariantType, StringType, int]]:
        """
        Recursively traces lineage by making successive DGQL queries based on response nodes.
        """
        if current_depth > total_depth:
            return []

        lineage_edges = self._get_lineage(
            object_name, object_domain, [direction], object_version, current_depth
        )
        if (
            current_depth == total_depth
            or not lineage_edges
            or self._is_terminal_entity(lineage_edges[0][0])
            or self._is_terminal_entity(lineage_edges[0][1])
        ):
            return lineage_edges

        next_object = (
            lineage_edges[0][1]
            if direction == LineageDirection.DOWNSTREAM
            else lineage_edges[0][0]
        )
        deeper_lineage_edges = self._recursive_trace(
            next_object[_ObjectField.NAME],
            next_object[_ObjectField.DOMAIN],
            direction,
            current_depth + 1,
            total_depth,
            next_object.get(_ObjectField.VERSION),
        )

        lineage_edges.extend(deeper_lineage_edges)
        return lineage_edges

    def _is_terminal_entity(self, entity) -> bool:
        """
        Determines if the entity should not be explored further.
        """
        return entity[_ObjectField.STATUS] in {"MASKED", "DELETED"}

    def _get_name_and_version(self, graph_entity):
        """
        Extracts and returns the name and version from the given graph entity.
        """
        if graph_entity[_ObjectField.USER_DOMAIN] in self._versioned_object_domains:
            if graph_entity[_ObjectField.USER_DOMAIN] == _UserDomain.FEATURE_VIEW:
                if "$" in graph_entity[_ObjectField.NAME]:
                    parts = graph_entity[_ObjectField.NAME].split("$")
                    return (
                        f"{graph_entity[_ObjectField.DB]}.{graph_entity[_ObjectField.SCHEMA]}.{parts[0]}",
                        parts[1],
                    )
                else:
                    raise SnowparkClientExceptionMessages.SERVER_FAILED_FETCH_LINEAGE(
                        f"unexpected {_UserDomain.FEATURE_VIEW} name format."
                    )
            elif (
                _ObjectField.PROPERTIES in graph_entity
                and _ObjectField.PARENT_NAME in graph_entity[_ObjectField.PROPERTIES]
            ):
                return (
                    f"{graph_entity[_ObjectField.DB]}.{graph_entity[_ObjectField.SCHEMA]}.{graph_entity[_ObjectField.PROPERTIES][_ObjectField.PARENT_NAME]}",
                    graph_entity[_ObjectField.NAME],
                )
            else:
                raise SnowparkClientExceptionMessages.SERVER_FAILED_FETCH_LINEAGE(
                    f"missing name/version field for domain {graph_entity[_ObjectField.USER_DOMAIN]}."
                )

        return (
            f"{graph_entity[_ObjectField.DB]}.{graph_entity[_ObjectField.SCHEMA]}.{graph_entity[_ObjectField.NAME]}",
            None,
        )

    def _get_user_entity(self, graph_entity) -> str:
        """
        Transforms the given graph entity into a user visible entity.
        """
        name, version = self._get_name_and_version(graph_entity)

        domain = (
            graph_entity.get(_ObjectField.USER_DOMAIN)
            or graph_entity.get(_ObjectField.REFINED_DOMAIN)
            or graph_entity.get(_ObjectField.DOMAIN)
        )

        if _ObjectField.CREATED_ON not in graph_entity:
            raise SnowparkClientExceptionMessages.SERVER_FAILED_FETCH_LINEAGE(
                f"missing {_ObjectField.CREATED_ON} property."
            )

        if _ObjectField.STATUS not in graph_entity:
            raise SnowparkClientExceptionMessages.SERVER_FAILED_FETCH_LINEAGE(
                f"missing {_ObjectField.STATUS} property."
            )

        user_entity = {
            _ObjectField.NAME: name,
            _ObjectField.DOMAIN: domain,
            _ObjectField.CREATED_ON: graph_entity[_ObjectField.CREATED_ON],
            _ObjectField.STATUS: graph_entity[_ObjectField.STATUS],
        }

        if version:
            user_entity[_ObjectField.VERSION] = version

        return user_entity

    def _get_result_dataframe(
        self, lineage_trace: List[Tuple[VariantType, VariantType, StringType, int]]
    ) -> "snowflake.snowpark.dataframe.DataFrame":
        """
        Constructs a dataframe of lineage results.
        """
        transformed_results = []

        for edge in lineage_trace:
            transformed_results.append(
                (
                    self._get_user_entity(edge[0]),
                    self._get_user_entity(edge[1]),
                    edge[2].value.capitalize(),
                    edge[3],
                )
            )

        schema = StructType(
            [
                StructField("source_object", VariantType()),
                StructField("target_object", VariantType()),
                StructField("lineage", StringType()),
                StructField("depth", IntegerType()),
            ]
        )
        return self._session.create_dataframe(transformed_results, schema=schema)

    def trace(
        self,
        object_name: str,
        object_domain: str,
        *,
        object_version: Optional[str] = None,
        direction: Union[str, LineageDirection] = LineageDirection.BOTH,
        depth: int = 2,
    ) -> "snowflake.snowpark.dataframe.DataFrame":
        """
        Traces the lineage of a object within Snowflake and returns it as a DataFrame.

        Args:
            object_name (str): The name of the Snowflake object to start trace, formatted as "database.schema.object".
            object_domain (str): The domain of the Snowflake object to start trace. e.g., "table", "view".
            object_version (Optional[str]): Version of the Snowflake object to start trace, defaults to None.
            direction (LineageDirection): The direction to trace (UPSTREAM, DOWNSTREAM, BOTH), defaults to BOTH.
            depth (int): The depth of the trace, defaults to 2.

        Returns:
            snowflake.snowpark.DataFrame: A DataFrame representing the traced lineage.

            Example:
                >>> db = "YOUR_DATABASE"
                >>> schema = "YOUR_SCHEMA"
                # Creating a table and views in the session
                >>> session.sql(f"CREATE OR REPLACE TABLE {db}.{schema}.T1(C1 INT)").collect()
                >>> session.sql(
                ...     f"CREATE OR REPLACE VIEW {db}.{schema}.V1 AS SELECT * FROM {db}.{schema}.T1"
                ... ).collect()
                >>> session.sql(
                ...     f"CREATE OR REPLACE VIEW {db}.{schema}.V2 AS SELECT * FROM {db}.{schema}.V1"
                ... ).collect()
                # Tracing the lineage of the table
                >>> df = session.lineage.trace(
                ...     f"{db}.{schema}.T1",
                ...     "table",
                ...     direction=LineageDirection.DOWNSTREAM
                ... )
                >>> df.show()
                ---------------------------------------------------------------------------------------------------------------------------------------
                | "SOURCE_OBJECT"                                        | "TARGET_OBJECT"                                    | "LINEAGE"   | "DEPTH" |
                ---------------------------------------------------------------------------------------------------------------------------------------
                | {"createdOn": "1712881232230", "domain": "TABLE",      | {"createdOn": "1712881232230", "domain": "VIEW",   | "Downstream"| 1       |
                |  "name": "YOUR_DATABASE.YOUR_SCHEMA.T1", "status":     |  "name": "YOUR_DATABASE.YOUR_SCHEMA.V1", "status": |             |         |
                |  "ACTIVE"}                                             |  "ACTIVE"}                                         |             |         |
                | {"createdOn": "1712881232230", "domain": "VIEW",       | {"createdOn": "1712881232230", "domain": "VIEW",   | "Downstream"| 2       |
                |  "name": "YOUR_DATABASE.YOUR_SCHEMA.V1", "status":     |  "name": "YOUR_DATABASE.YOUR_SCHEMA.V2", "status": |             |         |
                |  "ACTIVE"}                                             |  "ACTIVE"}                                         |             |         |
                ---------------------------------------------------------------------------------------------------------------------------------------
                <BLANKLINE>
        """
        if depth < 1 or depth > 10:
            raise ValueError("Depth must be between 1 and 10.")

        if isinstance(direction, str):
            direction = LineageDirection.value_of(direction)

        if direction == LineageDirection.BOTH and depth == 1:
            lineage_trace = self._get_lineage(
                object_name,
                object_domain,
                [LineageDirection.UPSTREAM, LineageDirection.DOWNSTREAM],
                object_version,
            )
        else:
            directions = (
                [LineageDirection.UPSTREAM, LineageDirection.DOWNSTREAM]
                if direction == LineageDirection.BOTH
                else [direction]
            )
            lineage_trace = []
            for dir in directions:
                lineage_trace.extend(
                    self._recursive_trace(
                        object_name, object_domain, dir, 1, depth, object_version
                    )
                )

        return self._get_result_dataframe(lineage_trace)
