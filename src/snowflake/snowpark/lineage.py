#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import datetime
import json
import re
from collections import deque
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple, Union

import snowflake.snowpark
from snowflake.snowpark._internal.error_message import SnowparkClientExceptionMessages
from snowflake.snowpark.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    VariantType,
)

_MIN_TRACE_DISTANCE = 1
_MAX_TRACE_DISTANCE = 5
_DEFAULT_TRACE_DISTANCE = 2


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
    PARENT_NAME = "parentName"
    PARENT_NAME_DEPRECATED = "ParentName"
    VERSION = "version"
    ID = "id"
    PARENT_ID = "parentId"
    PARENT_ID_DEPRECATED = "ParentId"
    TABLE_TYPE = "tableType"
    TYPE = "type"

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
        ID,
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
    MODEL = "MODEL"
    SERVICE = "SERVICE"


class _SnowflakeDomain:
    """
    Snowflake object domains relevant for querying lineage.
    Note: This is a subset and does not include all possible domains.
    """

    TABLE = "TABLE"
    MODULE = "MODULE"
    DATASET = "DATASET"
    VIEW = "VIEW"
    COLUMN = "COLUMN"
    SNOWSERVICE_INSTANCE = "SNOWSERVICE_INSTANCE"


class _DGQLQueryBuilder:
    """
    Provides methods for building DGQL query.
    """

    EDGE_TEMPLETE = "{direction}: {edge_key}(edgeType:[{edge_types}],direction:{dir}){{{source_key} {{{properties}}}, {target_key} {{{properties}}}}}"
    QUERY_TEMPLETE = '{{{nodeKey}({domainKey}: {domain}, {object_key}:"{query_object}"{parent_param}) {{{edges}}}}}'
    USER_TO_SYSTEM_DOMAIN_MAP = {
        _UserDomain.FEATURE_VIEW: _SnowflakeDomain.TABLE,
        _UserDomain.MODEL: _SnowflakeDomain.MODULE,
        _UserDomain.SERVICE: _SnowflakeDomain.SNOWSERVICE_INSTANCE,
    }

    @staticmethod
    def build_query(
        object_domain: str,
        edge_directions: List[LineageDirection],
        object_name: Optional[str] = None,
        object_id: Optional[str] = None,
        object_version: Optional[str] = None,
        parent_id: Optional[str] = None,
    ) -> str:
        """
        Builds fully executable DGQL query either by id or by name.
        """
        if (object_id and object_name) or (not object_id and not object_name):
            raise ValueError("Either object_name or object_id must be provided")

        properties_string = ", ".join(_ObjectField.GRAPH_ENTITY_PROPERTIES)
        edge_types_formatted = ", ".join(_EdgeType.values())

        parts = []
        for direction in edge_directions:

            dir_key = (
                _DGQLFields.OUT
                if direction == LineageDirection.DOWNSTREAM
                else _DGQLFields.IN
            )
            parts.append(
                _DGQLQueryBuilder.EDGE_TEMPLETE.format(
                    edge_key=_DGQLFields.EDGE,
                    source_key=_DGQLFields.SOURCE,
                    target_key=_DGQLFields.TARGET,
                    direction=direction.value,
                    dir=dir_key,
                    edge_types=edge_types_formatted,
                    properties=properties_string,
                )
            )

        parent_param = ""
        if object_id:
            object_key = "id"
            query_object = object_id
            if parent_id:
                parent_param = f', parentId:"{parent_id}"'
        else:
            object_key = "name"
            if object_domain == _UserDomain.FEATURE_VIEW:
                if not object_version:
                    raise ValueError(
                        f"Version cant be empty for {_UserDomain.FEATURE_VIEW}"
                    )
                object_name = _DGQLQueryBuilder._get_feature_view_name(
                    object_name, object_version
                )
                object_version = None
            object_name = object_name.replace('"', '\\\\"')
            query_object = object_name
            if object_version:
                object_version = object_version.replace('"', '\\\\"')
                query_object = object_version
                parent_param = f', parentName:"{object_name}"'

        object_domain = _DGQLQueryBuilder.USER_TO_SYSTEM_DOMAIN_MAP.get(
            object_domain.upper(), object_domain
        )

        query = _DGQLQueryBuilder.QUERY_TEMPLETE.format(
            nodeKey=_DGQLFields.NODE,
            domainKey=_ObjectField.DOMAIN,
            domain=object_domain.upper(),
            object_key=object_key,
            query_object=query_object,
            parent_param=parent_param,
            edges="".join(parts),
        )
        return f"select SYSTEM$DGQL('{query}')"

    @staticmethod
    def split_fully_qualified_name(name):
        """
        Splits the fully qualified name.
        Pattern matches either a string enclosed in double quotes or a sequence of word characters.
        """
        parts = re.findall(r'"[^"]*"|\w+', name)
        return parts

    @staticmethod
    def _get_feature_view_name(name: str, version: str) -> str:
        """
        Constructs feature view name.
        """
        parts = _DGQLQueryBuilder.split_fully_qualified_name(name)
        if len(parts) != 3:
            raise ValueError("Invalid object name: less than three parts")

        name_part = parts[2]

        # Feature view name is case SQL Identifier
        if name_part.startswith('"') and name_part.endswith('"'):
            name_part = name_part.strip('"')
        else:
            name_part = name_part.upper()

        # Feature view version is case sensitive.
        feature_view_name = f'"{name_part}${version}"'

        return ".".join(parts[:2] + [feature_view_name])


class Lineage:
    """
    Provides methods for exploring lineage of Snowflake objects.
    To access an object of this class, use :attr:`Session.lineage`.
    """

    def __init__(self, session: "snowflake.snowpark.session.Session") -> None:
        self._session = session
        self._versioned_object_domains = {
            _UserDomain.FEATURE_VIEW,
            _UserDomain.MODEL,
            _SnowflakeDomain.DATASET,
        }

    def _get_lineage(
        self,
        query_string: str,
        direction: LineageDirection,
        current_distance=1,
    ) -> List[Tuple[VariantType, VariantType, StringType, int]]:
        """
        Constructs and executes a query to trace the lineage of a given entity at a distance one.
        """
        response = self._session.sql(query_string)
        json_response = json.loads(response.collect()[0][0])
        rows = []
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
                        current_distance,
                    )
                )

        return rows

    def _trace(
        self,
        object_name: str,
        object_domain: str,
        direction: LineageDirection,
        total_distance: int,
        object_version: Optional[str] = None,
    ) -> List[Tuple[VariantType, VariantType, StringType, int]]:
        """
        Traces lineage by making successive DGQL queries based on response nodes using BFS.
        """
        visited = set()
        results = []
        queue = deque()

        lineage_edges = self._get_lineage(
            _DGQLQueryBuilder.build_query(
                object_domain,
                [direction],
                object_name=object_name,
                object_version=object_version,
            ),
            direction,
            current_distance=1,
        )

        self._process_lineage_edges(lineage_edges, direction, queue, results, 1)

        while queue:
            (
                object_domain,
                object_id,
                parent_id,
                current_distance,
            ) = queue.popleft()

            if current_distance == total_distance:
                continue

            current_node = (
                object_domain,
                object_id,
                parent_id,
                current_distance,
            )
            if current_node in visited:
                continue

            visited.add(current_node)

            lineage_edges = self._get_lineage(
                _DGQLQueryBuilder.build_query(
                    object_domain, [direction], object_id=object_id, parent_id=parent_id
                ),
                direction,
                current_distance + 1,
            )

            self._process_lineage_edges(
                lineage_edges, direction, queue, results, current_distance + 1
            )

        return results

    def _process_lineage_edges(
        self,
        lineage_edges: List[Tuple[VariantType, VariantType, StringType, int]],
        direction: LineageDirection,
        queue: deque,
        results: List[Tuple[VariantType, VariantType, StringType, int]],
        current_distance: int,
    ) -> None:
        """
        Process lineage edges and update the queue accordingly.
        """
        if not lineage_edges:
            return

        results.extend(lineage_edges)

        for edge in lineage_edges:
            next_object = (
                edge[1] if direction == LineageDirection.DOWNSTREAM else edge[0]
            )
            parent_id = None
            if _ObjectField.PROPERTIES in next_object:
                properties = next_object[_ObjectField.PROPERTIES]
                if _ObjectField.PARENT_ID in properties:
                    parent_id = properties[_ObjectField.PARENT_ID]
                elif _ObjectField.PARENT_ID_DEPRECATED in properties:
                    parent_id = properties[_ObjectField.PARENT_ID_DEPRECATED]
            queue.append(
                (
                    next_object[_ObjectField.DOMAIN],
                    next_object[_ObjectField.ID],
                    parent_id,
                    current_distance,
                )
            )

    def _is_terminal_entity(self, entity: Dict[str, Any]) -> bool:
        """
        Determines if the entity should not be explored further.
        """
        return entity[_ObjectField.STATUS] in {"MASKED", "DELETED"}

    def _get_name_and_version(self, graph_entity: Dict[str, Any]):
        """
        Extracts and returns the name and version from the given graph entity.
        """
        user_domain = graph_entity[_ObjectField.USER_DOMAIN]
        db = graph_entity[_ObjectField.DB]
        schema = graph_entity[_ObjectField.SCHEMA]
        name = graph_entity[_ObjectField.NAME]

        if user_domain in self._versioned_object_domains:
            if user_domain == _UserDomain.FEATURE_VIEW:
                if "$" in name:
                    had_quotes = name.startswith('"') and name.endswith('"')
                    parts = name.strip('"').split("$")
                    if len(parts) >= 2:
                        base_name = "$".join(parts[:-1])
                        version = parts[-1]
                        if had_quotes:
                            base_name = f'"{base_name}"'
                        return (f"{db}.{schema}.{base_name}", version)
                else:
                    raise SnowparkClientExceptionMessages.SERVER_FAILED_FETCH_LINEAGE(
                        f"unexpected {_UserDomain.FEATURE_VIEW} name format."
                    )
            elif _ObjectField.PROPERTIES in graph_entity:
                properties = graph_entity[_ObjectField.PROPERTIES]
                if _ObjectField.PARENT_NAME in properties:
                    parent_name = properties[_ObjectField.PARENT_NAME]
                elif (
                    _ObjectField.PARENT_NAME_DEPRECATED
                    in graph_entity[_ObjectField.PROPERTIES]
                ):
                    parent_name = properties[_ObjectField.PARENT_NAME_DEPRECATED]
                else:
                    raise SnowparkClientExceptionMessages.SERVER_FAILED_FETCH_LINEAGE(
                        f"missing name/version field for domain {graph_entity[_ObjectField.USER_DOMAIN]}."
                    )
                return (f"{db}.{schema}.{parent_name}", name)
            else:
                raise SnowparkClientExceptionMessages.SERVER_FAILED_FETCH_LINEAGE(
                    f"missing name/version field for domain {graph_entity[_ObjectField.USER_DOMAIN]}."
                )

        if (
            user_domain == _SnowflakeDomain.COLUMN
            and _ObjectField.PROPERTIES in graph_entity
            and _ObjectField.PARENT_NAME in graph_entity[_ObjectField.PROPERTIES]
        ):
            properties = graph_entity[_ObjectField.PROPERTIES]
            return (
                f"{db}.{schema}.{properties[_ObjectField.PARENT_NAME]}.{name}",
                None,
            )

        return (f"{db}.{schema}.{name}", None)

    def _get_user_entity(self, graph_entity: Dict[str, Any]) -> str:
        """
        Transforms the given graph entity into a user visible entity.
        """
        name, version = self._get_name_and_version(graph_entity)

        domain = (
            graph_entity.get(_ObjectField.USER_DOMAIN)
            or graph_entity.get(_ObjectField.REFINED_DOMAIN)
            or graph_entity.get(_ObjectField.DOMAIN)
        )

        # TODO: Remove this workaround once version 8.18 is deployed.
        if (
            graph_entity.get(_ObjectField.USER_DOMAIN) == _SnowflakeDomain.TABLE
            and graph_entity.get(_ObjectField.REFINED_DOMAIN) == _SnowflakeDomain.VIEW
        ):
            domain = _SnowflakeDomain.VIEW

        if _ObjectField.CREATED_ON not in graph_entity:
            raise SnowparkClientExceptionMessages.SERVER_FAILED_FETCH_LINEAGE(
                f"missing {_ObjectField.CREATED_ON} property."
            )

        if _ObjectField.STATUS not in graph_entity:
            raise SnowparkClientExceptionMessages.SERVER_FAILED_FETCH_LINEAGE(
                f"missing {_ObjectField.STATUS} property."
            )

        timestamp = int(graph_entity[_ObjectField.CREATED_ON]) / 1000
        dt_utc = datetime.datetime.fromtimestamp(timestamp, tz=datetime.timezone.utc)
        # ISO 8601 format for UTC
        formatted_date_iso = dt_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
        user_entity = {
            _ObjectField.NAME: name,
            _ObjectField.DOMAIN: domain,
            _ObjectField.CREATED_ON: formatted_date_iso,
            _ObjectField.STATUS: graph_entity[_ObjectField.STATUS],
        }

        if version:
            user_entity[_ObjectField.VERSION] = version

        if domain == _SnowflakeDomain.COLUMN:
            if _ObjectField.PROPERTIES in graph_entity:
                properties = graph_entity[_ObjectField.PROPERTIES]
                if _ObjectField.TABLE_TYPE in properties:
                    user_entity[_ObjectField.TYPE] = properties[_ObjectField.TABLE_TYPE]

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
                StructField("direction", StringType()),
                StructField("distance", IntegerType()),
            ]
        )
        return self._session.create_dataframe(transformed_results, schema=schema)

    def _check_valid_object_name(self, object_name: str, object_domain: str) -> None:
        """
        Checks if the object name is one of the below allowed format
            Non-Case-sensitive: "database.schema.object" or "database.schema.object.column_name"
            Case-sensitive: "\"database\".\"schema\".\"object\"" or "\"database\".\"schema\".\"object\"".\"column_name\"
        """
        parts = _DGQLQueryBuilder.split_fully_qualified_name(object_name)

        is_column_domain = object_domain.upper() == _SnowflakeDomain.COLUMN
        if (is_column_domain and len(parts) != 4) or (
            not is_column_domain and len(parts) != 3
        ):
            raise ValueError(f"Invalid object name: {object_name}")

        for part in parts:
            # Check if the part matches the pattern for a quoted string (starts and ends with double quotes),
            # or matches the pattern for a word (consists of alphanumeric characters and underscores).
            if not re.match(r'^"[^"]*"$|\w+', part):
                raise ValueError(f"Invalid object name: {object_name}")

    def trace(
        self,
        object_name: str,
        object_domain: str,
        *,
        object_version: Optional[str] = None,
        direction: Union[str, LineageDirection] = LineageDirection.BOTH,
        distance: int = _DEFAULT_TRACE_DISTANCE,
    ) -> "snowflake.snowpark.dataframe.DataFrame":
        """
        Traces the lineage of an object within Snowflake and returns it as a DataFrame.

        Args:
            object_name (str): The fully qualified name of the Snowflake object to start trace, formatted as below:
                    Non-Case-sensitive: "database.schema.object"
                    Case-sensitive: "\"database\".\"schema\".\"object\""
            object_domain (str): The domain of the Snowflake object to start trace. e.g., "table", "view".
            object_version (Optional[str]):Version of the versioned Snowflake object (e.g., model or dataset) to begin tracing. Defaults to None.
            direction (LineageDirection): The direction to trace (UPSTREAM, DOWNSTREAM, BOTH), defaults to BOTH.
            distance (int): Trace distance, defaults to 2, with a maximum of 5.

        Returns:
            snowflake.snowpark.DataFrame: A DataFrame representing the traced lineage with the following schema:
                - source (str): The source of the lineage.
                - target (str): The target of the lineage.
                - direction (str): The direction of the lineage ('upstream', 'downstream', or 'both').
                - distance (int): The distance of the lineage tracing from given object.

            Example:
                >>> db = session.get_current_database().replace('"', "")
                >>> schema = session.get_current_schema().replace('"', "")
                >>> _ = session.sql(f"CREATE OR REPLACE TABLE {db}.{schema}.T1(C1 INT)").collect()
                >>> _ = session.sql(
                ...     f"CREATE OR REPLACE VIEW {db}.{schema}.V1 AS SELECT * FROM {db}.{schema}.T1"
                ... ).collect()
                >>> _ = session.sql(
                ...     f"CREATE OR REPLACE VIEW {db}.{schema}.V2 AS SELECT * FROM {db}.{schema}.V1"
                ... ).collect()
                >>> df = session.lineage.trace(
                ...     f"{db}.{schema}.T1",
                ...     "table",
                ...     direction="downstream"
                ... )
                >>> df.show() # doctest: +SKIP
                -------------------------------------------------------------------------------------------------------------------------------------------------
                | "SOURCE_OBJECT"                                         | "TARGET_OBJECT"                                        | "DIRECTION"   | "DISTANCE" |
                -------------------------------------------------------------------------------------------------------------------------------------------------
                | {"createdOn": "2023-11-15T12:30:23Z", "domain": "TABLE",| {"createdOn": "2023-11-15T12:30:23Z", "domain": "VIEW",| "Downstream"  | 1          |
                |  "name": "YOUR_DATABASE.YOUR_SCHEMA.T1", "status":      |  "name": "YOUR_DATABASE.YOUR_SCHEMA.V1", "status":     |               |            |
                |  "ACTIVE"}                                              |  "ACTIVE"}                                             |               |            |
                | {"createdOn": "2023-11-15T12:30:23Z", "domain": "VIEW", | {"createdOn": "2023-11-15T12:30:23Z", "domain": "VIEW",| "Downstream"  | 2          |
                |  "name": "YOUR_DATABASE.YOUR_SCHEMA.V1", "status":      |  "name": "YOUR_DATABASE.YOUR_SCHEMA.V2", "status":     |               |            |
                |  "ACTIVE"}                                              |  "ACTIVE"}                                             |               |            |
                -------------------------------------------------------------------------------------------------------------------------------------------------
                <BLANKLINE>
        """
        if distance < _MIN_TRACE_DISTANCE or distance > _MAX_TRACE_DISTANCE:
            raise ValueError(
                f"Distance must be between {_MIN_TRACE_DISTANCE} and {_MAX_TRACE_DISTANCE}."
            )

        self._check_valid_object_name(object_name, object_domain)

        if isinstance(direction, str):
            direction = LineageDirection.value_of(direction)

        directions = (
            [LineageDirection.UPSTREAM, LineageDirection.DOWNSTREAM]
            if direction == LineageDirection.BOTH
            else [direction]
        )
        lineage_trace = []
        for dir in directions:
            lineage_trace.extend(
                self._trace(object_name, object_domain, dir, distance, object_version)
            )

        return self._get_result_dataframe(lineage_trace)
