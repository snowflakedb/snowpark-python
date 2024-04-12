#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

import json
from typing import List, Optional, Tuple

import snowflake.snowpark
from snowflake.snowpark.types import StringType, StructField, StructType, VariantType


class LineageDirection:
    DOWNSTREAM = "downstream"
    UPSTREAM = "upstream"
    BOTH = "both"


class Lineage:
    """
    Provides methods for exploring lineage of Snowflake entities.
    To access an object of this class, use :attr:`Session.lineage`.
    """

    def __init__(self, session: "snowflake.snowpark.session.Session") -> None:
        self._session = session
        self._edge_types = ["DATA_LINEAGE", "OBJECT_DEPENDENCY"]
        self._properties = [
            "domain",
            "refinedDomain",
            "userDomain",
            "name",
            "schema",
            "db",
            "status",
        ]
        self._edge_template = "{direction}: E(edgeType:[{edge_types}],direction:{dir}){{S {{{properties}}}, T {{{properties}}}}}"
        self._query_template = (
            '{{V(domain: {domain}, name:"{name}"{parent_param}) {{{edges}}}}}'
        )
        self._both_directions = [LineageDirection.UPSTREAM, LineageDirection.DOWNSTREAM]
        self.user_to_system_domain_map = {"feature_view": "table", "model": "module"}

    def _build_graphql_query(
        self,
        entity_name: str,
        entity_domain: str,
        edge_directions: list,
        entity_version: Optional[str] = None,
    ) -> str:
        """
        Constructs a GraphQL query for lineage tracing based on the specified parameters.
        """
        properties_string = ", ".join(self._properties)
        edge_types_formatted = ", ".join(self._edge_types)

        parts = []
        for direction in edge_directions:
            dir_text = "OUT" if direction == LineageDirection.DOWNSTREAM else "IN"
            parts.append(
                self._edge_template.format(
                    direction=direction,
                    dir=dir_text,
                    edge_types=edge_types_formatted,
                    properties=properties_string,
                )
            )

        entity_domain = self.user_to_system_domain_map.get(entity_domain, entity_domain)

        parent_param = ""
        query_entity_name = entity_name
        if entity_version:
            parent_param = f', parentName:"{entity_name}"'
            query_entity_name = entity_version

        edge_queries = "".join(parts)
        query = self._query_template.format(
            domain=entity_domain.upper(),
            name=query_entity_name,
            parent_param=parent_param,
            edges=edge_queries,
        )

        return f"select SYSTEM$DGQL('{query}')"

    def _get_lineage(
        self,
        entity_name: str,
        entity_domain: str,
        directions: list,
        entity_version: Optional[str] = None,
    ) -> List[Tuple[VariantType, VariantType, StringType]]:
        """
        Constructs and executes a query to trace the lineage of a given entity at a single depth level.
        """
        query_string = self._build_graphql_query(
            entity_name, entity_domain, directions, entity_version
        )
        response = self._session.sql(query_string)
        json_response = json.loads(response.collect()[0][0])

        rows = []
        for direction in directions:
            edges = json_response.get("data", {}).get("V", {}).get(direction, [])
            for edge in edges:
                if "S" in edge and "T" in edge:
                    rows.append((edge["S"], edge["T"], direction.capitalize()))

        return rows

    def _recursive_trace(
        self,
        entity_name: str,
        entity_domain: str,
        direction: str,
        depth: int,
        entity_version: Optional[str] = None,
    ) -> List[Tuple[VariantType, VariantType, StringType]]:
        """
        Recursively traces lineage by making successive queries based on response nodes.
        """
        if depth == 0:
            return []

        current_depth_results = self._get_lineage(
            entity_name, entity_domain, [direction], entity_version
        )
        if depth == 1 or not current_depth_results:
            return current_depth_results

        next_entity = (
            current_depth_results[0][1]
            if direction == LineageDirection.DOWNSTREAM
            else current_depth_results[0][0]
        )
        deeper_results = self._recursive_trace(
            next_entity["name"],
            next_entity["domain"],
            direction,
            depth - 1,
            next_entity.get("version"),
        )

        current_depth_results.extend(deeper_results)
        return current_depth_results

    def trace(
        self,
        entity_name: str,
        entity_domain: str,
        *,
        entity_version: Optional[str] = None,
        direction: LineageDirection = LineageDirection.BOTH,
        depth: int = 2,
    ) -> "snowflake.snowpark.dataframe.DataFrame":
        """
        Traces the lineage of a source entity within Snowflake and returns it as a DataFrame.

        Args:
            entity_name (str): The name of the entity to start trace.
            entity_domain (str): The domain of the entity to start trace.
            entity_version (Optional[str]): Version of the entity to start trace, defaults to None.
            direction (LineageDirection): The direction to trace (upstream, downstream, both), defaults to both.
            depth (int): The depth of the trace, defaults to 2.

        Returns:
            snowflake.snowpark.DataFrame: A DataFrame representing the traced lineage.
        """
        if not entity_name:
            raise ValueError("Entity name must be provided.")
        if not entity_domain:
            raise ValueError("Entity type must be provided.")

        if depth < 1 or depth > 5:
            raise ValueError("Depth must be between 1 and 5.")

        if direction == LineageDirection.BOTH and depth == 1:
            directions = [LineageDirection.UPSTREAM, LineageDirection.DOWNSTREAM]
            final_results = self._get_lineage(
                entity_name, entity_domain, directions, entity_version
            )
        else:
            directions = (
                [direction]
                if direction != LineageDirection.BOTH
                else [LineageDirection.UPSTREAM, LineageDirection.DOWNSTREAM]
            )
            final_results = []
            for dir in directions:
                final_results.extend(
                    self._recursive_trace(
                        entity_name, entity_domain, dir, depth, entity_version
                    )
                )

        schema = StructType(
            [
                StructField("sourceEntity", VariantType()),
                StructField("targetEntity", VariantType()),
                StructField("lineage", StringType()),
            ]
        )
        return self._session.create_dataframe(final_results, schema=schema)
