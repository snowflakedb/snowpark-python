#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
import logging
from functools import cached_property
from typing import Optional, Union, List, Any

from snowflake.snowpark._internal.data_source import DataSourcePartitioner
from snowflake.snowpark._internal.data_source.drivers import BaseDriver
from snowflake.snowpark.types import (
    StructType,
    DecimalType,
    FloatType,
    StringType,
    DateType,
    BooleanType,
    ArrayType,
    StructField,
)
from snowflake.snowpark.session import Session
from snowflake.snowpark.dataframe import DataFrame

logger = logging.getLogger(__name__)

# reference for java type to snowflake type:
# https://docs.snowflake.com/en/developer-guide/udf-stored-procedure-data-type-mapping

JDBC_TYPE_TO_SNOWFLAKE_TYPE = {
    "java.math.BigDecimal": DecimalType,
    "java.math.BigInteger": DecimalType,
    "java.lang.Float": FloatType,
    "java.lang.Double": FloatType,
    "java.lang.Boolean": BooleanType,
    "java.lang.String": StringType,
    "java.sql.Timestamp": StringType,
    "java.sql.Date": DateType,
    "java.sql.Array": ArrayType,
    "java.lang.String[]": ArrayType,
    "java.lang.Long": DecimalType,
    "java.lang.Integer": DecimalType,
    "java.lang.Short": DecimalType,
}
PARTITION_TABLE_COLUMN_NAME = "partition"


def to_snow_type(raw_schema: List[Any]) -> StructType:
    fields = []
    for (field_name, field_type, precision, scale, nullable) in raw_schema:
        snow_type = JDBC_TYPE_TO_SNOWFLAKE_TYPE.get(field_type, StringType)
        if snow_type == DecimalType:
            if not BaseDriver.validate_numeric_precision_scale(precision, scale):
                logger.debug(
                    f"Snowpark does not support column"
                    f" {field_name} of type {field_type} with precision {precision} and scale {scale}. "
                    "The default Numeric precision and scale will be used."
                )
                precision, scale = None, None
            data_type = snow_type(
                precision if precision is not None else 38,
                scale if scale is not None else 0,
            )
        else:
            data_type = snow_type()

        fields.append(StructField(field_name, data_type, nullable))
    return StructType(fields)


def generate_select_sql(table_or_query: str, raw_schema: List[str], is_query: bool):
    if is_query:
        return table_or_query
    else:
        cols = [col[0] for col in raw_schema]
        return f"SELECT {', '.join(cols)} FROM {table_or_query}"


class JDBCClient:
    def __init__(
        self,
        session: "Session",
        url: str,
        table_or_query: str,
        external_access_integration: str,
        imports: List[str],
        is_query: bool,
        packages: Optional[List[str]] = None,
        column: Optional[str] = None,
        lower_bound: Optional[Union[str, int]] = None,
        upper_bound: Optional[Union[str, int]] = None,
        num_partitions: Optional[int] = None,
        query_timeout: Optional[int] = 0,
        fetch_size: Optional[int] = 0,
        custom_schema: Optional[Union[str, StructType]] = None,
        predicates: Optional[List[str]] = None,
        session_init_statement: Optional[List[str]] = None,
        _emit_ast: bool = True,
    ) -> None:
        self.session = session
        self.url = url
        self.table_or_query = table_or_query

        self.external_access_integration = external_access_integration
        self.imports = imports
        self.packages = packages

        self.is_query = is_query
        self.column = column
        self.lower_bound = lower_bound
        self.upper_bound = upper_bound
        self.num_partitions = num_partitions
        self.query_timeout = query_timeout
        self.fetch_size = fetch_size
        self.custom_schema = custom_schema
        self.predicates = predicates
        self.session_init_statement = session_init_statement
        self.raw_schema = None
        self._emit_ast = _emit_ast
        self.imports = (
            f"""IMPORTS =({",".join([f"'{imp}'" for imp in self.imports])})"""
            if self.imports is not None
            else ""
        )
        self.packages = (
            f"""PACKAGES=('com.snowflake:snowpark:latest', {','.join([f"'{pack}'" for pack in self.packages])})"""
            if self.packages is not None
            else "PACKAGES=('com.snowflake:snowpark:latest')"
        )

    @cached_property
    def schema(self) -> StructType:

        infer_schema_udtf_registration = f"""
            CREATE OR REPLACE FUNCTION jdbc_test(query VARCHAR)
            RETURNS TABLE (field_name VARCHAR, field_type VARCHAR, precision INTEGER, scale INTEGER, nullable BOOLEAN)
            LANGUAGE JAVA
            RUNTIME_VERSION = '11'
            EXTERNAL_ACCESS_INTEGRATIONS=({self.external_access_integration})
            {self.imports}
            {self.packages}
            HANDLER = 'DataLoader'
            as
            $$

            import java.sql.*;
            import java.util.stream.Stream;
            import java.util.ArrayList;
            import java.util.List;
            import com.snowflake.snowpark_java.types.*;


            class OutputRow {{

                public String field_name;
                public String field_type;
                public int precision;
                public int scale;
                public boolean nullable;

            }}

            public class DataLoader{{
                private final Connection conn;

                private static Connection createConnection() {{
                    try {{
                        String url = "{self.url}";
                        return DriverManager.getConnection(url);
                    }} catch (Exception e) {{
                        throw new RuntimeException("Failed to create JDBC connection: " + e.getMessage());
                    }}
                }}
                public static Class<?> getOutputClass() {{
                    return OutputRow.class;
                }}

                public DataLoader() {{
                    this.conn = createConnection();
                }}

                public Stream<OutputRow> process(String query) {{
                    List<OutputRow> list = new ArrayList<>();
                     try {{
                        PreparedStatement stmt = this.conn.prepareStatement(query);
                        // Avoid fetching data – get only metadata
                        stmt.setMaxRows(1);
                        try (ResultSet rs = stmt.executeQuery()) {{
                            ResultSetMetaData meta = rs.getMetaData();
                            int columnCount = meta.getColumnCount();

                            for (int i = 1; i <= columnCount; i++) {{
                                OutputRow row = new OutputRow();

                                row.field_name = meta.getColumnName(i);
                                row.field_type = meta.getColumnClassName(i);
                                row.precision = meta.getPrecision(i);
                                row.scale = meta.getScale(i);
                                row.nullable = meta.isNullable(i) == ResultSetMetaData.columnNullable;
                                list.add(row);
                            }}
                        }}
                    }} catch (SQLException e) {{
                        throw new RuntimeException("SQL error: " + e.getMessage(), e);
                    }}

                    return list.stream();
                }}

                public Stream<OutputRow> endPartition() {{
                    return Stream.empty();
                }}
            }}
            $$
            ;
            """
        infer_schema_sql = (
            f"SELECT * FROM ({self.table_or_query}) WHERE 1 = 0"
            if self.is_query
            else f"SELECT * FROM {self.table_or_query} WHERE 1 = 0"
        )

        self.session.sql(
            infer_schema_udtf_registration, _emit_ast=self._emit_ast
        ).collect()
        self.raw_schema = self.session.sql(
            f"SELECT * FROM TABLE(jdbc_test('{infer_schema_sql}'))",
            _emit_ast=self._emit_ast,
        ).collect()

        return to_snow_type(self.raw_schema)

    @cached_property
    def partitions(self) -> List[str]:
        select_query = generate_select_sql(
            self.table_or_query,
            self.raw_schema,
            self.is_query,
        )
        logger.debug(f"Generated select query: {select_query}")

        return DataSourcePartitioner.generate_partitions(
            select_query,
            self.schema,
            self.predicates,
            self.column,
            self.lower_bound,
            self.upper_bound,
            self.num_partitions,
        )

    def read(self, partition_table: str) -> "DataFrame":
        udtf_table_return_type = ", ".join(
            [f"{field.name} VARCHAR" for field in self.schema.fields]
        )
        output_rows = "".join(
            [f"public String {field.name};\n" for field in self.schema.fields]
        )
        create_output_row = "".join(
            [
                f"row.{field.name} = rs.getString({i+1});\n"
                for i, field in enumerate(self.schema.fields)
            ]
        )
        jdbc_udtf_registration = f"""
            CREATE OR REPLACE FUNCTION jdbc(query VARCHAR)
            RETURNS TABLE ({udtf_table_return_type})
            LANGUAGE JAVA
            RUNTIME_VERSION = '11'
            EXTERNAL_ACCESS_INTEGRATIONS=({self.external_access_integration})
            {self.imports}
            {self.packages}
            HANDLER = 'DataLoader'
            as
            $$

            import java.sql.*;
            import java.util.stream.Stream;
            import java.util.ArrayList;
            import java.util.List;
            import com.snowflake.snowpark_java.types.*;


            class OutputRow {{
                {output_rows}
            }}

            public class DataLoader{{
            private final Connection conn;

            private static Connection createConnection() {{
                try {{
                    String url = "{self.url}";
                    return DriverManager.getConnection(url);
                }} catch (Exception e) {{
                    throw new RuntimeException("Failed to create JDBC connection: " + e.getMessage());
                }}
            }}
            public static Class<?> getOutputClass() {{
                return OutputRow.class;
            }}

            public DataLoader() {{
                this.conn = createConnection();
            }}

            public Stream<OutputRow> process(String query) {{
                List<OutputRow> list = new ArrayList<>();
                 try {{
                    PreparedStatement stmt = this.conn.prepareStatement(query);
                    try (ResultSet rs = stmt.executeQuery()) {{
                        while (rs.next()) {{
                            OutputRow row = new OutputRow();
                            {create_output_row}
                            list.add(row);
                        }}
                    }}
                }} catch (SQLException e) {{
                    throw new RuntimeException("SQL error: " + e.getMessage(), e);
                }}

                return list.stream();
            }}

            public Stream<OutputRow> endPartition() {{
                return Stream.empty();
            }}
        }}
        $$
        ;
        """

        jdbc_udtf = f"""
            select result.* from {partition_table}, table(jdbc({PARTITION_TABLE_COLUMN_NAME})) AS result
            """

        self.session.sql(jdbc_udtf_registration, _emit_ast=self._emit_ast).collect()
        return BaseDriver.to_result_snowpark_df_udtf(
            self.session.sql(jdbc_udtf, _emit_ast=self._emit_ast),
            self.schema,
            _emit_ast=self._emit_ast,
        )
