#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
from functools import cached_property
from typing import Optional, Union, List

from snowflake.snowpark.types import StructType
from snowflake.snowpark.session import Session


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

    @cached_property
    def schema(self) -> StructType:
        imports = (
            f"""IMPORTS =({",".join([f"'{imp}'" for imp in self.imports])})"""
            if self.imports is not None
            else ""
        )
        packages = (
            f"""PACKAGES=('com.snowflake:snowpark:latest', {','.join([f"'{pack}'" for pack in self.packages])})"""
            if self.packages is not None
            else "PACKAGES=('com.snowflake:snowpark:latest')"
        )

        INFER_SCHEMA_UDTF = f"""
            CREATE OR REPLACE FUNCTION jdbc_test(query VARCHAR)
            RETURNS TABLE (field_name VARCHAR, field_type VARCHAR, nullable BOOLEAN)
            LANGUAGE JAVA
            RUNTIME_VERSION = '11'
            EXTERNAL_ACCESS_INTEGRATIONS=({self.external_access_integration})
            {imports}
            {packages}
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

                public StructType outputSchema() {{
                    return StructType.create(
                        new StructField("field_name", DataTypes.StringType),
                        new StructField("field_type", DataTypes.StringType),
                        new StructField("nullable", DataTypes.BooleanType)
                    );
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

                                String name = meta.getColumnName(i);
                                String type = meta.getColumnTypeName(i);
                                boolean nullable = meta.isNullable(i) == ResultSetMetaData.columnNullable;
                                row.field_name = name;
                                row.field_type = type;
                                row.nullable = nullable;
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

        self.session.sql(INFER_SCHEMA_UDTF).collect()
        df = self.session.sql(f"SELECT * FROM TABLE(jdbc_test('{infer_schema_sql}'))")
        return df


JDBC_UDTF = """
CREATE OR REPLACE FUNCTION jdbc(query VARCHAR)
RETURNS TABLE (A VARIANT)
LANGUAGE JAVA
RUNTIME_VERSION = '11'
EXTERNAL_ACCESS_INTEGRATIONS=(snowpark_jdbc_influxdb_test_integration)
packages=('com.snowflake:snowpark:latest')
IMPORTS = ('@test_stage/influxdb-java-2.25.jar','@test_stage/amazon-timestream-jdbc-2.0.0.jar','@test_stage/amazon-timestream-jdbc-2.0.0-shaded.jar')
HANDLER = 'DataLoader'
as
$$

import java.sql.*;
import java.util.stream.Stream;

import com.snowflake.snowpark_java.types.*;
import com.snowflake.snowpark_java.udtf.*;
import com.snowflake.snowpark_java.types.SnowflakeSecrets;
import com.snowflake.snowpark_java.types.UsernamePassword;

class OutputRow {{

    public String A;

    public OutputRow(String A) {{
        this.A = A;
    }}

}}



public class DataLoader{{
    private final Connection conn;

    private static String escapeJson(String str) {{
        return str.replace("\\", "\\\\")
                  .replace("\"", "\\\"")
                  .replace("\b", "\\b")
                  .replace("\f", "\\f")
                  .replace("\n", "\\n")
                  .replace("\r", "\\r")
                  .replace("\t", "\\t");
    }}


    public StructType outputSchema() {{
        return StructType.create(new StructField("A", DataTypes.VariantType));
    }}
    public static Class getOutputClass() {{
      return OutputRow.class;
    }}

    private static Connection createConnection() {{
        try {{
            Class.forName("com.wisecoders.dbschema.influxdb.JdbcDriver");
            //Class.forName("software.amazon.timestream.jdbc.TimestreamDriver");
            String url = "";
            return DriverManager.getConnection(url);
        }} catch (Exception e) {{
            throw new RuntimeException("Failed to create JDBC connection: " + e.getMessage());
        }}
    }}

    public DataLoader() {{
        this.conn = createConnection();
    }}

    public Stream<OutputRow> process(String query) {{
        try {{
            ResultSet result = this.conn.createStatement().executeQuery(query);
            if (result.next()) {{
                ResultSetMetaData meta = result.getMetaData();
                int columnCount = meta.getColumnCount();
                StringBuilder json = new StringBuilder();
                json.append("{{");

                for (int i = 1; i <= columnCount; i++) {{
                    String name = meta.getColumnLabel(i);
                    Object value = result.getObject(i);

                    json.append("\"").append(escapeJson(name)).append("\":");

                    if (value == null) {{
                        json.append("null");
                    }} else if (value instanceof Number || value instanceof Boolean) {{
                        json.append(value.toString());
                    }} else {{
                        json.append("\"").append(escapeJson(value.toString())).append("\"");
                    }}

                    if (i < columnCount) {{
                        json.append(",");
                    }}
                }}

                json.append("}}");
                return Stream.of(new OutputRow(json.toString()));
            }}

            return Stream.empty();
        }} catch (Exception e) {{
            throw new RuntimeException("Failed to load data: " + e.getMessage());
        }}
    }}

    public Stream<OutputRow> endPartition() {{
        return Stream.empty();
    }}
}}
$$
;
"""
