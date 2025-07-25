#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

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


INFER_SCHEMA_UDTF = """
CREATE OR REPLACE FUNCTION jdbc_test(query VARCHAR)
RETURNS TABLE (field_name VARCHAR, field_type VARCHAR, nullable BOOLEAN)
LANGUAGE JAVA
RUNTIME_VERSION = '11'
EXTERNAL_ACCESS_INTEGRATIONS=(******************)
packages=('com.snowflake:snowpark:latest')
IMPORTS = ('**********')
HANDLER = 'DataLoader'
as
$$

import java.sql.*;
import java.util.stream.Stream;
import java.util.ArrayList;
import java.util.List;
import com.snowflake.snowpark_java.types.*;
import com.snowflake.snowpark_java.udtf.*;
import com.snowflake.snowpark_java.types.SnowflakeSecrets;
import com.snowflake.snowpark_java.types.UsernamePassword;

class OutputRow {{

    public String field_name;
    public String field_type;
    public boolean nullable;

}}

public class DataLoader{{
    private final Connection conn;

    private static Connection createConnection() {{
        try {{
            Class.forName("*********");
            String url = "jdbc:oracle:thin:@*****:1521/ORCL";
            String username = "*****";
            String password = "*****";
            return DriverManager.getConnection(url, username, password);
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
