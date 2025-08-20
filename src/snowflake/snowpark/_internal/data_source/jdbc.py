#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
import logging
import re
from collections import defaultdict
from functools import cached_property
from typing import Optional, Union, List, TYPE_CHECKING

from snowflake.snowpark._internal.data_source import DataSourcePartitioner
from snowflake.snowpark._internal.data_source.drivers import BaseDriver
from snowflake.snowpark._internal.utils import (
    generate_random_alphanumeric,
    random_name_for_temp_object,
    TempObjectType,
)
from snowflake.snowpark.types import (
    StructType,
    DecimalType,
    FloatType,
    StringType,
    DateType,
    BooleanType,
    ArrayType,
    StructField,
    VariantType,
    TimestampType,
    TimestampTimeZone,
)

if TYPE_CHECKING:
    from snowflake.snowpark.session import Session
    from snowflake.snowpark.dataframe import DataFrame

logger = logging.getLogger(__name__)

# reference for java type to snowflake type:
# https://docs.snowflake.com/en/developer-guide/udf-stored-procedure-data-type-mapping

JAVA_TYPE_TO_SNOWFLAKE_TYPE = {
    "java.math.BigDecimal": DecimalType,
    "java.math.BigInteger": DecimalType,
    "java.lang.Float": FloatType,
    "java.lang.Double": FloatType,
    "java.lang.Boolean": BooleanType,
    "java.lang.String": StringType,
    "java.sql.Timestamp": TimestampType,
    "java.sql.Date": DateType,
    "java.sql.Array": ArrayType,
    "java.lang.String[]": ArrayType,
    "java.lang.Long": DecimalType,
    "java.lang.Integer": DecimalType,
    "java.lang.Short": DecimalType,
}
JDBC_TYPE_TO_SNOWFLAKE_TYPE = {
    "NUMERIC": DecimalType,
    "VARCHAR": StringType,
    "CHAR": StringType,
    "CLOB": StringType,
    "NCHAR": StringType,
    "NVARCHAR": StringType,
    "NCLOB": StringType,
    "TIMESTAMP": TimestampType,
    "VARBINARY": StringType,
}

PARTITION_TABLE_COLUMN_NAME = "partition"


class JDBC:
    def __init__(
        self,
        session: "Session",
        url: str,
        table_or_query: str,
        external_access_integration: str,
        imports: List[str],
        is_query: bool,
        secret: str,
        *,
        properties: Optional[dict] = None,
        packages: Optional[List[str]] = None,
        java_version: Optional[int] = 11,
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
        self.java_version = java_version
        self.secret = secret
        self.properties = properties

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
        self.imports_sql = (
            f"""IMPORTS =({",".join([f"'{imp}'" for imp in self.imports])})"""
            if self.imports is not None
            else ""
        )
        self.packages_sql = (
            f"""PACKAGES=({','.join([f"'{pack}'" for pack in self.packages])})"""
            if self.packages is not None
            else "PACKAGES=('com.snowflake:snowpark:latest')"
        )
        self.secret_sql = f"SECRETS = ('cred' = {self.secret})"
        self._infer_schema_successful = True

    @cached_property
    def schema(self) -> StructType:
        infer_schema_udtf_name = random_name_for_temp_object(TempObjectType.FUNCTION)
        infer_schema_udtf_registration = f"""
            CREATE OR REPLACE FUNCTION {infer_schema_udtf_name}(query VARCHAR)
            RETURNS TABLE (field_name VARCHAR, jdbc_type VARCHAR, java_type VARCHAR, precision INTEGER, scale INTEGER, nullable BOOLEAN)
            LANGUAGE JAVA
            RUNTIME_VERSION = '{self.java_version}'
            EXTERNAL_ACCESS_INTEGRATIONS=({self.external_access_integration})
            {self.imports_sql}
            {self.packages_sql}
            {self.secret_sql}
            HANDLER = 'DataLoader'
            as
            $$

            import java.sql.*;
            import java.util.stream.Stream;
            import java.util.ArrayList;
            import java.util.List;
            import com.snowflake.snowpark_java.types.*;
            import com.snowflake.snowpark_java.types.SnowflakeSecrets;
            import com.snowflake.snowpark_java.types.UsernamePassword;


            class OutputRow {{

                public String field_name;
                public String jdbc_type;
                public String java_type;
                public int precision;
                public int scale;
                public boolean nullable;

            }}

            public class DataLoader{{
                private final Connection conn;

                private static Connection createConnection() {{
                    try {{
                        {self.generate_create_connection()}
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
                            ResultSetMetaData meta = rs.getMetaData();
                            int columnCount = meta.getColumnCount();

                            for (int i = 1; i <= columnCount; i++) {{
                                OutputRow row = new OutputRow();

                                row.field_name = meta.getColumnName(i);
                                row.java_type = meta.getColumnClassName(i);
                                try{{
                                    row.jdbc_type = JDBCType.valueOf(meta.getColumnType(i)).toString();
                                }} catch(Exception e){{
                                    row.jdbc_type = null;
                                }}
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

        self.session.sql(
            infer_schema_udtf_registration, _emit_ast=self._emit_ast
        ).collect()
        self.raw_schema = self.session.sql(
            f"SELECT * FROM TABLE({infer_schema_udtf_name}('{self.infer_schema_sql()}'))",
            _emit_ast=self._emit_ast,
        ).collect()

        auto_infer_schema = self.to_snow_type()

        if self.custom_schema is None:
            return auto_infer_schema
        else:
            custom_schema = DataSourcePartitioner.formatting_custom_schema(
                self.custom_schema
            )

            # generate final schema with auto infer schema and custom schema
            custom_schema_name_to_field = defaultdict()
            for field in custom_schema.fields:
                if field.name.lower() in custom_schema_name_to_field:
                    raise ValueError(
                        f"Invalid schema: {self.custom_schema}. "
                        f"Schema contains duplicate column: {field.name.lower()}. "
                        "Please choose another name or rename the existing column "
                    )
                custom_schema_name_to_field[field.name.lower()] = field
            final_fields = []
            for field in auto_infer_schema.fields:
                final_fields.append(
                    custom_schema_name_to_field.get(field.name.lower(), field)
                )

            return StructType(final_fields)

    @cached_property
    def partitions(self) -> List[str]:
        if self.raw_schema is None:
            self.schema
        select_query = self.generate_select_sql()
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
        jdbc_ingestion_name = random_name_for_temp_object(TempObjectType.FUNCTION)
        udtf_table_return_type = ", ".join(
            [f"{field.name} VARCHAR" for field in self.schema.fields]
        )

        jdbc_udtf_registration = f"""
            CREATE OR REPLACE FUNCTION {jdbc_ingestion_name}(query VARCHAR)
            RETURNS TABLE ({udtf_table_return_type})
            LANGUAGE JAVA
            RUNTIME_VERSION = '{self.java_version}'
            EXTERNAL_ACCESS_INTEGRATIONS=({self.external_access_integration})
            {self.imports_sql}
            {self.packages_sql}
            {self.secret_sql}
            HANDLER = 'DataLoader'
            as
            $$

            import java.sql.*;
            import java.util.stream.Stream;
            import java.util.ArrayList;
            import java.util.List;
            import java.util.Map;
            import java.util.LinkedHashMap;
            import com.snowflake.snowpark_java.types.*;
            import com.snowflake.snowpark_java.types.SnowflakeSecrets;
            import com.snowflake.snowpark_java.types.UsernamePassword;


            class OutputRow {{
                {self.create_output_row_class()}
            }}

            public class DataLoader{{
            private final Connection conn;

            private static Connection createConnection() {{
                try {{
                    {self.generate_create_connection()}
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
                    stmt.setQueryTimeout({str(self.query_timeout)});
                    stmt.setFetchSize({str(self.fetch_size)});
                    try (ResultSet rs = stmt.executeQuery()) {{
                        ResultSetMetaData meta = rs.getMetaData();
                        int columnCount = meta.getColumnCount();
                        while (rs.next()) {{
                            OutputRow row = new OutputRow();
                            {self.create_output_row_java_code()}
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
            select result.* from {partition_table}, table({jdbc_ingestion_name}({PARTITION_TABLE_COLUMN_NAME})) AS result
            """

        self.session.sql(jdbc_udtf_registration, _emit_ast=self._emit_ast).collect()
        return BaseDriver.to_result_snowpark_df_udtf(
            self.session.sql(jdbc_udtf, _emit_ast=self._emit_ast),
            self.schema,
            _emit_ast=self._emit_ast,
        )

    def generate_create_connection(self):
        user_properties_overwrite = (
            "\n".join(
                [
                    f'properties.put("{key}", "{value}");'
                    for key, value in self.properties.items()
                ]
            )
            if self.properties is not None
            else ""
        )
        get_secret = """
                    SnowflakeSecrets secrets = SnowflakeSecrets.newInstance();
                    UsernamePassword up = secrets.getUsernamePassword("cred");
                    properties.put("user", up.getUsername());
                    properties.put("password", up.getPassword());
        """
        return f"""
                String url = "{self.url}";
                java.util.Properties properties = new java.util.Properties();
                {get_secret}
                {user_properties_overwrite}
                return DriverManager.getConnection(url, properties);
            """

    def infer_schema_sql(self):
        infer_schema_alias = (
            f"SNOWPARK_JDBC_INFER_SCHEMA_ALIAS_{generate_random_alphanumeric(5)}"
        )
        return (
            f"SELECT {infer_schema_alias}.* FROM ({self.table_or_query}) {infer_schema_alias} WHERE 1 = 0"
            if self.is_query
            else f"SELECT * FROM {self.table_or_query} WHERE 1 = 0"
        )

    def create_output_row_java_code(self):
        return "".join(
            [
                f"row.{field.name} = rs.getString({i+1});\n"
                for i, field in enumerate(self.schema.fields)
            ]
        )

    def create_output_row_class(self):
        return "".join(
            [f"public String {field.name};\n" for field in self.schema.fields]
        )

    def to_snow_type(self) -> StructType:
        fields = []
        for (
            field_name,
            jdbc_type,
            java_type,
            precision,
            scale,
            nullable,
        ) in self.raw_schema:
            jdbc_to_snow_type = JDBC_TYPE_TO_SNOWFLAKE_TYPE.get(jdbc_type, None)
            java_to_snow_type = JAVA_TYPE_TO_SNOWFLAKE_TYPE.get(jdbc_type, VariantType)
            snow_type = (
                java_to_snow_type if jdbc_to_snow_type is None else jdbc_to_snow_type
            )
            if snow_type == DecimalType:
                if not BaseDriver.validate_numeric_precision_scale(precision, scale):
                    logger.debug(
                        f"Snowpark does not support column"
                        f" {field_name} of type {java_type} with precision {precision} and scale {scale}. "
                        "The default Numeric precision and scale will be used."
                    )
                    precision, scale = None, None
                data_type = snow_type(
                    precision if precision is not None else 38,
                    scale if scale is not None else 0,
                )
            elif snow_type == TimestampType:
                data_type = snow_type(TimestampTimeZone.TZ)
            else:
                data_type = snow_type()

            fields.append(StructField(field_name, data_type, nullable))
        return StructType(fields)

    def generate_select_sql(self):
        select_sql_alias = (
            f"SNOWPARK_JDBC_SELECT_SQL_ALIAS_{generate_random_alphanumeric(5)}"
        )
        cols = (
            [col[0] for col in self.raw_schema]
            if self.raw_schema is not None
            else ["*"]
        )
        if self.is_query:
            return f"SELECT {select_sql_alias}.* FROM ({self.table_or_query}) {select_sql_alias}"
        else:
            return f"SELECT {', '.join(cols)} FROM {self.table_or_query}"

    def secret_detector(self):
        secret_keys = {
            "password",
            "pwd",
            "token",
            "accesskey",
            "secret",
            "apikey",
            "user",
            "username",
        }
        if self.properties is not None:
            for key in list(self.properties.keys()):
                if key.lower() in secret_keys:
                    del self.properties[key]

        self.url = re.sub(
            r"(?<=://)([^:/]+)(:[^@]+)?@", "", self.url  # Matches user[:password]@
        )

    @staticmethod
    def to_result_snowpark_df(
        res_df: "DataFrame",
        schema: StructType,
        _emit_ast: bool = True,
    ) -> "DataFrame":
        cols = [
            res_df[field.name].cast(field.datatype).alias(field.name)
            for field in schema.fields
        ]
        return res_df.select(cols, _emit_ast=_emit_ast)
