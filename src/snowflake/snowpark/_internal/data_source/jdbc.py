#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
import copy
import logging
import re
from collections import defaultdict
from enum import Enum
from functools import cached_property
from typing import Optional, Union, List, TYPE_CHECKING
from urllib.parse import unquote_plus

from snowflake.snowpark._internal.analyzer.datatype_mapper import str_to_sql
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
    IntegerType,
    DoubleType,
    TimeType,
)

if TYPE_CHECKING:
    from snowflake.snowpark.session import Session
    from snowflake.snowpark.dataframe import DataFrame

logger = logging.getLogger(__name__)

# reference for java type to snowflake type:
# https://docs.snowflake.com/en/developer-guide/udf-stored-procedure-data-type-mapping


# TODO: SNOW-2303890: support unsupported types for jdbc
class JDBCType(Enum):
    # ARRAY = "ARRAY"  # not supported in snowflake
    BIGINT = "BIGINT"
    BINARY = "BINARY"
    BIT = "BIT"
    # BLOB = "BLOB"  # not supported in snowflake
    BOOLEAN = "BOOLEAN"
    CHAR = "CHAR"
    CLOB = "CLOB"
    # DATALINK = "DATALINK"  # not supported in snowflake
    DATE = "DATE"
    DECIMAL = "DECIMAL"
    # DISTINCT = "DISTINCT"  # not supported in snowflake
    DOUBLE = "DOUBLE"
    FLOAT = "FLOAT"
    INTEGER = "INTEGER"
    # JAVA_OBJECT = "JAVA_OBJECT"  # not supported in snowflake
    LONGNVARCHAR = "LONGNVARCHAR"
    LONGVARBINARY = "LONGVARBINARY"
    LONGVARCHAR = "LONGVARCHAR"
    NCHAR = "NCHAR"
    NCLOB = "NCLOB"
    # NULL = "NULL"  # not supported in snowflake
    NUMERIC = "NUMERIC"
    NVARCHAR = "NVARCHAR"
    OTHER = "OTHER"
    # REAL = "REAL"  # not supported in snowflake
    # REF = "REF"  # not supported in snowflake
    # REF_CURSOR = "REF_CURSOR"  # not supported in snowflake
    # ROWID = "ROWID"  # not supported in snowflake
    SMALLINT = "SMALLINT"
    # SQLXML = "SQLXML" # not supported in snowflake
    # STRUCT = "STRUCT"  # not supported in snowflake
    TIME = "TIME"
    TIME_WITH_TIMEZONE = "TIME_WITH_TIMEZONE"
    TIMESTAMP = "TIMESTAMP"
    TIMESTAMP_WITH_TIMEZONE = "TIMESTAMP_WITH_TIMEZONE"
    TINYINT = "TINYINT"
    VARBINARY = "VARBINARY"
    VARCHAR = "VARCHAR"
    NOT_SUPPORTED = "NOT_SUPPORTED"
    NONE = None


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
    JDBCType.BIGINT: IntegerType,
    JDBCType.BINARY: StringType,
    JDBCType.BIT: StringType,
    JDBCType.BOOLEAN: BooleanType,
    JDBCType.CHAR: StringType,
    JDBCType.CLOB: StringType,
    JDBCType.DATE: DateType,
    JDBCType.DECIMAL: DecimalType,
    JDBCType.DOUBLE: DoubleType,
    JDBCType.FLOAT: FloatType,
    JDBCType.INTEGER: IntegerType,
    JDBCType.LONGNVARCHAR: StringType,
    JDBCType.LONGVARBINARY: StringType,
    JDBCType.LONGVARCHAR: StringType,
    JDBCType.NCHAR: StringType,
    JDBCType.NCLOB: StringType,
    JDBCType.NUMERIC: DecimalType,
    JDBCType.NVARCHAR: StringType,
    JDBCType.OTHER: StringType,
    JDBCType.SMALLINT: IntegerType,
    JDBCType.TIME: TimeType,
    JDBCType.TIME_WITH_TIMEZONE: TimestampType,
    JDBCType.TIMESTAMP: TimestampType,
    JDBCType.TIMESTAMP_WITH_TIMEZONE: TimestampType,
    JDBCType.TINYINT: IntegerType,
    JDBCType.VARBINARY: StringType,
    JDBCType.VARCHAR: StringType,
}

PARTITION_TABLE_COLUMN_NAME = "partition"

# Exact credential option names, not substrings of arbitrary option values.
_JDBC_SECRET_KEYS = {
    "user",
    "username",
    "password",
    "pwd",
    "token",
    "accesskey",
    "secret",
    "apikey",
    "sslpassword",
    "password1",
    "password2",
    "password3",
    "certkeystorepass",
    "clientcertificatekeystorepassword",
    "trustcertificatekeystorepassword",
    "accesstoken",
    "truststorepassword",
    "keystoresecret",
    "clientkeypassword",
    "keyvaultproviderclientkey",
    "aadsecureprincipalsecret",
    "oracle.jdbc.password",
    "oracle.jdbc.user",
    "oracle.jdbc.accesstoken",
    "newpassword",
    "oracle.jdbc.newpassword",
    "clientsecret",
    "oracle.jdbc.clientsecret",
    "clientcertificatepassword",
    "oracle.jdbc.clientcertificatepassword",
    "oracle.net.wallet_password",
    "websocketpassword",
    "oracle.net.websocketpassword",
    "javax.net.ssl.keystorepassword",
    "javax.net.ssl.truststorepassword",
    "oracle.net.ldap.ssl.keystorepassword",
    "oracle.net.ldap.ssl.truststorepassword",
    "oracle.net.ldap.ssl.walletpassword",
    "oracle.net.ldap.security.credentials",
    "oracle.jdbc.ons.walletpassword",
}
_JDBC_URL_ERROR = "Unsupported or malformed JDBC URL; use a supported connection URL and the secret for credentials"


def _java_string_literal(value: str) -> str:
    # Octal escapes avoid both Java's pre-lexing Unicode escapes and the SQL $$
    # delimiter. In particular, a literal backslash-u must stay literal at runtime.
    escapes = {
        "\\": r"\\",
        '"': r"\"",
        "\n": r"\n",
        "\r": r"\r",
        "\t": r"\t",
        "$": r"\044",
    }
    return (
        '"'
        + "".join(
            escapes.get(char, f"\\{ord(char):03o}" if ord(char) < 32 else char)
            for char in value
        )
        + '"'
    )


def _validate_jdbc_percent_encoding(value: str) -> None:
    if re.search(r"%(?![0-9a-fA-F]{2})", value):
        raise ValueError(_JDBC_URL_ERROR)


def _jdbc_option_name(key: str) -> str:
    _validate_jdbc_percent_encoding(key)
    # MySQL decodes names. Apply that conservative interpretation to properties
    # and other drivers too, without changing the retained spelling or values.
    name = unquote_plus(key).strip().lower()
    if not re.fullmatch(r"[a-z][a-z0-9_.$-]*", name):
        raise ValueError("Unsupported JDBC option name; use the secret for credentials")
    return name


def _strip_jdbc_query_credentials(url: str) -> str:
    base, separator, query = url.partition("?")
    if not separator:
        return url
    retained = []
    for option in query.split("&"):
        if not option:
            continue
        key, equals, _ = option.partition("=")
        if not equals:
            # PostgreSQL alone accepts the exact bare ssl flag. Other bare
            # names could be malformed credential tails and must still reject.
            if url.startswith("jdbc:postgresql://") and option == "ssl":
                retained.append(option)
                continue
            raise ValueError(_JDBC_URL_ERROR)
        if _jdbc_option_name(key) not in _JDBC_SECRET_KEYS:
            retained.append(option)
    return base + ("?" + "&".join(retained) if retained else "")


def _strip_network_jdbc_credentials(url: str, prefix: str) -> str:
    base, separator, query = url.partition("?")
    authority, slash, path = base[len(prefix) :].partition("/")
    # Host-specific MySQL properties and userinfo on individual hosts need a
    # different grammar. Reject rather than dropping hosts or partial passwords.
    if any(char in authority for char in "()=;\\\"'") or any(
        char in path for char in "@;="
    ):
        raise ValueError(_JDBC_URL_ERROR)
    if "@" in authority:
        if authority.count("@") != 1 or "," in authority:
            raise ValueError(_JDBC_URL_ERROR)
        userinfo, authority = authority.split("@")
        if not userinfo or any(char in userinfo for char in "[]"):
            raise ValueError(_JDBC_URL_ERROR)
    # Simple comma-separated hosts and bracketed IPv6 are supported. Encoded
    # authority syntax is deliberately not decoded into a different host grammar.
    host = r"(?:\[[0-9a-fA-F:.%]+\]|[a-zA-Z0-9_.-]+)(?::[0-9]+)?"
    if not re.fullmatch(host + r"(?:," + host + r")*", authority):
        raise ValueError(_JDBC_URL_ERROR)
    clean = prefix + authority + slash + path
    return _strip_jdbc_query_credentials(
        clean + (separator + query if separator else "")
    )


def _strip_sqlserver_credentials(url: str) -> str:
    # SQL Server braces quote semicolons; doubled closing braces are literal.
    prefix, _, properties = url.partition(";")
    if not re.fullmatch(r"jdbc:sqlserver://[a-zA-Z0-9_.:\[\]\\-]*", prefix):
        raise ValueError(_JDBC_URL_ERROR)
    retained = []
    position = 0
    while position < len(properties):
        if properties[position] == ";":
            position += 1
            continue
        start = position
        match = re.match(r"([^;=]+)=\s*", properties[position:])
        if match is None:
            raise ValueError(_JDBC_URL_ERROR)
        key = _jdbc_option_name(match.group(1))
        position += match.end()
        if position < len(properties) and properties[position] == "{":
            position += 1
            while position < len(properties):
                if properties[position] == "}":
                    if properties[position : position + 2] == "}}":
                        position += 2
                        continue
                    position += 1
                    break
                position += 1
            else:
                raise ValueError(_JDBC_URL_ERROR)
            while position < len(properties) and properties[position].isspace():
                position += 1
            if position < len(properties) and properties[position] != ";":
                raise ValueError(_JDBC_URL_ERROR)
        else:
            end = properties.find(";", position)
            position = len(properties) if end < 0 else end
            if any(char in properties[match.end() + start : position] for char in "{}"):
                raise ValueError(_JDBC_URL_ERROR)
        if key not in _JDBC_SECRET_KEYS:
            retained.append(properties[start:position])
        position += 1
    return ";".join([prefix, *retained])


def _validate_oracle_descriptor(descriptor: str) -> None:
    # Validate nested (KEY=value) / (KEY=(...)(...)) nodes without interpreting
    # arbitrary descriptor text as a URL. Credential nodes are not supported.
    position = 0
    depth = 0
    while position < len(descriptor):
        if descriptor[position].isspace():
            position += 1
        elif descriptor[position] == "(":
            match = re.match(
                r"\(\s*([a-zA-Z][a-zA-Z0-9_.]*)\s*=\s*", descriptor[position:]
            )
            if match is None or _jdbc_option_name(match.group(1)) in _JDBC_SECRET_KEYS:
                raise ValueError(_JDBC_URL_ERROR)
            depth += 1
            position += match.end()
            if position < len(descriptor) and descriptor[position] == '"':
                # A quoted leaf (for example SSL_SERVER_CERT_DN) may contain
                # parentheses and equals signs, but is never another option.
                position += 1
                while position < len(descriptor):
                    if descriptor[position] == '"':
                        position += 1
                        break
                    if descriptor[position] == "\\":
                        raise ValueError(_JDBC_URL_ERROR)
                    position += 1
                else:
                    raise ValueError(_JDBC_URL_ERROR)
                while position < len(descriptor) and descriptor[position].isspace():
                    position += 1
                if position >= len(descriptor) or descriptor[position] != ")":
                    raise ValueError(_JDBC_URL_ERROR)
            elif position < len(descriptor) and descriptor[position] != "(":
                end = descriptor.find(")", position)
                if end < 0 or any(
                    char in descriptor[position:end] for char in "(=\"'@?\\"
                ):
                    raise ValueError(_JDBC_URL_ERROR)
                position = end
        elif descriptor[position] == ")" and depth:
            depth -= 1
            position += 1
            if depth == 0 and descriptor[position:].strip():
                raise ValueError(_JDBC_URL_ERROR)
        else:
            raise ValueError(_JDBC_URL_ERROR)
    if depth:
        raise ValueError(_JDBC_URL_ERROR)


def _strip_oracle_credentials(url: str) -> str:
    prefix = "jdbc:oracle:thin:"
    # A descriptor is a separate grammar, not an Easy Connect query string.
    if url.startswith(prefix + "@("):
        _validate_oracle_descriptor(url[len(prefix) + 1 :])
        return url
    # Quoted authentication and Easy Connect query values can contain delimiters.
    # Reject them whole until their full grammar is supported, never split them.
    if any(char in url for char in "\"'\\"):
        raise ValueError(_JDBC_URL_ERROR)
    base, separator, query = url[len(prefix) :].partition("?")
    if base.count("@") != 1:
        raise ValueError(_JDBC_URL_ERROR)
    credentials, target = base.split("@")
    if credentials and not re.fullmatch(r"[^\s/@:]+/[^\s/@]+", credentials):
        raise ValueError(_JDBC_URL_ERROR)
    if target.startswith("("):
        _validate_oracle_descriptor(target)
    elif not re.fullmatch(r"(?://)?[a-zA-Z0-9_.:\[\]-]+(?:/[a-zA-Z0-9_.:-]+)?", target):
        raise ValueError(_JDBC_URL_ERROR)
    return _strip_jdbc_query_credentials(prefix + "@" + target + separator + query)


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
        java_version: Optional[int] = 17,
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
        self.properties = copy.deepcopy(properties)

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
        if (
            self.packages is not None
            and "com.snowflake:snowpark:latest" not in self.packages
            and "com.snowflake:snowpark" not in self.packages
        ):
            self.packages.append("com.snowflake:snowpark:latest")
        self.packages_sql = (
            f"""PACKAGES=({','.join([f"'{pack}'" for pack in self.packages])})"""
            if self.packages is not None
            else "PACKAGES=('com.snowflake:snowpark:latest')"
        )
        self.secret_sql = f"SECRETS = ('cred' = {self.secret})"
        self._infer_schema_successful = True
        self.secret_detector()

    @cached_property
    def schema(self) -> StructType:
        infer_schema_udtf_name = random_name_for_temp_object(TempObjectType.FUNCTION)
        infer_schema_udtf_registration = f"""
            CREATE OR REPLACE TEMPORARY FUNCTION {infer_schema_udtf_name}(query VARCHAR)
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
            import java.util.Objects;
            import java.util.concurrent.atomic.AtomicInteger;
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
                        // Driver messages and causes can contain credentials.
                        throw new RuntimeException("Failed to create JDBC connection");
                    }}
                }}
                public static Class<?> getOutputClass() {{
                    return OutputRow.class;
                }}

                public DataLoader() {{
                    this.conn = createConnection();
                }}

                public Stream<OutputRow> process(String query) {{
                    try {{
                        Statement stmt = this.conn.createStatement();
                        ResultSet rs = stmt.executeQuery(query);
                        ResultSetMetaData meta = rs.getMetaData();
                        int columnCount = meta.getColumnCount();
                        final AtomicInteger counter = new AtomicInteger(1);
                        Stream<OutputRow> resultStream = Stream.generate(() -> {{
                            try {{
                                int currentColumnIndex = counter.getAndIncrement();
                                if (currentColumnIndex <= columnCount) {{
                                    OutputRow row = new OutputRow();
                                    row.field_name = meta.getColumnName(currentColumnIndex);
                                    row.java_type = meta.getColumnClassName(currentColumnIndex);
                                    try{{
                                        row.jdbc_type = JDBCType.valueOf(meta.getColumnType(currentColumnIndex)).toString();
                                    }} catch(Exception e){{
                                        row.jdbc_type = null;
                                    }}
                                    row.precision = meta.getPrecision(currentColumnIndex);
                                    row.scale = meta.getScale(currentColumnIndex);
                                    row.nullable = meta.isNullable(currentColumnIndex) == ResultSetMetaData.columnNullable;
                                    return row;
                                }} else {{
                                    rs.close();
                                    stmt.close();
                                    this.conn.close();
                                    return null;
                                }}
                            }} catch (Exception e) {{
                                throw new RuntimeException("Ingestion error: JDBC schema metadata failed");
                            }}
                        }}).takeWhile(Objects::nonNull);
                        return resultStream;
                    }} catch (Exception e) {{
                        throw new RuntimeException("Ingestion error: JDBC schema inference failed");
                    }}

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
        # The inference query is passed as a SQL string literal, so it must be escaped:
        # otherwise quotes, backslashes or newlines in a user `query` break or alter it.
        self.raw_schema = self.session.sql(
            f"SELECT * FROM TABLE({infer_schema_udtf_name}({str_to_sql(self.infer_schema_sql())}))",
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
            CREATE OR REPLACE TEMPORARY FUNCTION {jdbc_ingestion_name}(query VARCHAR)
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
            import java.util.Objects;
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
            // Snowflake may call process() several times on one handler instance (one call
            // per partition row it routes here), so the connection is shared across calls,
            // opened on first use. Each call owns its Statement and ResultSet. A failed
            // or explicitly closed stream releases the connection only when no other
            // stream is using it; endPartition() also cleans up abandoned streams.
            private Connection conn;
            private final List<AutoCloseable> openResources = new ArrayList<>();

            private static Connection createConnection() {{
                try {{
                    {self.generate_create_connection()}
                }} catch (Exception e) {{
                    // Driver messages and causes can contain credentials.
                    throw new RuntimeException("Failed to create JDBC connection");
                }}
            }}

            private Connection getConnection() throws SQLException {{
                if (this.conn == null || this.conn.isClosed()) {{
                    this.conn = createConnection();
                }}
                return this.conn;
            }}

            private void closeQuietly(AutoCloseable resource) {{
                if (resource == null) {{
                    return;
                }}
                try {{
                    openResources.remove(resource);
                    resource.close();
                }} catch (Exception ignored) {{
                }}
            }}

            private void closeStream(Statement stmt, ResultSet rs) {{
                closeQuietly(rs);
                closeQuietly(stmt);
                // Do not invalidate another process() stream on this handler.
                if (openResources.isEmpty()) {{
                    closeQuietly(this.conn);
                    this.conn = null;
                }}
            }}

            public static Class<?> getOutputClass() {{
                return OutputRow.class;
            }}

            public DataLoader() {{
            }}

            public Stream<OutputRow> process(String query) {{
                // Execute lazily, on the first pull from the returned Stream.
                // JAVA_UDF_HANDLER_TIMEOUT_SECONDS (600s by default) only bounds the time
                // until process() returns, so calling executeQuery() here would charge the
                // source's entire query time against it. Row fetching stays lazy.
                final Statement[] stmtRef = new Statement[1];
                final ResultSet[] rsRef = new ResultSet[1];
                return Stream.generate(() -> {{
                    try {{
                        if (rsRef[0] == null) {{
                            Statement stmt = getConnection().createStatement();
                            openResources.add(stmt);
                            stmtRef[0] = stmt;
                            stmt.setQueryTimeout({str(self.query_timeout)});
                            stmt.setFetchSize({str(self.fetch_size)});
                            {self.generate_session_init_statement()}
                            rsRef[0] = stmt.executeQuery(query);
                            openResources.add(rsRef[0]);
                        }}
                        ResultSet rs = rsRef[0];
                        if (rs.next()) {{
                            OutputRow row = new OutputRow();
                            {self.create_output_row_java_code()}
                            return row;
                        }} else {{
                            closeQuietly(rs);
                            closeQuietly(stmtRef[0]);
                            return null;
                        }}
                    }} catch (Exception e) {{
                        closeStream(stmtRef[0], rsRef[0]);
                        // Exception rather than SQLException: this block now also covers
                        // connection and statement setup, whose failures are not always
                        // SQLException.
                        throw new RuntimeException("Ingestion error: JDBC read failed");
                    }}
                }}).takeWhile(Objects::nonNull)
                  .onClose(() -> closeStream(stmtRef[0], rsRef[0]));
            }}

            public Stream<OutputRow> endPartition() {{
                for (AutoCloseable resource : new ArrayList<>(openResources)) {{
                    closeQuietly(resource);
                }}
                closeQuietly(this.conn);
                this.conn = null;
                return Stream.empty();
            }}
        }}
        $$
        ;
        """

        # Cast to VARCHAR to drop any collation the partition column inherits,
        # which would otherwise fail to match the UDTF signature.
        jdbc_udtf = f"""
            select result.* from {partition_table}, table({jdbc_ingestion_name}({PARTITION_TABLE_COLUMN_NAME}::VARCHAR)) AS result
            """

        self.session.sql(jdbc_udtf_registration, _emit_ast=self._emit_ast).collect()
        return BaseDriver.to_result_snowpark_df_udtf(
            self.session.sql(jdbc_udtf, _emit_ast=self._emit_ast),
            self.schema,
            _emit_ast=self._emit_ast,
        )

    def generate_create_connection(self):
        user_properties_overwrite = ""
        if self.properties is not None:
            user_properties_overwrite = "\n".join(
                [
                    f"properties.put({_java_string_literal(key)}, {_java_string_literal(str(value))});"
                    for key, value in self.properties.items()
                ]
            )
        get_secret = """
                    SnowflakeSecrets secrets = SnowflakeSecrets.newInstance();
                    UsernamePassword up = secrets.getUsernamePassword("cred");
                    properties.put("user", up.getUsername());
                    properties.put("password", up.getPassword());
        """
        return f"""
                String url = {_java_string_literal(self.url)};
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
            try:
                jdbc_type = JDBCType(jdbc_type)
            except Exception:
                jdbc_type = JDBCType.NOT_SUPPORTED

            jdbc_to_snow_type = JDBC_TYPE_TO_SNOWFLAKE_TYPE.get(jdbc_type, None)
            java_to_snow_type = JAVA_TYPE_TO_SNOWFLAKE_TYPE.get(java_type, VariantType)
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
            elif snow_type == TimestampType and jdbc_type in (
                JDBCType.TIMESTAMP,
                None,
                JDBCType.OTHER,
            ):
                data_type = snow_type(TimestampTimeZone.NTZ)
            elif snow_type == TimestampType and jdbc_type in (
                JDBCType.TIMESTAMP_WITH_TIMEZONE,
                JDBCType.TIME_WITH_TIMEZONE,
            ):
                data_type = snow_type(TimestampTimeZone.TZ)
            else:
                data_type = snow_type()

            fields.append(StructField(field_name, data_type, nullable))
        return StructType(fields)

    def generate_select_sql(self):
        select_sql_alias = (
            f"SNOWPARK_JDBC_SELECT_SQL_ALIAS_{generate_random_alphanumeric(5)}"
        )
        cols = [col[0] for col in self.raw_schema]
        if self.is_query:
            return f"SELECT {select_sql_alias}.* FROM ({self.table_or_query}) {select_sql_alias}"
        else:
            return f"SELECT {', '.join(cols)} FROM {self.table_or_query}"

    def secret_detector(self):
        """Sanitize URL/properties before either UDTF is registered.

        Supported URL grammars are simple PostgreSQL/MySQL network URLs,
        SQL Server semicolon properties (including braced values), and Oracle
        thin URLs with unquoted authentication or credential-free descriptors.
        Ambiguous/unsupported forms are rejected.
        This does not detect or rewrite credentials in arbitrary query or
        session-initialization SQL; callers must keep secrets out of that SQL.
        """
        if not isinstance(self.url, str):
            raise ValueError("JDBC URL must be a string")
        if self.properties is not None:
            if not isinstance(self.properties, dict) or any(
                not isinstance(key, str) for key in self.properties
            ):
                raise ValueError(
                    "JDBC properties must be a dictionary with string keys"
                )
            self.properties = {
                key: value
                for key, value in self.properties.items()
                if _jdbc_option_name(key) not in _JDBC_SECRET_KEYS
            }
        if self.url.startswith(("jdbc:postgresql://", "jdbc:mysql://")):
            # Validate even values that will be removed. SQL Server and Oracle
            # values use literal percent signs rather than percent encoding.
            _validate_jdbc_percent_encoding(self.url)
            prefix = self.url[: self.url.index("://") + 3]
            self.url = _strip_network_jdbc_credentials(self.url, prefix)
        elif self.url.startswith("jdbc:sqlserver://"):
            self.url = _strip_sqlserver_credentials(self.url)
        elif self.url.startswith("jdbc:oracle:thin:"):
            self.url = _strip_oracle_credentials(self.url)
        else:
            raise ValueError(_JDBC_URL_ERROR)

    def generate_session_init_statement(self):
        if self.session_init_statement is not None:
            return "\n".join(
                [
                    f"stmt.execute({_java_string_literal(query)});"
                    for query in self.session_init_statement
                ]
            )
        else:
            return ""

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
