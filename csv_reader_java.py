#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

"""
Java CSV UDTF SQL definitions for the large CSV ingestion POC.

This module builds SQL strings that include:
1) CREATE FUNCTION DDL
2) inline Java handler source code

The inline handler uses `nl.basjes.hadoop:splittablegzip` classes.
"""

JAVA_CSV_UDTF_NAME = "CSV_READER_JAVA_UDTF_POC"
# Inline Java handler class name for Snowflake table function.
JAVA_CSV_UDTF_HANDLER = "CsvReaderJavaUdtf"

# Staged jar path for splittablegzip dependency used by the inline Java handler.
JAVA_CSV_UDTF_JAR_STAGE_PATH = "@TEMP_CSV_READER_POC_STAGE/splittablegzip-1.3.jar"

JAVA_CSV_UDTF_HANDLER_SOURCE = r"""
import com.snowflake.snowpark_java.types.SnowflakeFile;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;

public class CsvReaderJavaUdtf {
  public static class OutputRow {
    public String id;
    public String event_ts;
    public String category;
    public String score;
    public String notes;
    public String country;
    public String active;
    public String payload;

    public OutputRow(
      String id,
      String event_ts,
      String category,
      String score,
      String notes,
      String country,
      String active,
      String payload
    ) {
      this.id = id;
      this.event_ts = event_ts;
      this.category = category;
      this.score = score;
      this.notes = notes;
      this.country = country;
      this.active = active;
      this.payload = payload;
    }
  }

  private static String[] splitCsvLine(String line) {
    List<String> out = new ArrayList<>();
    StringBuilder cur = new StringBuilder();
    boolean inQuotes = false;
    for (int i = 0; i < line.length(); i++) {
      char c = line.charAt(i);
      if (c == '"') {
        if (inQuotes && i + 1 < line.length() && line.charAt(i + 1) == '"') {
          cur.append('"');
          i++;
        } else {
          inQuotes = !inQuotes;
        }
      } else if (c == ',' && !inQuotes) {
        out.add(cur.toString());
        cur.setLength(0);
      } else {
        cur.append(c);
      }
    }
    out.add(cur.toString());
    return out.toArray(new String[0]);
  }

  private static InputStream openGzipInputStream(InputStream rawInput) throws Exception {
    // Try splittablegzip via reflection so we avoid compile-time Hadoop deps.
    // If Hadoop classes are unavailable in IMPORTS, gracefully fallback to JDK gzip.
    try {
      Class<?> codecClass = Class.forName("nl.basjes.hadoop.io.compress.SplittableGzipCodec");
      Object codec = codecClass.getDeclaredConstructor().newInstance();

      Class<?> decompressorClass = Class.forName("org.apache.hadoop.io.compress.Decompressor");
      Class<?> readModeClass =
          Class.forName("org.apache.hadoop.io.compress.SplittableCompressionCodec$READ_MODE");

      @SuppressWarnings("unchecked")
      Object byBlock =
          Enum.valueOf((Class<? extends Enum>) readModeClass.asSubclass(Enum.class), "BYBLOCK");

      Method createInputStream = codecClass.getMethod(
          "createInputStream",
          InputStream.class,
          decompressorClass,
          long.class,
          long.class,
          readModeClass);

      Object splitInput = createInputStream.invoke(codec, rawInput, null, 0L, Long.MAX_VALUE, byBlock);
      return (InputStream) splitInput;
    } catch (Throwable ignored) {
      return new GZIPInputStream(rawInput);
    }
  }

  public static Stream<OutputRow> process(String filename, Integer num_workers, Integer worker_id)
      throws Exception {
    if (num_workers == null || num_workers <= 0) {
      throw new IllegalArgumentException("num_workers must be positive");
    }
    if (worker_id == null || worker_id < 0 || worker_id >= num_workers) {
      throw new IllegalArgumentException("worker_id is out of range");
    }

    InputStream rawInput = SnowflakeFile.newInstance(filename, false).getInputStream();
    BufferedReader reader;

    if (filename != null && filename.toLowerCase().endsWith(".gz")) {
      InputStream gzipInput = openGzipInputStream(rawInput);
      reader = new BufferedReader(new InputStreamReader(gzipInput, StandardCharsets.UTF_8));
    } else {
      reader = new BufferedReader(new InputStreamReader(rawInput, StandardCharsets.UTF_8));
    }

    List<OutputRow> rows = new ArrayList<>();
    String line;
    boolean headerSkipped = false;
    long dataRowIdx = 0L;
    while ((line = reader.readLine()) != null) {
      if (!headerSkipped) {
        headerSkipped = true;
        continue;
      }
      if (line.isEmpty()) {
        continue;
      }
      if (dataRowIdx % num_workers == worker_id) {
        String[] fields = splitCsvLine(line);
        String id = fields.length > 0 ? fields[0] : null;
        String event_ts = fields.length > 1 ? fields[1] : null;
        String category = fields.length > 2 ? fields[2] : null;
        String score = fields.length > 3 ? fields[3] : null;
        String notes = fields.length > 4 ? fields[4] : null;
        String country = fields.length > 5 ? fields[5] : null;
        String active = fields.length > 6 ? fields[6] : null;
        String payload = fields.length > 7 ? fields[7] : null;
        rows.add(new OutputRow(id, event_ts, category, score, notes, country, active, payload));
      }
      dataRowIdx++;
    }
    reader.close();
    return rows.stream();
  }
}
"""


def create_java_csv_udtf_sql(
    function_name: str = JAVA_CSV_UDTF_NAME,
    handler_name: str = JAVA_CSV_UDTF_HANDLER,
    jar_stage_path: str = JAVA_CSV_UDTF_JAR_STAGE_PATH,
) -> str:
    return f"""
CREATE OR REPLACE TEMP FUNCTION {function_name}(
  filename STRING,
  num_workers INTEGER,
  worker_id INTEGER
)
RETURNS TABLE(
  id STRING,
  event_ts STRING,
  category STRING,
  score STRING,
  notes STRING,
  country STRING,
  active STRING,
  payload STRING
)
LANGUAGE JAVA
RUNTIME_VERSION = '11'
IMPORTS = ('{jar_stage_path}')
HANDLER = '{handler_name}'
AS
$$
{JAVA_CSV_UDTF_HANDLER_SOURCE}
$$
"""


def drop_java_csv_udtf_sql(function_name: str = JAVA_CSV_UDTF_NAME) -> str:
    return f"DROP FUNCTION IF EXISTS {function_name}(STRING, INTEGER, INTEGER)"
