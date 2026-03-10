# CSV Reader UDTF POC Flow

This document explains how to use `csv_reader.py` as a standalone UDTF handler POC.

## What the handler does

- Class: `CSVReader`
- Entry point: `CSVReader.process(filename, num_workers, i, custom_schema)`
- Output shape: each yielded row is `(row_dict,)`, where `row_dict` is intended for a VARIANT output column.

`csv_reader.py` follows the same worker-splitting pattern as `xml_reader.py`:

1. Compute this worker's approximate byte range from `num_workers` and worker id `i`.
2. Align range start/end to line breaks so partial records are not emitted.
3. Parse rows in this range with minimal CSV settings.
4. Convert parsed row values to a dictionary and `yield (dict,)`.

## CSV behavior in this POC

- Delimiter: `,`
- Quote char: `"`
- Record delimiter: newline
- Malformed rows: skipped
- Multiline quoted records: not supported in this POC

## Schema behavior

- `custom_schema` is a Snowpark schema string that should represent a `StructType`.
- When schema is provided:
  - output keys use schema field names;
  - missing CSV columns are padded with `None`;
  - extra CSV columns are ignored.
- When schema is empty:
  - keys are generated as `C1`, `C2`, `C3`, ...

## Example registration flow (POC)

The code below shows a typical flow to register and call this handler from Snowpark:

```python
from snowflake.snowpark.types import StructField, StructType, VariantType
from snowflake.snowpark._internal.udf_utils import get_types_from_type_hints
from snowflake.snowpark._internal.utils import TempObjectType

python_file_path = "src/snowflake/snowpark/_internal/csv_reader.py"
handler_name = "CSVReader"

_, input_types = get_types_from_type_hints(
    (python_file_path, handler_name), TempObjectType.TABLE_FUNCTION
)

output_schema = StructType([StructField("ROW_DATA", VariantType(), True)])

csv_reader_udtf = session.udtf.register_from_file(
    python_file_path,
    handler_name,
    output_schema=output_schema,
    input_types=input_types,
    packages=["snowflake-snowpark-python"],
    replace=True,
)

# Example invocation shape:
# csv_reader_udtf(lit("@stage/path/file.csv"), lit(8), col("WORKER"), lit(schema_string))
```

## Notes

- This is intentionally isolated from `DataFrameReader.csv()` and analyzer integration.
- Future integration can follow the `_create_xml_query` pattern in `snowflake_plan.py`.
