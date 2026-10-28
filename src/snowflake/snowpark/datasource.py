#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
"""Python custom data sources in Snowpark.

A custom data source lets you read data from any external system, such as a REST API, a
SaaS application, or a proprietary file format, into a Snowpark :class:`~snowflake.snowpark.DataFrame`.
The API follows `PySpark's Python Data Source API
<https://spark.apache.org/docs/latest/api/python/tutorial/sql/python_data_source.html>`_:
you implement :class:`DataSource` and :class:`DataSourceReader`, register the data source
with :attr:`Session.data_source <snowflake.snowpark.Session.data_source>`, and load it with
:meth:`DataFrameReader.format() <snowflake.snowpark.DataFrameReader.format>`.

Snowpark plans the read on the client by calling :meth:`DataSource.schema`,
:meth:`DataSource.reader` and :meth:`DataSourceReader.partitions`, then calls
:meth:`DataSourceReader.read` once per partition in parallel and loads the results
into a temporary table. ``read()`` runs in one of two places:

- **Local ingestion** (default): ``read()`` runs in threads (or processes) on the client. Data is
  staged as Parquet files and copied into a temporary table when
  :meth:`~snowflake.snowpark.DataFrameReader.load` is called.
- **Server-side ingestion**: when the ``snowpark.external_access_integration`` option is set,
  ``read()`` runs inside Snowflake in a temporary UDTF, one UDTF partition per input partition.
  Data is read lazily when an action is called on the returned DataFrame.

Options whose names start with ``snowpark.`` configure ingestion and are not passed to the data source:

- ``snowpark.external_access_integration``: Name of the external access integration that allows
  the UDTF to reach the external system. Setting it enables server-side ingestion.
- ``snowpark.packages``: List of packages required by ``read()`` in server-side ingestion.
- ``snowpark.imports``: List of stage files or local files and modules to import in server-side ingestion.
- ``snowpark.artifact_repository``: Artifact repository to resolve ``snowpark.packages`` from.
- ``snowpark.secrets``: Dictionary mapping secret aliases to secret names, used in server-side ingestion.
  Read them in ``read()`` with ``_snowflake.get_generic_secret_string(alias)``.
- ``snowpark.max_workers``: Number of threads or processes used in local ingestion. Defaults to the number of CPUs.
- ``snowpark.fetch_with_process``: Whether to use processes instead of threads in local ingestion. Defaults to ``False``.
- ``snowpark.batch_size``: Maximum number of rows staged per Parquet file in local ingestion. Defaults to 100000.

Example::

    >>> from snowflake.snowpark.datasource import DataSource, DataSourceReader, InputPartition
    >>> class RangeDataSource(DataSource):
    ...     @classmethod
    ...     def name(cls):
    ...         return "range_source"
    ...     def schema(self):
    ...         return "id INT, label STRING"
    ...     def reader(self, schema):
    ...         return RangeReader(int(self.options["n"]))
    >>> class RangeReader(DataSourceReader):
    ...     def __init__(self, n):
    ...         self.n = n
    ...     def partitions(self):
    ...         return [InputPartition(start) for start in range(0, self.n, 2)]
    ...     def read(self, partition):
    ...         for i in range(partition.value, min(partition.value + 2, self.n)):
    ...             yield (i, f"row {i}")
    >>> session.data_source.register(RangeDataSource)
    >>> df = session.read.format("range_source").option("n", 5).load()
    >>> df.sort("id").collect()
    [Row(ID=0, LABEL='row 0'), Row(ID=1, LABEL='row 1'), Row(ID=2, LABEL='row 2'), Row(ID=3, LABEL='row 3'), Row(ID=4, LABEL='row 4')]
"""

import threading
from abc import ABC, abstractmethod
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Iterator,
    List,
    Optional,
    Sequence,
    Tuple,
    Type,
    Union,
)

from snowflake.snowpark.types import StructType

if TYPE_CHECKING:
    import snowflake.snowpark
    from snowflake.snowpark.row import Row

    try:
        import pyarrow
    except ImportError:  # pragma: no cover
        pass

__all__ = [
    "DataSource",
    "DataSourceReader",
    "DataSourceRegistration",
    "InputPartition",
]

# Formats implemented by Snowpark itself. A custom data source cannot be registered under these names.
_RESERVED_DATA_SOURCE_NAMES = frozenset(
    [
        "avro",
        "csv",
        "dbapi",
        "documents",
        "jdbc",
        "json",
        "orc",
        "parquet",
        "xml",
    ]
)


class DataSource(ABC):  # noqa: B024 (matches PySpark; no method is required)
    """A base class for custom data sources.

    Subclass it and implement :meth:`schema` and :meth:`reader`. Optionally override
    :meth:`name` to choose the format name used in :meth:`DataFrameReader.format()
    <snowflake.snowpark.DataFrameReader.format>`.

    Snowpark creates one instance per :meth:`~snowflake.snowpark.DataFrameReader.load` call
    with the options set by :meth:`~snowflake.snowpark.DataFrameReader.option`, excluding the
    options that start with ``snowpark.``. The instance must be picklable.

    Args:
        options: A case-insensitive dictionary of the user options. Values are strings,
            following PySpark. Boolean values are converted to ``"true"`` or ``"false"``.
    """

    def __init__(self, options: Dict[str, str]) -> None:
        self.options = options

    @classmethod
    def name(cls) -> str:
        """Returns the format name of this data source. Defaults to the class name."""
        return cls.__name__

    def schema(self) -> Union[StructType, str]:
        """Returns the schema of the data, as a :class:`~snowflake.snowpark.types.StructType`
        or a DDL string such as ``"id INT, name STRING"``.

        This method is not called if a schema is set with
        :meth:`DataFrameReader.schema() <snowflake.snowpark.DataFrameReader.schema>`.
        """
        raise NotImplementedError(
            f"{type(self).__name__}.schema() is not implemented. Implement it or provide "
            "a schema with DataFrameReader.schema()."
        )

    def reader(self, schema: StructType) -> "DataSourceReader":
        """Returns a :class:`DataSourceReader` that reads data for the given schema.

        Args:
            schema: The resolved schema, from :meth:`DataFrameReader.schema()
                <snowflake.snowpark.DataFrameReader.schema>` or :meth:`schema`.
        """
        raise NotImplementedError(f"{type(self).__name__}.reader() is not implemented.")


class InputPartition:
    """A partition of the data to read, passed to :meth:`DataSourceReader.read`.

    The partition must be picklable. Subclass it to carry more partition metadata.

    Args:
        value: Any picklable value that identifies the partition.
    """

    def __init__(self, value: Any) -> None:
        self.value = value

    def __repr__(self) -> str:
        attributes = ", ".join(f"{k}={v!r}" for k, v in self.__dict__.items())
        return f"{type(self).__name__}({attributes})"

    def __eq__(self, other: object) -> bool:
        return type(self) is type(other) and self.__dict__ == other.__dict__


class DataSourceReader(ABC):
    """A base class for data source readers.

    :meth:`partitions` is called once on the client. :meth:`read` is called once per
    partition, on the client or in a Snowflake UDTF, so the reader must be picklable.
    Open connections or clients inside :meth:`read` rather than in ``__init__``.
    """

    def partitions(self) -> Sequence[InputPartition]:
        """Returns the partitions of the data. Each partition is read by a separate
        :meth:`read` call, and partitions are read in parallel.

        Defaults to a single partition, ``InputPartition(None)``.
        """
        return [InputPartition(None)]

    @abstractmethod
    def read(
        self, partition: InputPartition
    ) -> Iterator[Union[Tuple, "Row", Dict[str, Any], "pyarrow.RecordBatch"]]:
        """Reads the data of one partition.

        Yields rows or `pyarrow.RecordBatch <https://arrow.apache.org/docs/python/generated/pyarrow.RecordBatch.html>`_ objects. A row is one of:

        - a tuple or list with a value for each schema field, in schema order;
        - a :class:`~snowflake.snowpark.Row`, matched to the schema by field name if it has
          field names, otherwise by position;
        - a dict, matched to the schema by field name.

        Field names are matched case-insensitively unless the schema field name is quoted.
        Columns of a ``RecordBatch`` are matched to the schema by name the same way, or by
        position when the names don't match and the number of columns is the same.

        Args:
            partition: One of the partitions returned by :meth:`partitions`.
        """
        pass


class DataSourceRegistration:
    """Registers custom data sources for a session. Access an instance with
    :attr:`Session.data_source <snowflake.snowpark.Session.data_source>`.

    Registered data sources are used with :meth:`DataFrameReader.format()
    <snowflake.snowpark.DataFrameReader.format>`. Format names are case-insensitive.
    """

    def __init__(self, session: "snowflake.snowpark.Session") -> None:
        self._session = session
        self._data_sources: Dict[str, Type[DataSource]] = {}
        self._lock = threading.RLock()

    def register(self, data_source: Type[DataSource]) -> None:
        """Registers a custom data source under the name returned by :meth:`DataSource.name`.

        Registering a name that is already registered replaces the previous data source.

        Args:
            data_source: A subclass of :class:`DataSource`.
        """
        name = _validate_data_source_class(data_source)
        with self._lock:
            self._data_sources[name] = data_source

    def unregister(self, name: str) -> None:
        """Unregisters the custom data source with the given name.

        Args:
            name: The format name of the data source.
        """
        key = name.strip().lower()
        with self._lock:
            if key not in self._data_sources:
                raise KeyError(f"Data source '{name}' is not registered.")
            del self._data_sources[key]

    def registered_names(self) -> List[str]:
        """Returns the format names of the registered data sources."""
        with self._lock:
            return list(self._data_sources.keys())

    def _get(self, name: str) -> Optional[Type[DataSource]]:
        with self._lock:
            return self._data_sources.get(name.strip().lower())


def _validate_data_source_class(data_source: Any) -> str:
    """Validates a custom data source class and returns its canonical (lower-case) format name."""
    if not (isinstance(data_source, type) and issubclass(data_source, DataSource)):
        raise TypeError(
            "A custom data source must be a subclass of snowflake.snowpark.datasource.DataSource, "
            f"got {data_source!r}."
        )
    name = data_source.name()
    if not isinstance(name, str) or not name.strip():
        raise ValueError(
            f"{data_source.__name__}.name() must return a non-empty string, got {name!r}."
        )
    canonical_name = name.strip().lower()
    if canonical_name in _RESERVED_DATA_SOURCE_NAMES:
        raise ValueError(
            f"Data source name '{name}' is reserved by Snowpark. Reserved names are "
            f"{sorted(_RESERVED_DATA_SOURCE_NAMES)}. Override {data_source.__name__}.name() to use another name."
        )
    return canonical_name
