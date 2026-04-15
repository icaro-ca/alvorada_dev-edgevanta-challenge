import enum
import logging
import os
from dataclasses import dataclass, field
from typing import Any, Dict, Iterator, List, Optional, Tuple

import pandas as pd
from psycopg import connect, Connection
from psycopg.rows import tuple_row

logger = logging.getLogger(__name__)

@dataclass(frozen=True)
class PostgreSQLConnConfig:
    """Immutable connection parameters, populated from environment variables."""
    host:     str = field(default_factory=lambda: os.environ.get("POSTGRESQL_HOST", "localhost"))
    port:     int = field(default_factory=lambda: int(os.environ.get("POSTGRESQL_PORT", "5432")))
    database: str = field(default_factory=lambda: os.environ.get("POSTGRESQL_DATABASE", "etl"))
    user:     str = field(default_factory=lambda: os.environ.get("POSTGRESQL_USER", "etl"))
    password: str = field(default_factory=lambda: os.environ.get("POSTGRESQL_PASSWORD", ""))

class OperationType(enum.Enum):
    """Classifies SQL statements to control execution path in ``_run_command``."""
    DDL = 1   # CREATE, DROP, ALTER, TRUNCATE, DO  — no result set
    DML = 2   # INSERT, UPDATE, DELETE, MERGE       — optional param rows
    DQL = 3   # SELECT                              — returns rows

class PostgreSQLClient:
    """
    Thin psycopg3 wrapper with separate DDL, DML, and DQL execution paths.

    Connections are opened per command and closed in the ``finally`` block,
    keeping the client stateless between calls. NaN values are normalised
    to ``None`` before insert so they map to SQL ``NULL``.
    """

    _DDL_COMMANDS = frozenset({"CREATE", "DROP", "ALTER", "TRUNCATE", "DO"})
    _DML_COMMANDS = frozenset({"INSERT", "UPDATE", "DELETE", "MERGE"})

    def __init__(
        self,
        connection_config: Optional[PostgreSQLConnConfig] = None,
        connect_timeout: int = 10,
        sslmode: Optional[str] = None,
    ) -> None:
        cfg = connection_config or PostgreSQLConnConfig()
        self._host = cfg.host
        self._port = cfg.port
        self._database = cfg.database
        self._user = cfg.user
        self._password = cfg.password
        self._connect_timeout = connect_timeout
        self._sslmode = sslmode
        self._connection: Optional[Connection] = None

    def _connect_db(self) -> None:
        """Open a psycopg3 connection using ``tuple_row`` factory."""
        conninfo = (
            f"host={self._host} port={self._port} dbname={self._database} "
            f"user={self._user} password={self._password} "
            f"connect_timeout={self._connect_timeout}"
        )
        if self._sslmode:
            conninfo += f" sslmode={self._sslmode}"
        try:
            self._connection = connect(conninfo, row_factory=tuple_row)
        except Exception as exc:
            logger.error("Unable to connect to the database: %s", exc)
            raise

    def _disconnect_db(self) -> None:
        """Close the connection, suppressing errors on close."""
        if self._connection is not None:
            try:
                self._connection.close()
            finally:
                self._connection = None

    def _run_command(
        self,
        command: str,
        operation_type: OperationType,
        row_values: Optional[List[Tuple[Any, ...]]] = None,
    ):
        """
        Execute *command* using the appropriate cursor method for its type.

        - DQL  → ``execute`` + ``fetchall``, returns ``(rows, col_names)``.
        - DML  → ``executemany`` when *row_values* provided, else ``execute``.
        - DDL  → ``execute`` with no return value.

        Rolls back and re-raises on any exception.
        """
        try:
            if self._connection is None:
                self._connect_db()

            assert self._connection is not None

            with self._connection.cursor() as cur:
                if operation_type == OperationType.DQL:
                    cur.execute(command)
                    return cur.fetchall(), [desc.name for desc in cur.description]
                elif operation_type == OperationType.DML and row_values:
                    cur.executemany(command, row_values)
                else:
                    cur.execute(command)

            self._connection.commit()

        except Exception as error:
            if self._connection is not None:
                self._connection.rollback()
            logger.error("Error executing command: %s", error)
            raise

        finally:
            self._disconnect_db()

    def _run_streaming_command(
        self,
        command: str,
        fetch_size: int,
        cursor_name: str = "stream_cursor",
    ) -> Iterator[List[Tuple[Any, ...]]]:
        """
        Execute *command* with a server-side named cursor and yield chunks.

        Each yielded item is ``(chunk, col_names)`` where *chunk* is a list
        of tuples of length *fetch_size* (or fewer for the final chunk).
        """
        try:
            if self._connection is None:
                self._connect_db()

            assert self._connection is not None

            with self._connection.cursor(name=cursor_name) as cur:
                cur.itersize = fetch_size
                cur.execute(command)
                while True:
                    chunk = cur.fetchmany(fetch_size)
                    if not chunk:
                        break
                    yield chunk, [desc.name for desc in cur.description]

        except Exception as error:
            logger.error("Error executing streaming command: %s", error)
            raise
        finally:
            self._disconnect_db()

    @staticmethod
    def _validate_sql_statement(statement: str, allowed_commands: frozenset) -> None:
        """Raise ``ValueError`` if the first token of *statement* is not in *allowed_commands*."""
        first_token = statement.strip().split()[0].upper() if statement.strip() else ""
        if not first_token:
            raise ValueError("SQL statement cannot be empty.")
        if first_token not in allowed_commands:
            raise ValueError(
                f"Statement '{first_token}' is not allowed here. "
                f"Expected one of: {sorted(allowed_commands)}"
            )

    def create_table(
        self,
        schema_name: str,
        table_name: str,
        table_schema: Dict[str, str],
    ) -> None:
        """Create *table_name* in *schema_name* if it does not already exist."""
        col_defs = ",\n".join(f'    "{k}" {v}' for k, v in table_schema.items())
        command  = (
            f'CREATE TABLE IF NOT EXISTS "{schema_name}"."{table_name}" (\n'
            f'{col_defs}\n);'
        )
        self._run_command(command, OperationType.DDL)

    def drop_table(self, schema_name: str, table_name: str) -> None:
        """Drop *table_name* from *schema_name* if it exists."""
        self._run_command(
            f'DROP TABLE IF EXISTS "{schema_name}"."{table_name}";',
            OperationType.DDL,
        )

    def execute_ddl(self, statement: str) -> None:
        """Execute a raw DDL statement after validating its first token."""
        self._validate_sql_statement(statement, self._DDL_COMMANDS)
        self._run_command(statement, OperationType.DDL)

    def insert_rows(
        self,
        schema_name: str,
        table_name: str,
        columns: List[str],
        rows: List[Tuple[Any, ...]],
    ) -> None:
        """
        Bulk-insert *rows* into *table_name*.

        ``NaN`` values are converted to ``None`` before insert so they
        are stored as SQL ``NULL``.
        """
        col_names    = ", ".join(f'"{c}"' for c in columns)
        placeholders = ", ".join(["%s"] * len(columns))
        command      = (
            f'INSERT INTO "{schema_name}"."{table_name}" '
            f'({col_names}) VALUES ({placeholders});'
        )
        cleaned_rows = [
            tuple(None if (val is not None and pd.isna(val)) else val for val in row)
            for row in rows
        ]
        self._run_command(command, OperationType.DML, cleaned_rows)

    def execute_dml(
        self,
        statement: str,
        rows: Optional[List[Tuple[Any, ...]]] = None,
    ) -> None:
        """Execute a raw DML statement after validating its first token."""
        self._validate_sql_statement(statement, self._DML_COMMANDS)
        self._run_command(statement, OperationType.DML, row_values=rows)

    def table_exists(self, schema_name: str, table_name: str) -> bool:
        """Return ``True`` if *table_name* exists in *schema_name*."""
        command = f"""
        SELECT EXISTS (
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = '{schema_name}' AND table_name = '{table_name}'
        );
        """
        result, _ = self._run_command(command, OperationType.DQL)
        return bool(result and result[0] and result[0][0])

    def select_data(self, query: str) -> List[Tuple[Any, ...]]:
        """Execute *query* and return all rows as a list of tuples."""
        rows, _ = self._run_command(query, OperationType.DQL)
        return rows

    def select_data_as_df(self, query: str) -> pd.DataFrame:
        """Execute *query* and return all rows as a DataFrame."""
        rows, col_names = self._run_command(query, OperationType.DQL)
        return pd.DataFrame.from_records(rows, columns=col_names)

    def stream_data(self, query: str, fetch_size: int = 1000) -> Iterator[List[Tuple[Any, ...]]]:
        """Yield chunks of rows from *query* using a server-side cursor."""
        for chunk, _ in self._run_streaming_command(query, fetch_size=fetch_size):
            yield chunk

    def stream_data_as_df(self, query: str, fetch_size: int = 1000) -> Iterator[pd.DataFrame]:
        """Yield chunks of rows from *query* as DataFrames."""
        for chunk, col_names in self._run_streaming_command(query, fetch_size=fetch_size):
            yield pd.DataFrame.from_records(chunk, columns=col_names)