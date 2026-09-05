import logging
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, cast

from clickhouse_connect.datatypes.base import ClickHouseType
from clickhouse_connect.datatypes.registry import get_from_name
from clickhouse_connect.driver import Client
from clickhouse_connect.driver.binding import _query_is_insert, external_bind_re
from clickhouse_connect.driver.common import unescape_identifier
from clickhouse_connect.driver.exceptions import DatabaseError, ProgrammingError

logger = logging.getLogger(__name__)

str_type = get_from_name("String")
int_type = get_from_name("Int32")
_IMPLICIT_NULLABLE_BASE_TYPES = frozenset(("Dynamic", "Variant"))
_BARE_VALUES_TAIL_RE = re.compile(r"\bVALUES\s*;?\s*$", re.IGNORECASE)
_PYFORMAT_PLACEHOLDER_RE = re.compile(r"%s|%\([^)]+\)s")


@dataclass(frozen=True)
class _NativeInsertPlan:
    table: str
    column_names: tuple[str, ...]
    parameter_keys: tuple[str, ...]
    settings: dict[str, Any] | None = None


@dataclass(frozen=True)
class _BareValuesInsert:
    table: str
    column_names: tuple[str, ...] | None


@dataclass(frozen=True)
class _ScannedSql:
    text: str
    has_placeholder: bool
    valid: bool


def _scan_insert_sql(sql: str) -> _ScannedSql:
    uncommented: list[str] = []
    unquoted: list[str] = []
    pos = 0
    length = len(sql)
    valid = True
    while pos < length:
        char = sql[pos]
        if char in ("'", '"', "`"):
            quote = char
            uncommented.append(char)
            unquoted.append(" ")
            pos += 1
            closed = False
            while pos < length:
                char = sql[pos]
                uncommented.append(char)
                unquoted.append(" ")
                pos += 1
                if char == "\\" and pos < length:
                    uncommented.append(sql[pos])
                    unquoted.append(" ")
                    pos += 1
                elif char == quote:
                    if pos < length and sql[pos] == quote:
                        uncommented.append(sql[pos])
                        unquoted.append(" ")
                        pos += 1
                    else:
                        closed = True
                        break
            if not closed:
                valid = False
            continue
        if sql.startswith("/*", pos):
            uncommented.append(" ")
            unquoted.append(" ")
            pos += 2
            depth = 1
            while pos < length and depth:
                if sql.startswith("/*", pos):
                    depth += 1
                    pos += 2
                elif sql.startswith("*/", pos):
                    depth -= 1
                    pos += 2
                else:
                    pos += 1
            if depth:
                valid = False
            continue
        if sql.startswith("--", pos) or sql.startswith("//", pos) or sql.startswith("#!", pos) or sql.startswith("# ", pos):
            next_line = sql.find("\n", pos + 2)
            if next_line == -1:
                break
            uncommented.append("\n")
            unquoted.append("\n")
            pos = next_line + 1
            continue
        uncommented.append(char)
        unquoted.append(char)
        pos += 1
    code = "".join(unquoted)
    has_placeholder = bool(_PYFORMAT_PLACEHOLDER_RE.search(code) or external_bind_re.search(code))
    return _ScannedSql("".join(uncommented), has_placeholder, valid)


def _skip_whitespace(sql: str, pos: int) -> int:
    while pos < len(sql) and sql[pos].isspace():
        pos += 1
    return pos


def _parse_keyword(sql: str, pos: int, keyword: str) -> int | None:
    pos = _skip_whitespace(sql, pos)
    end = pos + len(keyword)
    if sql[pos:end].upper() != keyword:
        return None
    if end < len(sql) and (sql[end].isalnum() or sql[end] == "_"):
        return None
    return end


def _parse_identifier(sql: str, pos: int) -> tuple[str, int] | None:
    pos = _skip_whitespace(sql, pos)
    if pos >= len(sql):
        return None
    quote = sql[pos] if sql[pos] in ('"', "`") else None
    if quote:
        start = pos
        pos += 1
        while pos < len(sql):
            char = sql[pos]
            if char == "\\" and pos + 1 < len(sql):
                pos += 2
            elif char == quote:
                if pos + 1 < len(sql) and sql[pos + 1] == quote:
                    pos += 2
                else:
                    return sql[start : pos + 1], pos + 1
            else:
                pos += 1
        return None
    match = re.match(r"[A-Za-z_][A-Za-z0-9_]*", sql[pos:])
    if match is None:
        return None
    return match.group(0), pos + len(match.group(0))


def _parse_qualified_identifier(sql: str, pos: int) -> tuple[str, int] | None:
    parsed = _parse_identifier(sql, pos)
    if parsed is None:
        return None
    parts = [parsed[0]]
    pos = parsed[1]
    while True:
        dot_pos = _skip_whitespace(sql, pos)
        if dot_pos >= len(sql) or sql[dot_pos] != ".":
            return ".".join(parts), pos
        parsed = _parse_identifier(sql, dot_pos + 1)
        if parsed is None:
            return None
        parts.append(parsed[0])
        pos = parsed[1]


def _parse_column_list(sql: str, pos: int) -> tuple[tuple[str, ...], int] | None:
    pos = _skip_whitespace(sql, pos)
    if pos >= len(sql) or sql[pos] != "(":
        return None
    columns: list[str] = []
    pos += 1
    while True:
        parsed = _parse_qualified_identifier(sql, pos)
        if parsed is None:
            return None
        raw_column, pos = parsed
        columns.append(unescape_identifier(raw_column))
        pos = _skip_whitespace(sql, pos)
        if pos >= len(sql):
            return None
        if sql[pos] == ")":
            return tuple(columns), pos + 1
        if sql[pos] != ",":
            return None
        pos += 1


def _bare_values_insert(scan: _ScannedSql, *, pyformat_encoded: bool) -> _BareValuesInsert | None:
    if not scan.valid:
        return None
    sql = scan.text.strip()
    if pyformat_encoded:
        sql = sql.replace("%%", "%")
    if sql.endswith(";"):
        sql = sql[:-1].rstrip()
    pos = _parse_keyword(sql, 0, "INSERT")
    if pos is None:
        return None
    pos = _parse_keyword(sql, pos, "INTO")
    if pos is None:
        return None
    table_keyword = _parse_keyword(sql, pos, "TABLE")
    if table_keyword is not None:
        pos = table_keyword
    parsed_table = _parse_qualified_identifier(sql, pos)
    if parsed_table is None:
        return None
    table, pos = parsed_table
    pos = _skip_whitespace(sql, pos)
    column_names = None
    if pos < len(sql) and sql[pos] == "(":
        parsed_columns = _parse_column_list(sql, pos)
        if parsed_columns is None:
            return None
        column_names, pos = parsed_columns
    pos = _parse_keyword(sql, pos, "VALUES")
    if pos is None or _skip_whitespace(sql, pos) != len(sql):
        return None
    return _BareValuesInsert(table, column_names)


def _cursor_null_ok(ch_type: Any) -> bool | None:
    if isinstance(ch_type, str):
        try:
            ch_type = get_from_name(ch_type)
        except (DatabaseError, ValueError, TypeError, IndexError):
            return None
    if not isinstance(ch_type, ClickHouseType):
        return None
    if ch_type.nullable or ch_type.base_type in _IMPLICIT_NULLABLE_BASE_TYPES:
        return True
    if ch_type.base_type == "SimpleAggregateFunction":
        return _cursor_null_ok(getattr(ch_type, "element_type", None))
    return False


def _leading_keyword(sql: str) -> str:
    pos = 0
    length = len(sql)
    while pos < length:
        char = sql[pos]
        if char.isspace():
            pos += 1
            continue
        if sql.startswith("--", pos) or sql.startswith("//", pos) or sql.startswith("#!", pos) or sql.startswith("# ", pos):
            next_line = sql.find("\n", pos + 1)
            if next_line == -1:
                return ""
            pos = next_line + 1
            continue
        if sql.startswith("/*", pos):
            pos += 2
            depth = 1
            while pos < length and depth:
                if sql.startswith("/*", pos):
                    depth += 1
                    pos += 2
                elif sql.startswith("*/", pos):
                    depth -= 1
                    pos += 2
                else:
                    pos += 1
            if depth:
                return ""
            continue
        break
    match = re.match(r"[A-Za-z_]+", sql[pos:])
    return "" if match is None else match.group(0).upper()


class Cursor:
    """
    See :ref:`https://peps.python.org/pep-0249/`
    """

    def __init__(self, client: Client):
        self.client = client
        self.arraysize: int = 1
        self.data: Sequence | None = None
        self.names: Sequence[str] = []
        self.types: Sequence[Any] = []
        self._rowcount: int = 0
        self._summary: list[dict[str, Any]] = []
        self._ix: int = 0

    def check_valid(self) -> None:
        if self.data is None:
            raise ProgrammingError("Cursor is not valid")

    @property
    def description(self) -> list[tuple[str, Any, None, None, None, None, bool | None]]:
        return [(n, t, None, None, None, None, _cursor_null_ok(t)) for n, t in zip(self.names, self.types)]

    @property
    def rowcount(self) -> int:
        return self._rowcount

    @property
    def summary(self) -> list[dict[str, Any]]:
        return self._summary

    def close(self) -> None:
        self.data = None

    def execute(
        self,
        operation: str,
        parameters: Any = None,
        settings: dict[str, Any] | None = None,
        query_formats: dict[str, str] | None = None,
        *,
        pyformat_encoded: bool = True,
    ) -> None:
        Cursor._execute(
            self,
            operation,
            parameters,
            settings,
            query_formats,
            pyformat_encoded=pyformat_encoded,
            internal=False,
        )

    def _execute(
        self,
        operation: str,
        parameters: Any = None,
        settings: dict[str, Any] | None = None,
        query_formats: dict[str, str] | None = None,
        *,
        pyformat_encoded: bool = True,
        internal: bool = False,
    ) -> None:
        if pyformat_encoded and not parameters and isinstance(operation, str):
            # Per PEP 249 pyformat paramstyle, callers (e.g. SQLAlchemy) escape
            # literal percent signs as %% in operation strings.  When there are
            # parameters, Python's % operator in finalize_query handles the
            # unescaping automatically.  When there are no parameters,
            # finalize_query short-circuits, so we must unescape here.
            operation = operation.replace("%%", "%")
        if internal:
            # Dialect metadata statements decode with the Python codec in every native_codec mode.
            context = self.client.create_query_context(
                query=operation, parameters=parameters, settings=settings, query_formats=query_formats
            )
            context.internal = True
            query_result = self.client.query(
                operation,
                parameters,
                settings=settings,
                query_formats=query_formats,
                context=context,
            )
        else:
            query_result = self.client.query(operation, parameters, settings=settings, query_formats=query_formats)
        self.data = query_result.result_set
        self._rowcount = len(self.data)
        self._summary.append(query_result.summary)

        # Need to reset cursor _ix after performing an execute
        self._ix = 0
        self.names = []
        self.types = []

        if query_result.column_names:
            self.names = query_result.column_names
            self.types = [x.name for x in query_result.column_types]
        elif self.data:
            self.names = [f"col_{x}" for x in range(len(self.data[0]))]
            self.types = [x.__class__ for x in self.data[0]]
        else:
            stripped = operation.strip().rstrip(";").strip()
            if _leading_keyword(stripped) in ("SELECT", "WITH"):
                # Introspection re-query carries the same settings/formats so the derived column shape matches.
                try:
                    meta_result = self.client.query(
                        f"SELECT * FROM ({stripped}) LIMIT 0",
                        parameters,
                        settings=settings,
                        query_formats=query_formats,
                    )
                except DatabaseError:
                    logger.debug("DB-API cursor metadata probe failed; leaving description empty", exc_info=True)
                    return
                if meta_result.column_names:
                    self.names = meta_result.column_names
                    self.types = [x.name for x in meta_result.column_types]

    def _executemany_native(self, plan: _NativeInsertPlan, parameters: Any) -> None:
        self.names = []
        self.types = []
        self.data = []
        self._rowcount = 0
        self._ix = 0

        expected_keys = set(plan.parameter_keys)
        if len(expected_keys) != len(plan.parameter_keys) or len(plan.column_names) != len(plan.parameter_keys):
            raise ProgrammingError("Invalid SQLAlchemy Native insert plan")

        if not isinstance(parameters, Sequence):
            raise ProgrammingError("Native SQLAlchemy inserts require a sequence of mapping parameters")
        data_values: list[list[Any]] = []
        for row in parameters:
            if not isinstance(row, Mapping) or set(row) != expected_keys:
                raise ProgrammingError("Native SQLAlchemy inserts require matching mapping parameters")
            data_values.append([row[key] for key in plan.parameter_keys])

        if not data_values:
            return

        insert_summary = self.client.insert(
            plan.table,
            data_values,
            plan.column_names,
            settings=plan.settings,
        )
        self._rowcount = insert_summary.written_rows
        self._summary.append(insert_summary.summary)

    def _executemany_bare_values(
        self,
        plan: _BareValuesInsert,
        parameters: Any,
        settings: dict[str, Any] | None,
    ) -> None:
        try:
            rows = list(parameters)
        except TypeError as ex:
            raise ProgrammingError("Placeholder-less INSERT ... VALUES requires iterable rows") from ex
        if not rows:
            return

        first_row = rows[0]
        if isinstance(first_row, Mapping):
            if plan.column_names is None:
                if not first_row or not all(isinstance(key, str) for key in first_row):
                    raise ProgrammingError("Placeholder-less INSERT mapping rows require string column names")
                column_names = tuple(first_row)
            else:
                column_names = plan.column_names
            expected_keys = set(column_names)
            if len(expected_keys) != len(column_names):
                raise ProgrammingError("Placeholder-less INSERT column names must be unique")
            data_values: list[list[Any]] = []
            for row in rows:
                if not isinstance(row, Mapping) or set(row) != expected_keys:
                    raise ProgrammingError("Placeholder-less INSERT requires matching mapping rows")
                data_values.append([row[key] for key in column_names])
            insert_columns: str | tuple[str, ...] = column_names
        elif isinstance(first_row, Sequence) and not isinstance(first_row, (str, bytes, bytearray)):
            expected_width = len(plan.column_names) if plan.column_names is not None else len(first_row)
            if expected_width == 0:
                raise ProgrammingError("Placeholder-less INSERT rows cannot be empty")
            data_values = []
            for row in rows:
                if not isinstance(row, Sequence) or isinstance(row, (str, bytes, bytearray)) or len(row) != expected_width:
                    raise ProgrammingError("Placeholder-less INSERT requires equal-width sequence rows")
                data_values.append(list(row))
            insert_columns = plan.column_names if plan.column_names is not None else "*"
        else:
            raise ProgrammingError("Placeholder-less INSERT requires mapping or sequence rows")

        insert_summary = self.client.insert(
            plan.table,
            data_values,
            insert_columns,
            settings=settings,
        )
        self._rowcount = insert_summary.written_rows
        self._summary.append(insert_summary.summary)

    def executemany(
        self,
        operation: str,
        parameters: Any,
        settings: dict[str, Any] | None = None,
        query_formats: dict[str, str] | None = None,
        *,
        pyformat_encoded: bool = True,
    ) -> None:
        self.names = []
        self.types = []
        self.data = []
        self._rowcount = 0
        self._ix = 0
        if not parameters:
            return
        scan = _scan_insert_sql(operation)
        bare_values_plan = _bare_values_insert(scan, pyformat_encoded=pyformat_encoded)
        if bare_values_plan is not None:
            self._executemany_bare_values(bare_values_plan, parameters, settings)
            return
        if _leading_keyword(scan.text) == "INSERT" and _BARE_VALUES_TAIL_RE.search(scan.text) and not scan.has_placeholder:
            raise ProgrammingError("Cursor.executemany() cannot safely route this placeholder-less INSERT statement")
        keyword = _leading_keyword(operation)
        written_rows = 0
        has_reliable_written_rows = True
        expects_rows = keyword == "SELECT" or (keyword == "WITH" and not _query_is_insert(operation))
        has_result_columns = False
        operation_without_pyformat_escapes = operation.replace("%%", "%") if pyformat_encoded else operation
        try:
            for param_row in parameters:
                row_operation = operation_without_pyformat_escapes if not param_row else operation
                query_result = self.client.query(row_operation, param_row, settings=settings, query_formats=query_formats)
                self.data.extend(query_result.result_set)
                has_result_columns = has_result_columns or bool(query_result.column_names)
                if has_reliable_written_rows:
                    summary_written_rows = query_result.summary.get("written_rows")
                    try:
                        summary_written_rows = int(cast(Any, summary_written_rows))
                    except (TypeError, ValueError):
                        has_reliable_written_rows = False
                    else:
                        if summary_written_rows < 0:
                            has_reliable_written_rows = False
                        else:
                            written_rows += summary_written_rows
                if self.names or self.types:
                    if query_result.column_names != self.names:
                        logger.warning(
                            "Inconsistent column names %s : %s for operation %s in cursor executemany",
                            self.names,
                            query_result.column_names,
                            operation,
                        )
                else:
                    self.names = query_result.column_names
                    self.types = query_result.column_types
                self._summary.append(query_result.summary)
        except TypeError as ex:
            raise ProgrammingError(f"Invalid parameters {parameters} passed to cursor executemany") from ex
        if has_result_columns or self.data:
            self._rowcount = len(self.data)
        elif has_reliable_written_rows:
            self._rowcount = written_rows
        elif expects_rows:
            self._rowcount = len(self.data)
        else:
            self._rowcount = -1

    def fetchall(self) -> Sequence:
        self.check_valid()
        data = cast(Sequence, self.data)
        ret = data[self._ix :]
        self._ix = len(data)
        return ret

    def fetchone(self) -> Any:
        self.check_valid()
        data = cast(Sequence, self.data)
        if self._ix >= len(data):
            return None
        val = data[self._ix]
        self._ix += 1
        return val

    def fetchmany(self, size: int = -1) -> Sequence:
        self.check_valid()
        data = cast(Sequence, self.data)

        if size < 0:
            # Fetch all remaining rows
            size = len(data) - self._ix
        elif size == 0:
            # Return empty list for size=0
            return []

        end = min(self._ix + size, len(data))
        ret = data[self._ix : end]
        self._ix = end
        return ret

    def nextset(self) -> None:
        raise NotImplementedError

    def callproc(self, *args, **kwargs) -> None:
        raise NotImplementedError
