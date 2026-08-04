import logging
import re
from collections.abc import Mapping, Sequence
from typing import Any, cast

from clickhouse_connect.datatypes.base import ClickHouseType
from clickhouse_connect.datatypes.registry import get_from_name
from clickhouse_connect.driver import Client
from clickhouse_connect.driver.common import unescape_identifier
from clickhouse_connect.driver.exceptions import DatabaseError, ProgrammingError
from clickhouse_connect.driver.parser import parse_callable
from clickhouse_connect.driver.query import remove_sql_comments

logger = logging.getLogger(__name__)

# DOTALL because a formatted INSERT commonly spans multiple lines, so the target and the VALUES
# keyword are not necessarily on the same line as the INSERT INTO
insert_re = re.compile(r"^\s*INSERT\s+INTO\s+(.*)$", re.IGNORECASE | re.DOTALL)
# A table identifier ends at the first whitespace character or at the opening paren of a column list
_table_end_re = re.compile(r"[\s(]")
# ClickHouse lexes an unquoted identifier or keyword as a run of ASCII word characters or dollar signs
_unquoted_word_re = re.compile(r"[0-9A-Za-z_$]+")
str_type = get_from_name("String")
int_type = get_from_name("Int32")
_IMPLICIT_NULLABLE_BASE_TYPES = frozenset(("Dynamic", "Variant"))


def _skip_keyword(expr: str, keyword: str) -> str | None:
    """
    Remove a leading unquoted keyword, and the whitespace that must follow it, from expr.

    :param expr: Expression to strip the keyword from
    :param keyword: Upper case keyword to look for
    :return: The remainder of expr after the keyword, or None if expr does not start with the keyword
    """
    match = _unquoted_word_re.match(expr)
    if match is None or match.group(0).upper() != keyword:
        return None
    remainder = expr[match.end() :]
    if not remainder[:1].isspace():
        return None
    return remainder.lstrip()


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

    def execute(self, operation: str, parameters: Any = None, settings: dict[str, Any] | None = None) -> None:
        if not parameters and isinstance(operation, str):
            # Per PEP 249 pyformat paramstyle, callers (e.g. SQLAlchemy) escape
            # literal percent signs as %% in operation strings.  When there are
            # parameters, Python's % operator in finalize_query handles the
            # unescaping automatically.  When there are no parameters,
            # finalize_query short-circuits, so we must unescape here.
            operation = operation.replace("%%", "%")
        query_result = self.client.query(operation, parameters, settings=settings)
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
                # Introspection re-query carries the same settings so the derived column shape matches.
                try:
                    meta_result = self.client.query(f"SELECT * FROM ({stripped}) LIMIT 0", parameters, settings=settings)
                except DatabaseError:
                    logger.debug("DB-API cursor metadata probe failed; leaving description empty", exc_info=True)
                    return
                if meta_result.column_names:
                    self.names = meta_result.column_names
                    self.types = [x.name for x in meta_result.column_types]

    def _try_bulk_insert(self, operation: str, data: Any, settings: dict[str, Any] | None = None) -> bool:
        match = insert_re.match(remove_sql_comments(operation))
        if not match:
            return False
        temp = match.group(1).lstrip()
        # ClickHouse accepts an optional TABLE keyword before the target of an INSERT
        without_table_keyword = _skip_keyword(temp, "TABLE")
        if without_table_keyword is not None:
            temp = without_table_keyword
        if _skip_keyword(temp, "FUNCTION") is not None:
            return False  # INSERT INTO [TABLE] FUNCTION targets a table function, which has no table to insert into
        table_end_match = _table_end_re.search(temp)
        table_end = table_end_match.start() if table_end_match else len(temp)
        table = temp[:table_end]
        if not table:
            return False
        temp = temp[table_end:].strip()
        if temp.startswith("("):
            try:
                _, op_columns, temp = parse_callable(temp)
            except IndexError:
                # An unterminated column list runs off the end of the statement.  The server is the right
                # place to report that syntax error, so keep the statement intact on the row by row path.
                return False
        else:
            op_columns = None
        # The rows can only be sent as a bulk insert if VALUES immediately follows the target and its column
        # list.  Anything else, such as an INSERT SELECT or an INSERT with a SETTINGS clause, is not a simple
        # VALUES insert and must keep the original statement intact on the row by row path.
        if not temp.upper().startswith("VALUES"):
            return False
        if not isinstance(data, Sequence) or len(data) == 0:
            return False
        first_row = data[0]
        col_names: list[str] | str
        data_values: Sequence[Sequence[Any]]
        if isinstance(first_row, Mapping):
            col_names = [str(k) for k in first_row.keys()]
            if op_columns and {unescape_identifier(str(x)) for x in op_columns} != set(col_names):
                return False  # Data sent in doesn't match the columns in the insert statement
            data_values = [list(row.values()) for row in data]
        elif isinstance(first_row, Sequence) and not isinstance(first_row, (str, bytes)):
            # PEP 249 also allows rows as sequences; take column names from the
            # insert statement if present, otherwise insert into all columns
            col_names = [unescape_identifier(str(x)) for x in op_columns] if op_columns else "*"
            data_values = data
        else:
            return False
        insert_summary = self.client.insert(table, data_values, col_names, settings=settings)
        self.data = []
        self._rowcount = insert_summary.written_rows
        self._ix = 0
        self._summary.append(insert_summary.summary)
        return True

    def executemany(self, operation: str, parameters: Any, settings: dict[str, Any] | None = None) -> None:
        self.names = []
        self.types = []
        if not parameters or self._try_bulk_insert(operation, parameters, settings):
            return
        self.data = []
        try:
            for param_row in parameters:
                query_result = self.client.query(operation, param_row, settings=settings)
                self.data.extend(query_result.result_set)
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
        self._rowcount = len(self.data)

        # Need to reset cursor _ix after performing an execute
        self._ix = 0

    def fetchall(self) -> Sequence:
        self.check_valid()
        data = cast(Sequence, self.data)
        ret = data[self._ix :]
        self._ix = self._rowcount
        return ret

    def fetchone(self) -> Any:
        self.check_valid()
        if self._ix >= self._rowcount:
            return None
        data = cast(Sequence, self.data)
        val = data[self._ix]
        self._ix += 1
        return val

    def fetchmany(self, size: int = -1) -> Sequence:
        self.check_valid()
        data = cast(Sequence, self.data)

        if size < 0:
            # Fetch all remaining rows
            size = self._rowcount - self._ix
        elif size == 0:
            # Return empty list for size=0
            return []

        end = min(self._ix + size, self._rowcount)
        ret = data[self._ix : end]
        self._ix = end
        return ret

    def nextset(self) -> None:
        raise NotImplementedError

    def callproc(self, *args, **kwargs) -> None:
        raise NotImplementedError
