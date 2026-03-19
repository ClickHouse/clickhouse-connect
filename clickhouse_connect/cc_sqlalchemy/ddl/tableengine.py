import logging
from typing import Any, Type, Sequence, Optional, Dict, Union

from sqlalchemy.exc import ArgumentError, SQLAlchemyError
from sqlalchemy.sql.elements import TextClause
from sqlalchemy.sql.base import SchemaEventTarget
from sqlalchemy.sql.visitors import Visitable

from clickhouse_connect.driver.parser import parse_callable

logger = logging.getLogger(__name__)

engine_map: Dict[str, Type['TableEngine']] = {}
EngineExpr = Union[str, TextClause]
EngineParam = Optional[Union[EngineExpr, Sequence[EngineExpr]]]
ENGINE_CLAUSES = ("ORDER BY", "PARTITION BY", "PRIMARY KEY", "SAMPLE BY", "TTL", "SETTINGS")


def _render_engine_expr(value: EngineExpr) -> str:
    if isinstance(value, TextClause):
        return value.text
    return value


def tuple_expr(expr_name, value: EngineParam):
    """
    Create a table parameter with a tuple or list correctly formatted
    :param expr_name: parameter
    :param value: string or tuple of strings to format
    :return: formatted parameter string
    """
    if value is None:
        return ''
    v = f'{expr_name.strip()}'
    if isinstance(value, (tuple, list)):
        return f" {v} ({','.join(_render_engine_expr(item) for item in value)})"
    return f"{v} {_render_engine_expr(value)}"


def repr_engine_value(value: Any) -> str:
    if isinstance(value, str):
        return repr(value)
    if isinstance(value, TextClause):
        return f"sa.text({value.text!r})"
    if isinstance(value, tuple):
        items = ", ".join(repr_engine_value(item) for item in value)
        if len(value) == 1:
            items += ","
        return f"({items})"
    if isinstance(value, list):
        return f"[{', '.join(repr_engine_value(item) for item in value)}]"
    return repr(value)


class TableEngine(SchemaEventTarget, Visitable):
    """
    SqlAlchemy Schema element to support ClickHouse table engines.  At the moment provides no real
    functionality other than the CREATE TABLE argument string
    """
    arg_names = ()
    quoted_args = set()
    optional_args = set()
    eng_params = ()

    def __init_subclass__(cls, **kwargs):
        engine_map[cls.__name__] = cls

    def __init__(self, kwargs):
        # pylint: disable=no-value-for-parameter
        Visitable.__init__(self)
        self.name = self.__class__.__name__
        te_name = f'{self.name} Table Engine'
        self._orig_kwargs = kwargs.copy()
        engine_args = []
        for arg_name in self.arg_names:
            v = kwargs.pop(arg_name, None)
            if v is None:
                if arg_name in self.optional_args:
                    continue
                raise ValueError(f'Required engine parameter {arg_name} not provided for {te_name}')
            if arg_name in self.quoted_args:
                engine_args.append(f"'{v}'")
            else:
                engine_args.append(v)
        if engine_args:
            self.arg_str = f'({", ".join(engine_args)})'
        params = []
        for param_name in self.eng_params:
            v = kwargs.pop(param_name, None)
            if v is not None:
                params.append(tuple_expr(param_name.upper().replace('_', ' '), v))
        settings = kwargs.pop("settings", None)
        self.settings = settings or {}

        self.full_engine = 'Engine ' + self.name
        if engine_args:
            self.full_engine += f'({", ".join(engine_args)})'
        if params:
            self.full_engine += ' ' + ' '.join(params)
        if self.settings:
            settings_expr = ", ".join(f"{k} = {v}" for k, v in self.settings.items())
            self.full_engine += f" SETTINGS {settings_expr}"

    def __repr__(self):
        """Produce Python code representation of the engine for Alembic autogeneration."""
        args = []
        for k, v in self._orig_kwargs.items():
            if k in {"self", "__class__"}:
                continue
            if v is None:
                continue
            args.append(f"{k}={repr_engine_value(v)}")
        return f"{self.name}({', '.join(args)})"

    def compile(self):
        return self.full_engine

    def check_primary_keys(self, primary_keys: Sequence):
        raise SQLAlchemyError(f'Table Engine {self.name} does not support primary keys')

    def _set_parent(self, parent, **_kwargs):
        parent.engine = self
        if parent.kwargs.get("clickhouse_engine") is None and parent.kwargs.get("clickhousedb_engine") is None:
            parent.kwargs["clickhouse_engine"] = self


class Memory(TableEngine):
    pass


class Log(TableEngine):
    pass


class StripeLog(TableEngine):
    pass


class TinyLog(TableEngine):
    pass


class Null(TableEngine):
    pass


class Set(TableEngine):
    pass


class Dictionary(TableEngine):
    arg_names = ['dictionary']

    # pylint: disable=unused-argument
    def __init__(self, dictionary: str = None):
        super().__init__(locals())


class Merge(TableEngine):
    arg_names = ['db_name, tables_regexp']

    # pylint: disable=unused-argument
    def __init__(self, db_name: str = None, tables_regexp: str = None):
        super().__init__(locals())


class File(TableEngine):
    arg_names = ['fmt']

    # pylint: disable=unused-argument
    def __init__(self, fmt: str = None):
        super().__init__(locals())


class Distributed(TableEngine):
    arg_names = ['cluster', 'database', 'table', 'sharding_key', 'policy_name']
    optional_args = {'sharding_key', 'policy_name'}

    # pylint: disable=unused-argument
    def __init__(self, cluster: str = None, database: str = None, table=None,
                 sharding_key: str = None, policy_name: str = None):
        super().__init__(locals())


class MergeTree(TableEngine):
    eng_params = ["order_by", "partition_by", "primary_key", "sample_by", "ttl"]

    # pylint: disable=unused-argument
    def __init__(self, order_by: EngineParam = None, primary_key: EngineParam = None,
                 partition_by: EngineParam = None, sample_by: EngineParam = None,
                 ttl: Optional[EngineExpr] = None, settings: Optional[Dict[str, Any]] = None):
        if order_by is None and primary_key is None:
            raise ArgumentError(None, 'Either PRIMARY KEY or ORDER BY must be specified')
        super().__init__(locals())


class SharedMergeTree(MergeTree):
    pass


class SummingMergeTree(MergeTree):
    pass


class AggregatingMergeTree(MergeTree):
    pass


class ReplacingMergeTree(TableEngine):
    arg_names = ['version', 'is_deleted']
    optional_args = set(arg_names)
    eng_params = MergeTree.eng_params

    # pylint: disable=unused-argument
    def __init__(self, ver: str = None, version: str = None,
                 is_deleted: str = None, order_by: EngineParam = None,
                 primary_key: EngineParam = None, partition_by: EngineParam = None,
                 sample_by: EngineParam = None, ttl: Optional[EngineExpr] = None,
                 settings: Optional[Dict[str, Any]] = None):
        if order_by is None and primary_key is None:
            raise ArgumentError(None, 'Either PRIMARY KEY or ORDER BY must be specified')
        kwargs = {
            'version': version or ver,
            'is_deleted': is_deleted,
            'order_by': order_by,
            'primary_key': primary_key,
            'partition_by': partition_by,
            'sample_by': sample_by,
            'ttl': ttl,
            'settings': settings,
        }
        super().__init__(kwargs)


class CollapsingMergeTree(TableEngine):
    arg_names = ['sign']
    eng_params = MergeTree.eng_params

    # pylint: disable=unused-argument
    def __init__(self, sign: str = None, order_by: EngineParam = None,
                 primary_key: EngineParam = None, partition_by: EngineParam = None,
                 sample_by: EngineParam = None, ttl: Optional[EngineExpr] = None,
                 settings: Optional[Dict[str, Any]] = None):
        if order_by is None and primary_key is None:
            raise ArgumentError(None, 'Either PRIMARY KEY or ORDER BY must be specified')
        super().__init__(locals())


class VersionedCollapsingMergeTree(TableEngine):
    arg_names = ['sign', 'version']
    eng_params = MergeTree.eng_params

    # pylint: disable=unused-argument
    def __init__(self, sign: str = None, version: str = None,
                 order_by: EngineParam = None, primary_key: EngineParam = None,
                 partition_by: EngineParam = None, sample_by: EngineParam = None,
                 ttl: Optional[EngineExpr] = None, settings: Optional[Dict[str, Any]] = None):
        if order_by is None and primary_key is None:
            raise ArgumentError(None, 'Either PRIMARY KEY or ORDER BY must be specified')
        super().__init__(locals())


class GraphiteMergeTree(TableEngine):
    arg_names = ['config_section']
    eng_params = MergeTree.eng_params

    # pylint: disable=unused-argument
    def __init__(self, config_section: str = None, version: str = None,
                 order_by: EngineParam = None, primary_key: EngineParam = None,
                 partition_by: EngineParam = None, sample_by: EngineParam = None,
                 ttl: Optional[EngineExpr] = None, settings: Optional[Dict[str, Any]] = None):
        if order_by is None and primary_key is None:
            raise ArgumentError(None, 'Either PRIMARY KEY or ORDER BY must be specified')
        super().__init__(locals())


class ReplicatedMergeTree(TableEngine):
    arg_names = ['zk_path', 'replica']
    quoted_args = set(arg_names)
    optional_args = quoted_args
    eng_params = MergeTree.eng_params

    # pylint: disable=unused-argument
    def __init__(self, order_by: EngineParam = None, primary_key: EngineParam = None,
                 partition_by: EngineParam = None, sample_by: EngineParam = None,
                 zk_path: str = None, replica: str = None,
                 ttl: Optional[EngineExpr] = None, settings: Optional[Dict[str, Any]] = None):
        if order_by is None and primary_key is None:
            raise ArgumentError(None, 'Either PRIMARY KEY or ORDER BY must be specified')
        super().__init__(locals())


class ReplicatedAggregatingMergeTree(ReplicatedMergeTree):
    pass


class ReplicatedSummingMergeTree(ReplicatedMergeTree):
    pass


class SharedReplacingMergeTree(ReplacingMergeTree):
    pass


class SharedAggregatingMergeTree(AggregatingMergeTree):
    pass


class SharedSummingMergeTree(SummingMergeTree):
    pass


class SharedVersionedCollapsingMergeTree(VersionedCollapsingMergeTree):
    pass


class SharedGraphiteMergeTree(GraphiteMergeTree):
    pass


def _walk_sql(sql: str, start: int = 0):
    """Yield unquoted characters while tracking nested parenthesis depth."""
    depth = 0
    quote_char = None
    escape = False
    for i in range(start, len(sql)):
        char = sql[i]
        if escape:
            escape = False
            continue
        if quote_char:
            if char == "\\" and quote_char == "'":
                escape = True
            elif char == quote_char:
                quote_char = None
            continue
        if char in {"'", "\"", "`"}:
            quote_char = char
            continue
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
        yield i, char, depth


def _split_top_level_sql(sql: str, delimiter: str = ",") -> list[str]:
    parts = []
    part_start = 0
    for i, char, depth in _walk_sql(sql):
        if char == delimiter and depth == 0:
            part = sql[part_start:i].strip()
            if part:
                parts.append(part)
            part_start = i + 1
    tail = sql[part_start:].strip()
    if tail:
        parts.append(tail)
    return parts


def _strip_string_quotes(value: Any) -> Any:
    if isinstance(value, str) and len(value) > 1 and value[0] == value[-1] == "'":
        return value[1:-1]
    return value


def _parse_positional_engine_args(full_engine: str, engine_cls: Type['TableEngine']) -> Dict[str, Any]:
    if not engine_cls.arg_names:
        return {}
    _, arg_values, _ = parse_callable(full_engine)
    return {
        arg_name: _strip_string_quotes(arg_value)
        for arg_name, arg_value in zip(engine_cls.arg_names, arg_values)
        if arg_value != ""
    }


def _find_clause_markers(sql: str) -> list[tuple[int, str]]:
    markers = []
    upper_sql = sql.upper()
    for i, _char, depth in _walk_sql(sql):
        if depth != 0 or (i > 0 and not sql[i - 1].isspace()):
            continue
        for clause in ENGINE_CLAUSES:
            if upper_sql.startswith(clause, i):
                markers.append((i, clause))
                break
    return markers


def _parse_settings_clause(raw_settings: str) -> Dict[str, Any]:
    settings: Dict[str, Any] = {}
    for pair in _split_top_level_sql(raw_settings):
        if "=" not in pair:
            continue
        key, value = pair.split("=", 1)
        key = key.strip()
        value = value.strip()
        try:
            settings[key] = int(value)
        except ValueError:
            settings[key] = value
    return settings


def _parse_keyword_engine_clauses(clause_sql: str) -> Dict[str, Any]:
    params: Dict[str, Any] = {}
    markers = _find_clause_markers(clause_sql)
    for index, (start, clause) in enumerate(markers):
        value_start = start + len(clause)
        value_end = markers[index + 1][0] if index + 1 < len(markers) else len(clause_sql)
        value = clause_sql[value_start:value_end].strip()
        if not value:
            continue
        if clause == "SETTINGS":
            settings = _parse_settings_clause(value)
            if settings:
                params["settings"] = settings
            continue
        params[clause.lower().replace(" ", "_")] = value
    return params


def _parse_engine_params(full_engine: str, engine_cls: Type['TableEngine']) -> Dict[str, Any]:
    """Extract engine parameters from a full_engine expression for repr().

    Parses both positional constructor args (e.g. the ``version`` in
    ``ReplacingMergeTree(version)``) and keyword clauses (``ORDER BY``,
    ``PARTITION BY``, etc.) so that reflected engines round-trip through
    ``repr()`` correctly.
    """
    params = _parse_positional_engine_args(full_engine, engine_cls)
    _, _, clause_sql = parse_callable(full_engine)
    params.update(_parse_keyword_engine_clauses(clause_sql))
    return params


# pylint: disable=protected-access
def build_engine(full_engine: str) -> Optional[TableEngine]:
    """
    Factory function to create TableEngine class from ClickHouse full_engine expression
    :param full_engine
    :return: TableEngine DDL element
    """
    if not full_engine:
        return None
    name, _, _ = parse_callable(full_engine)
    try:
        engine_cls = engine_map[name]
    except KeyError:
        if not name.startswith('System'):
            logger.warning('Engine %s not found', name)
        return None
    engine = engine_cls.__new__(engine_cls)
    engine.name = name
    engine.full_engine = full_engine
    engine._orig_kwargs = _parse_engine_params(full_engine, engine_cls)
    engine.settings = {}
    return engine
