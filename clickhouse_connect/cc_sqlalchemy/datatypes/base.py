import logging
from collections.abc import Callable, Mapping
from datetime import datetime, timezone, tzinfo
from typing import Any, TypeAlias

from sqlalchemy.engine.interfaces import Dialect
from sqlalchemy.exc import CompileError
from sqlalchemy.sql.sqltypes import ARRAY, TypeDecorator, TypeEngine

from clickhouse_connect import common
from clickhouse_connect.datatypes.base import EMPTY_TYPE_DEF, ClickHouseType, TypeDef
from clickhouse_connect.datatypes.container import Array, Map, Tuple
from clickhouse_connect.datatypes.registry import parse_name, type_map
from clickhouse_connect.datatypes.temporal import DateTime64
from clickhouse_connect.driver import tzutil
from clickhouse_connect.driver.binding import (
    DT64Param,
    _datetime_is_aware,
    _legacy_naive_datetime_binding,
    _named_tuple_values,
    str_query_value,
)

logger = logging.getLogger(__name__)
_QueryValueSignature: TypeAlias = (
    bool | tuple[str, tuple["_QueryValueSignature", ...]] | tuple[str, tuple["_QueryValueSignature", ...], tuple[str, ...]]
)


class _DateTime64QueryParam(DT64Param):
    def format(self, tz: tzinfo | None, top_level: bool) -> str:
        # Preserve SQL-text datetime timezone rules, including legacy naive UTC values.
        if self.tz is not None:
            tz = self.tz
        value = self.value
        if _legacy_naive_datetime_binding():
            if value.tzinfo is not None or not tzutil.is_utc_timezone(tz):
                value = value.astimezone(tz)
        elif _datetime_is_aware(value):
            value = value.astimezone(tz)
        return self._render(value, top_level)


def _non_temporal_query_shape(ch_type: ClickHouseType) -> _QueryValueSignature:
    if isinstance(ch_type, Array):
        return ("array_shape", ())
    if isinstance(ch_type, Tuple):
        return ("tuple_shape", tuple(_non_temporal_query_shape(element) for element in ch_type.element_types))
    return False


def _query_value_signature(ch_type: ClickHouseType | None) -> _QueryValueSignature:
    """Identify DateTime64 leaves and named Tuple fields for SQL-text values."""
    if isinstance(ch_type, DateTime64):
        return True
    if isinstance(ch_type, Array):
        child = _query_value_signature(ch_type.element_type)
        return ("array", (child,)) if child else False
    if isinstance(ch_type, Tuple):
        children = tuple(_query_value_signature(element) for element in ch_type.element_types)
        if ch_type.element_names:
            return (
                "named_tuple",
                tuple(child or _non_temporal_query_shape(element) for child, element in zip(children, ch_type.element_types)),
                ch_type.element_names,
            )
        if any(children):
            return ("tuple", tuple(child or _non_temporal_query_shape(element) for child, element in zip(children, ch_type.element_types)))
    if isinstance(ch_type, Map):
        children = (_query_value_signature(ch_type.key_type), _query_value_signature(ch_type.value_type))
        if any(children):
            return ("map", children)
    return False


def _unwrap_dialect_type(sqla_type: TypeEngine[Any], dialect: Dialect) -> TypeEngine[Any]:
    """Resolve decorators without allowing endless dialect adaptation."""
    effective_type = sqla_type.dialect_impl(dialect)
    for _ in range(1024):
        if not isinstance(effective_type, TypeDecorator):
            return effective_type
        effective_type = effective_type.type_engine(dialect).dialect_impl(dialect)
    if isinstance(effective_type, TypeDecorator):
        raise CompileError(
            "TypeDecorator adaptation did not resolve after 1024 steps. Check the dialect's colspecs and custom type hooks for cycles."
        )
    return effective_type


def _query_bind_signature(sqla_type: TypeEngine[Any], dialect: Dialect) -> _QueryValueSignature:
    effective_type = _unwrap_dialect_type(sqla_type, dialect)
    if isinstance(effective_type, ChSqlaType):
        return _query_value_signature(effective_type.ch_type)
    if isinstance(effective_type, ARRAY):
        signature = _query_bind_signature(effective_type.item_type, dialect)
        if signature:
            if effective_type.dimensions is None:
                return ("array_any", (signature,))
            for _ in range(effective_type.dimensions):
                signature = ("array", (signature,))
        return signature
    return False


def _positional_signature(signature: _QueryValueSignature) -> _QueryValueSignature:
    """Drop named Tuple field names so compatible positional shapes compare equal."""
    if isinstance(signature, bool):
        return signature
    kind, children = signature[0], signature[1]
    return ("tuple" if kind == "named_tuple" else kind, tuple(_positional_signature(child) for child in children))


def _query_value_matches_shape(signature: _QueryValueSignature, value: Any) -> bool:
    if value is None:
        return True
    if isinstance(signature, bool):
        return not isinstance(value, (list, tuple))
    kind, children = signature[0], signature[1]
    if kind == "map":
        return isinstance(value, dict)
    if kind == "named_tuple" and isinstance(value, Mapping):
        return True
    if kind in ("tuple", "named_tuple", "tuple_shape"):
        return (
            isinstance(value, tuple)
            and len(value) == len(children)
            and all(_query_value_matches_shape(child, item) for child, item in zip(children, value))
        )
    return isinstance(value, (list, tuple))


def _typed_query_value(signature: _QueryValueSignature, value: Any) -> Any:
    """Prepare typed DateTime64 and named Tuple values for SQL-text formatting."""
    if value is None or not signature:
        return value
    if isinstance(signature, bool):
        return _DateTime64QueryParam(value) if isinstance(value, datetime) else value
    kind, children = signature[0], signature[1]
    if kind in ("array", "array_any") and isinstance(value, (list, tuple)):
        child = children[0]
        if kind == "array_any" and value and isinstance(value[0], (list, tuple)):
            tuple_item = isinstance(child, tuple) and child[0] in ("tuple", "named_tuple") and _query_value_matches_shape(child, value[0])
            if not tuple_item:
                child = signature
        items = (_typed_query_value(child, item) for item in value)
        return tuple(items) if isinstance(value, tuple) else list(items)
    if kind == "named_tuple" and len(signature) == 3 and isinstance(value, Mapping):
        value = _named_tuple_values(signature[2], value)
    if kind in ("tuple", "named_tuple") and isinstance(value, tuple) and len(value) == len(children):
        return tuple(_typed_query_value(element, item) for element, item in zip(children, value))
    if kind == "map" and isinstance(value, dict) and common.get_setting("dict_parameter_format") == "map":
        return {_typed_query_value(children[0], key): _typed_query_value(children[1], item) for key, item in value.items()}
    return value


class ChSqlaType:
    """
    A SQLAlchemy TypeEngine that wraps a ClickHouseType.  We don't extend TypeEngine directly, instead all concrete
    subclasses will inherit from TypeEngine.
    """

    ch_type: ClickHouseType | None = None
    generic_type: None
    _ch_type_cls: type[ClickHouseType] | None = None
    _instance_cache: dict[TypeDef, "ChSqlaType"] | None = None
    _schema_name: str | None = None

    def __init_subclass__(cls):
        """
        Registers ChSqla type in the type map and sets the underlying ClickHouseType class to use to initialize
        ChSqlaType instances
        """
        base = cls.__dict__.get("_schema_name") or cls.__name__
        if not cls._ch_type_cls:
            try:
                cls._ch_type_cls = type_map[base]
            except KeyError:
                logger.warning("Attempted to register SQLAlchemy type without corresponding ClickHouse Type")
                return
        schema_types.append(base)
        sqla_type_map[base] = cls
        cls._instance_cache = {}

    @classmethod
    def build(cls, type_def: TypeDef):
        """
        Factory function for building a ChSqlaType based on the type definition
        :param type_def: -- TypeDef tuple that defines arguments for this instance
        :return: Shared instance of a configured ChSqlaType
        """
        return cls._instance_cache.setdefault(type_def, cls(type_def=type_def))  # type: ignore[union-attr]

    def __init__(self, type_def: TypeDef = EMPTY_TYPE_DEF):
        """
        Basic constructor that does nothing but set the wrapped ClickHouseType.  It is overridden in some cases
        to add specific SqlAlchemy behavior when constructing subclasses "by hand", in which case the type_def
        parameter is normally set to None and other keyword parameters used for construction
        :param type_def: TypeDef tuple used to build the underlying ClickHouseType.  This is normally populated by the
        parse_name function
        """
        self.type_def = type_def
        self.ch_type = self._ch_type_cls.build(type_def)  # type: ignore[union-attr]

    @property
    def name(self):
        return self.ch_type.name

    @name.setter
    def name(self, name):  # Keep SQLAlchemy from overriding our ClickHouse name
        pass

    @property
    def nullable(self):
        return self.ch_type.nullable

    @property
    def low_card(self):
        return self.ch_type.low_card

    def result_processor(self, dialect, coltype):
        """
        Override for the SqlAlchemy TypeEngine result_processor method, which is used to convert row values to the
        correct Python type.  The core driver handles this automatically, so we always return None.
        """
        return None

    def literal_processor(self, dialect: Dialect) -> Callable[[Any], str]:
        """
        Delegate SQLAlchemy literal rendering to the driver's query value formatter.
        """
        signature = _query_value_signature(self.ch_type)
        double_percents = dialect.identifier_preparer._double_percents
        if not signature and not double_percents:
            return str_query_value

        def process(value: Any) -> str:
            if signature:
                value = _typed_query_value(signature, value)
            rendered = str_query_value(value, timezone.utc)
            return rendered.replace("%", "%%") if double_percents else rendered

        return process

    def _compiler_dispatch(self, _visitor, **_):
        """
        Override for the SqlAlchemy TypeEngine _compiler_dispatch method to sidestep unnecessary layers and complexity
        when generating the type name.  The underlying ClickHouseType generates the correct name for the type
        :return: Name generated by the underlying driver.
        """
        return self.name

    def _with_collation(self, collation: str | None) -> "ChSqlaType":
        """
        SQLAlchemy 2.x compatibility: TypeEngine declares this abstract to support
        text types that can carry a collation. ClickHouse types in this dialect
        do not vary by collation, so this is a no-op that returns self.
        """
        return self


class CaseInsensitiveDict(dict):
    def __setitem__(self, key, value):
        super().__setitem__(key.lower(), value)

    def __getitem__(self, item):
        return super().__getitem__(item.lower())


sqla_type_map: dict[str, type[ChSqlaType]] = CaseInsensitiveDict()
schema_types: list[str] = []


def sqla_type_from_name(name: str) -> ChSqlaType:
    """
    Factory function to convert a ClickHouse type name to the appropriate ChSqlaType
    :param name: Name returned from ClickHouse using Native protocol or WithNames format
    :return: ChSqlaType
    """
    base, name, type_def = parse_name(name)
    try:
        type_cls = sqla_type_map[base]
    except KeyError:
        err_str = f"Unrecognized ClickHouse type base: {base} name: {name}"
        logger.error(err_str)
        raise CompileError(err_str) from KeyError
    return type_cls.build(type_def)
