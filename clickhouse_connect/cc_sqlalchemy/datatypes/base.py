import logging
from typing import Dict, Type

from sqlalchemy.exc import CompileError

from clickhouse_connect.datatypes.base import ClickHouseType, TypeDef, EMPTY_TYPE_DEF
from clickhouse_connect.datatypes.registry import parse_name, type_map
from clickhouse_connect.driver.query import format_query_value

logger = logging.getLogger(__name__)


class ChSqlaType:
    """
    A SQLAlchemy TypeEngine that wraps a ClickHouseType.  We don't extend TypeEngine directly, instead all concrete
    subclasses will inherit from TypeEngine.
    """
    ch_type: ClickHouseType = None
    generic_type: None
    _ch_type_cls = None
    _instance = None
    _instance_cache: Dict[TypeDef, 'ChSqlaType'] = None

    def __init_subclass__(cls):
        """
        Registers ChSqla type in the type map and sets the underlying ClickHouseType class to use to initialize
        ChSqlaType instances
        """
        base = cls.__name__
        if not cls._ch_type_cls:
            try:
                cls._ch_type_cls = type_map[base]
            except KeyError:
                logger.warning('Attempted to register SQLAlchemy type without corresponding ClickHouse Type')
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
        return cls._instance_cache.setdefault(type_def, cls(type_def=type_def))

    def __init__(self, type_def: TypeDef = EMPTY_TYPE_DEF):
        """
        Basic constructor that does nothing but set the wrapped ClickHouseType.  It is overridden in some cases
        to add specific SqlAlchemy behavior when constructing subclasses "by hand", in which case the type_def
        parameter is normally set to None and other keyword parameters used for construction
        :param type_def: TypeDef tuple used to build the underlying ClickHouseType.  This is normally populated by the
        parse_name function
        """
        self.type_def = type_def
        self.ch_type = self._ch_type_cls.build(type_def)

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

    @staticmethod
    def result_processor():
        """
        Override for the SqlAlchemy TypeEngine result_processor method, which is used to convert row values to the
        correct Python type.  The core driver handles this automatically, so we always return None.
        """
        return None

    @staticmethod
    def _cached_result_processor(*_):
        """
        Override for the SqlAlchemy TypeEngine _cached_result_processor method to prevent weird behavior
        when SQLAlchemy tries to cache.
        """
        return None

    @staticmethod
    def _cached_literal_processor(*_):
        """
        Override for the SqlAlchemy TypeEngine _cached_literal_processor. We delegate to the driver format_query_value
        method and should be able to ignore literal_processor definitions in the dialect, which are verbose and
        confusing.
        """
        return format_query_value

    def _compiler_dispatch(self, _visitor, **_):
        """
        Override for the SqlAlchemy TypeEngine _compiler_dispatch method to sidestep unnecessary layers and complexity
        when generating the type name.  The underlying ClickHouseType generates the correct name
        :return: Name generated by the underlying driver.
        """
        return self.name


class CaseInsensitiveDict(dict):
    def __setitem__(self, key, value):
        super().__setitem__(key.lower(), value)

    def __getitem__(self, item):
        return super().__getitem__(item.lower())


sqla_type_map: CaseInsensitiveDict[str, Type[ChSqlaType]] = CaseInsensitiveDict()
schema_types = []


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
        err_str = f'Unrecognized ClickHouse type base: {base} name: {name}'
        logger.error(err_str)
        raise CompileError(err_str) from KeyError
    return type_cls.build(type_def)
