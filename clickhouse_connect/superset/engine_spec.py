import logging
import re

from datetime import datetime
from typing import Any, Dict, List, Optional, Type, TYPE_CHECKING, Tuple

from sqlalchemy.sql.type_api import TypeEngine
from superset.db_engine_specs.base import BaseEngineSpec, ColumnTypeMapping
from superset.db_engine_specs.exceptions import SupersetDBAPIDatabaseError
from superset.utils import core as utils
from superset.utils.core import GenericDataType

from clickhouse_connect import driver_name
from clickhouse_connect.chtypes import registry

if TYPE_CHECKING:
    from superset.models.core import Database

logger = logging.getLogger(__name__)


def _get_sqla_type(column_type):
    ch_type = registry.get_from_name(column_type)
    return ch_type.get_sqla_type()


class ClickHouseEngineSpec(BaseEngineSpec):
    engine = driver_name
    engine_name = "ClickHouse Connect"

    time_secondary_columns = True
    time_groupby_inline = True
    _function_names = None

    _time_grain_expressions = {
        None: "{col}",
        "PT1M": "toStartOfMinute(toDateTime({col}))",
        "PT5M": "toStartOfFiveMinutes(toDateTime({col}))",
        "PT10M": "toStartOfTenMinutes(toDateTime({col}))",
        "PT15M": "toStartOfFifteenMinutes(toDateTime({col}))",
        "PT30M": "toDateTime(intDiv(toUInt32(toDateTime({col})), 1800)*1800)",
        "PT1H": "toStartOfHour(toDateTime({col}))",
        "P1D": "toStartOfDay(toDateTime({col}))",
        "P1W": "toMonday(toDateTime({col}))",
        "P1M": "toStartOfMonth(toDateTime({col}))",
        "P3M": "toStartOfQuarter(toDateTime({col}))",
        "P1Y": "toStartOfYear(toDateTime({col}))",
    }

    column_type_mappings: Tuple[ColumnTypeMapping, ...] = (
        (re.compile(r'^(lowcardinality\()?(nullable\()?[u]?(float|int)[\d]*(\)|$)', re.IGNORECASE),
         _get_sqla_type, GenericDataType.NUMERIC),
        (re.compile(r'^(lowcardinality\()?(nullable\()?[fixed]string(\)|$)', re.IGNORECASE),
         _get_sqla_type, GenericDataType.STRING),
        (re.compile(r'^(lowcardinality\()?(nullable\()?date(time)?(\)|$)', re.IGNORECASE),
         _get_sqla_type, GenericDataType.TEMPORAL),
        (re.compile(r'^(lowcardinality\()?(nullable\()?(enum|array)'), re.IGNORECASE,
         _get_sqla_type, GenericDataType.STRING),
    )

    @classmethod
    def epoch_to_dttm(cls) -> str:
        return '{col}'

    @classmethod
    def get_dbapi_exception_mapping(cls) -> Dict[Type[Exception], Type[Exception]]:
        return {}

    @classmethod
    def get_dbapi_mapped_exception(cls, exception: Exception) -> Exception:
        new_exception = cls.get_dbapi_exception_mapping().get(type(exception))
        if new_exception == SupersetDBAPIDatabaseError:
            return SupersetDBAPIDatabaseError("Connection failed")
        if not new_exception:
            return exception
        return new_exception(str(exception))

    @classmethod
    def convert_dttm(cls, target_type: str, dttm: datetime, db_extra: Optional[Dict[str, Any]] = None)\
            -> Optional[str]:
        tt = target_type.upper()
        if tt == utils.TemporalType.DATE:
            return f"toDate('{dttm.date().isoformat()}')"
        if tt == utils.TemporalType.DATETIME:
            return f"""toDateTime('{dttm.isoformat(sep=" ", timespec="seconds")}')"""
        return None

    @classmethod
    def get_function_names(cls, database: 'Database') -> List[str]:
        if cls._function_names:
            return cls._function_names
        try:
            names = database.get_df('SELECT name FROM system.functions')['name'].tolist()
            cls._function_names = names
            return names
        except Exception as ex:  # pylint: disable=broad-except
            logger.error('Error retrieving system.functions', str(ex), exc_info=True)
            return []

    @classmethod
    def old_get_sqla_column_type(cls, column_type: Optional[str], *args, **kwargs)\
            -> Optional[Tuple[TypeEngine, GenericDataType]]:
        if column_type is None:
            return None
        ch_type = registry.get_from_name(column_type)
        return ch_type.get_sqla_type(), ch_type.gen_type

    @classmethod
    def column_datatype_to_string(cls, sqla_column_type: TypeEngine, *args):
        return sqla_column_type.compile()
