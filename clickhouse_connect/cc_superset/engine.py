import logging

from datetime import datetime
from typing import Dict, List, Optional, Type

from flask import current_app
from flask_babel import gettext as __
from marshmallow import Schema, fields
from marshmallow.validate import Range
from sqlalchemy.engine.url import URL, make_url
from sqlalchemy.sql.type_api import TypeEngine
from superset.db_engine_specs.base import BaseEngineSpec, BasicParametersType, BasicParametersMixin
from superset.db_engine_specs.exceptions import SupersetDBAPIDatabaseError
from superset.errors import SupersetError, SupersetErrorType, ErrorLevel
from superset.utils.core import ColumnSpec, GenericDataType
from superset.utils.network import is_hostname_valid, is_port_open
from superset.models.core import Database

from clickhouse_connect import driver_name
from clickhouse_connect.common import set_setting
from clickhouse_connect.driver import default_port
from clickhouse_connect.cc_sqlalchemy.datatypes.base import sqla_type_from_name
from clickhouse_connect.cc_superset.datatypes import configure_types
from clickhouse_connect.driver.exceptions import ClickHouseError

logger = logging.getLogger(__name__)

configure_types()
set_setting('product_name', f"superset/{current_app.config.get('VERSION_STRING', 'dev')}")


class ClickHouseParametersSchema(Schema):
    username = fields.String(allow_none=True, description=__('Username'))
    password = fields.String(allow_none=True, description=__('Password'))
    host = fields.String(required=True, description=__('Hostname or IP address'))
    port = fields.Integer(allow_none=True, description=__('Database port'), validate=Range(min=0, max=65535), )
    database = fields.String(allow_none=True, description=__('Database name'))
    encryption = fields.Boolean(default=True, description=__('Use an encrypted connection to the database'))
    query = fields.Dict(keys=fields.Str(), values=fields.Raw(), description=__('Additional parameters'))


class ClickHouseEngineSpec(BaseEngineSpec, BasicParametersMixin):
    """
    See :py:class:`superset.db_engine_specs.base.BaseEngineSpec`
    """

    engine = driver_name
    engine_name = 'ClickHouse Connect'

    default_driver = 'connect'
    time_secondary_columns = True
    time_groupby_inline = True
    _function_names = []

    _time_grain_expressions = {
        None: '{col}',
        'PT1M': 'toStartOfMinute(toDateTime({col}))',
        'PT5M': 'toStartOfFiveMinutes(toDateTime({col}))',
        'PT10M': 'toStartOfTenMinutes(toDateTime({col}))',
        'PT15M': 'toStartOfFifteenMinutes(toDateTime({col}))',
        'PT30M': 'toDateTime(intDiv(toUInt32(toDateTime({col})), 1800)*1800)',
        'PT1H': 'toStartOfHour(toDateTime({col}))',
        'P1D': 'toStartOfDay(toDateTime({col}))',
        'P1W': 'toMonday(toDateTime({col}))',
        'P1M': 'toStartOfMonth(toDateTime({col}))',
        'P3M': 'toStartOfQuarter(toDateTime({col}))',
        'P1Y': 'toStartOfYear(toDateTime({col}))',
    }

    sqlalchemy_uri_placeholder = 'clickhousedb://user:password@host[:port][/dbname][?secure=value&=value...]'
    parameters_schema = ClickHouseParametersSchema()
    encryption_parameters = {'secure': 'true'}

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
            return SupersetDBAPIDatabaseError('Connection failed')
        if not new_exception:
            return exception
        return new_exception(str(exception))

    @classmethod
    def convert_dttm(cls, target_type: str, dttm: datetime, *_args, **_kwargs) -> Optional[str]:
        if target_type.upper() == 'DATE':
            return f"'{dttm.date().isoformat()}'"
        if target_type.upper() == 'DATETIME':
            return f"""'{dttm.isoformat(sep=" ", timespec="seconds")}'"""
        return None

    @classmethod
    def get_function_names(cls, database: Database) -> List[str]:
        if cls._function_names:
            return cls._function_names
        try:
            names = database.get_df(
                'SELECT name FROM system.functions UNION ALL ' +
                'SELECT name FROM system.table_functions LIMIT 10000')['name'].tolist()
            cls._function_names = names
            return names
        except ClickHouseError:
            logger.exception('Error retrieving system.functions')
            return []

    @classmethod
    def get_datatype(cls, type_code: str) -> str:
        return type_code

    @classmethod
    def get_column_spec(cls, native_type: Optional[str], *_args, **_kwargs) -> Optional[ColumnSpec]:
        if not native_type:
            return None
        sqla_type = sqla_type_from_name(native_type)
        generic_type = sqla_type.generic_type
        return ColumnSpec(sqla_type, generic_type, generic_type == GenericDataType.TEMPORAL)

    @classmethod
    def get_sqla_column_type(cls, column_type: Optional[str], *_args, **_kwargs):
        if column_type is None:
            return None
        sqla_type = sqla_type_from_name(column_type)
        return sqla_type, sqla_type.generic_type

    @classmethod
    def column_datatype_to_string(cls, sqla_column_type: TypeEngine, *_args):
        return sqla_column_type.compile()

    @classmethod
    def build_sqlalchemy_uri(cls, parameters: BasicParametersType, *_args):
        url_params = parameters.copy()
        if url_params.get('encryption'):
            query = parameters.get('query', {}).copy()
            query.update(cls.encryption_parameters)
            url_params['query'] = query
        if not url_params.get('database'):
            url_params['database'] = '__default__'
        url_params.pop('encryption', None)
        return str(URL(f'{cls.engine}+{cls.default_driver}', **url_params))

    @classmethod
    def get_parameters_from_uri(cls, uri: str, *_args, **_kwargs) -> BasicParametersType:
        url = make_url(uri)
        query = url.query
        if 'secure' in query:
            encryption = url.query.get('secure') == 'true'
            query.pop('secure')
        else:
            encryption = False
        return BasicParametersType(
            username=url.username,
            password=url.password,
            host=url.host,
            port=url.port,
            database=None if url.database == '__default__' else url.database,
            query=dict(query),
            encryption=encryption)

    @classmethod
    # pylint: disable=arguments-renamed
    def validate_parameters(cls, properties) -> List[SupersetError]:
        # The newest versions of superset send a "properties" object with a parameters key, instead of just
        # the parameters, so we hack to be compatible
        parameters = properties.get('parameters', properties)
        host = parameters.get('host', None)
        if not host:
            return [SupersetError(
                'Hostname is required',
                SupersetErrorType.CONNECTION_MISSING_PARAMETERS_ERROR,
                ErrorLevel.WARNING,
                {'missing': ['host']},
            )]
        if not is_hostname_valid(host):
            return [SupersetError(
                "The hostname provided can't be resolved.",
                SupersetErrorType.CONNECTION_INVALID_HOSTNAME_ERROR,
                ErrorLevel.ERROR,
                {'invalid': ['host']},
            )]
        port = parameters.get('port')
        if port is None:
            port = default_port('http', parameters.get('encryption', False))
        try:
            port = int(port)
        except (ValueError, TypeError):
            port = -1
        if port <= 0 or port >= 65535:
            return [SupersetError(
                'Port must be a valid integer between 0 and 65535 (inclusive).',
                SupersetErrorType.CONNECTION_INVALID_PORT_ERROR,
                ErrorLevel.ERROR,
                {'invalid': ['port']})]
        if not is_port_open(host, port):
            return [SupersetError(
                'The port is closed.',
                SupersetErrorType.CONNECTION_PORT_CLOSED_ERROR,
                ErrorLevel.ERROR,
                {'invalid': ['port']})]
        return []
