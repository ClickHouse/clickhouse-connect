import logging
import json
import atexit
import re
import uuid
import http as PyHttp
from http.client import RemoteDisconnected

from typing import Optional, Dict, Any, Sequence, Union, List, Callable

from requests import Session, Response, get as req_get
from requests.exceptions import RequestException

from clickhouse_connect import common
from clickhouse_connect.datatypes import registry
from clickhouse_connect.datatypes.base import ClickHouseType
from clickhouse_connect.driver.client import Client
from clickhouse_connect.driver.common import dict_copy
from clickhouse_connect.driver.exceptions import DatabaseError, OperationalError, ProgrammingError
from clickhouse_connect.driver.httpadapter import KeepAliveAdapter
from clickhouse_connect.driver.insert import InsertContext
from clickhouse_connect.driver.native import NativeTransform
from clickhouse_connect.driver.query import QueryResult, DataResult, QueryContext, finalize_query, quote_identifier

logger = logging.getLogger(__name__)
columns_only_re = re.compile(r'LIMIT 0\s*$', re.IGNORECASE)

# Create a single HttpAdapter that will be shared by all client sessions.  This is intended to make
# the client as thread safe as possible while sharing a single connection pool.  For the same reason we
# don't call the Session.close() method from the client so the connection pool remains available
default_adapter = KeepAliveAdapter(pool_connections=4, pool_maxsize=8, max_retries=0)
atexit.register(default_adapter.close)

# Increase this number just to be safe when ClickHouse is returning progress headers
PyHttp._MAXHEADERS = 10000  # pylint: disable=protected-access


# pylint: disable=too-many-instance-attributes
class HttpClient(Client):
    valid_transport_settings = {'database', 'buffer_size', 'session_id', 'compress', 'decompress',
                                'session_timeout', 'session_check', 'query_id', 'quota_key', 'wait_end_of_query',
                                }
    optional_transport_settings = {'send_progress_in_http_headers', 'http_headers_progress_interval_ms',
                                   'enable_http_compression'}

    # pylint: disable=too-many-arguments,too-many-locals,too-many-branches,too-many-statements
    def __init__(self,
                 interface: str,
                 host: str,
                 port: int,
                 username: str,
                 password: str,
                 database: str,
                 compress: Union[bool, str] = True,
                 query_limit: int = 5000,
                 query_retries: int = 2,
                 connect_timeout: int = 10,
                 send_receive_timeout: int = 300,
                 client_name: str = 'clickhouse-connect',
                 send_progress: bool = True,
                 verify: bool = True,
                 ca_cert: str = None,
                 client_cert: str = None,
                 client_cert_key: str = None,
                 session_id: str = None,
                 settings: Optional[Dict] = None,
                 http_adapter: KeepAliveAdapter = default_adapter,
                 **kwargs):
        """
        Create an HTTP ClickHouse Connect client
        :param interface: http or https
        :param host: hostname
        :param port: host port
        :param username: ClickHouse user
        :param password: ClickHouse password
        :param database: Default database for the connection
        :param compress: Accept compressed HTTP type from server (brotli or gzip)
        :param query_limit: Default LIMIT on returned rows
        :param connect_timeout:  Timeout in seconds for the http connection
        :param send_receive_timeout: Read timeout in seconds for http connection
        :param client_name: Http user agent header value
        :param send_progress: Ask ClickHouse to send progress headers.  Used for summary and keep alive
        :param verify: Verify the server certificate in secure/https mode
        :param ca_cert: If verify is True, the file path to Certificate Authority root to validate ClickHouse server
         certificate, in .pem format.  Ignored if verify is False.  This is not necessary if the ClickHouse server
         certificate is a globally trusted root as verified by the operating system
        :param client_cert: File path to a TLS Client certificate in .pem format.  This file should contain any
          applicable intermediate certificates
        :param client_cert_key: File path to the private key for the Client Certificate.  Required if the private key
          is not included the Client Certificate key file
        :param session_id ClickHouse session id.  If not specified and the common setting 'autogenerate_session_id'
          is True, the client will generate a UUID1 session id
        :param settings Optional dictionary of ClickHouse setting values (str/value) for every connection request
        :param http_adapter Optional requests.HTTPAdapter for this client.  Useful for creating separate connection
          pools for multiple client endpoints instead the singleton pool in the default_adapter
        :param kwargs: Optional clickhouse setting values (str/value) for every connection request (deprecated)
        """
        self.url = f'{interface}://{host}:{port}'
        session = Session()
        session.stream = False
        session.max_redirects = 3
        if client_cert:
            if not username:
                raise ProgrammingError('username parameter is required for Mutual TLS authentication')
            if client_cert_key:
                session.cert = (client_cert, client_cert_key)
            else:
                session.cert = client_cert
            session.headers['X-ClickHouse-User'] = username
            session.headers['X-ClickHouse-SSL-Certificate-Auth'] = 'on'
        else:
            session.auth = (username, password if password else '') if username else None
        session.verify = False
        if interface == 'https':
            if verify and ca_cert:
                session.verify = ca_cert
            else:
                session.verify = verify

        # Remove the default session adapters, they are not used and this avoids issues with their connection pools
        session.adapters.pop('http://').close()
        session.adapters.pop('https://').close()
        session.mount(self.url, adapter=http_adapter if http_adapter else default_adapter)
        session.headers['User-Agent'] = client_name

        self.read_format = self.write_format = 'Native'
        self.column_inserts = True
        self.transform = NativeTransform()

        self.session = session
        self.connect_timeout = connect_timeout
        self.read_timeout = send_receive_timeout
        self.query_retries = query_retries
        ch_settings = dict_copy(settings, kwargs)
        if send_progress:
            ch_settings['send_progress_in_http_headers'] = '1'
            ch_settings['wait_end_of_query'] = '1'
            if self.read_timeout > 10:
                progress_interval = (self.read_timeout - 5) * 1000
            else:
                progress_interval = 120000  # Two minutes
            ch_settings['http_headers_progress_interval_ms'] = str(progress_interval)
        compression = 'gzip' if compress is True else compress
        if compression:
            session.headers['Accept-Encoding'] = compression
            ch_settings['enable_http_compression'] = '1'
        if session_id:
            ch_settings['session_id'] = session_id
        elif 'session_id' not in ch_settings and common.get_setting('autogenerate_session_id'):
            ch_settings['session_id'] = str(uuid.uuid1())
        super().__init__(database=database, query_limit=query_limit, uri=self.url, compression=compression)
        self.session.params = self._validate_settings(ch_settings)

    def client_setting(self, key, value):
        str_value = self._validate_setting(key, value, common.get_setting('invalid_setting_action') == 'send')
        if str_value is not None:
            self.session.params[key] = str_value

    def _prep_query(self, context: QueryContext):
        final_query = super()._prep_query(context)
        if context.is_insert:
            return final_query
        return f'{final_query}\n FORMAT {self.write_format}'

    def _query_with_context(self, context: QueryContext) -> QueryResult:
        headers = {'Content-Type': 'text/plain; charset=utf-8'}
        params = {'database': self.database}
        params.update(self._validate_settings(context.settings))
        if columns_only_re.search(context.uncommented_query):
            response = self._raw_request(f'{context.final_query}\n FORMAT JSON',
                                         params, headers, retries=self.query_retries)
            json_result = json.loads(response.content)
            # ClickHouse will respond with a JSON object of meta, data, and some other objects
            # We just grab the column names and column types from the metadata sub object
            names: List[str] = []
            types: List[ClickHouseType] = []
            for col in json_result['meta']:
                names.append(col['name'])
                types.append(registry.get_from_name(col['type']))
            data_result = DataResult([], tuple(names), tuple(types))
        else:
            response = self._raw_request(self._prep_query(context), params, headers, retries=self.query_retries)
            data_result = self.transform.parse_response(response.content, context)
        summary = {}
        if 'X-ClickHouse-Summary' in response.headers:
            try:
                summary = json.loads(response.headers['X-ClickHouse-Summary'])
            except json.JSONDecodeError:
                pass
        return QueryResult(data_result.result,
                           data_result.column_names,
                           data_result.column_types,
                           response.headers.get('X-ClickHouse-Query-Id'),
                           summary,
                           data_result.column_oriented)

    def data_insert(self, context: InsertContext):
        """
        See BaseClient doc_string for this method
        """
        if context.empty:
            logger.debug('No data included in insert, skipping')
            return
        context.compression = self.compression
        block_gen = self.transform.build_insert(context)

        def error_handler(response: Response):
            # If we actually had a local exception when building the insert, throw that instead
            if context.insert_exception:
                ex = context.insert_exception
                context.insert_exception = None
                raise ProgrammingError('Internal serialization error.  This usually indicates invalid data types ' +
                                       'in an inserted row or column') from ex
            self._error_handler(response)

        self.raw_insert(context.table,
                        context.column_names,
                        block_gen,
                        context.settings,
                        self.write_format,
                        context.compression,
                        error_handler)
        context.data = None

    def raw_insert(self, table: str,
                   column_names: Sequence[str],
                   insert_block: Union[str, bytes],
                   settings: Optional[Dict] = None,
                   fmt: Optional[str] = None,
                   compression: Optional[str] = None,
                   status_handler: Optional[Callable] = None):
        """
        See BaseClient doc_string for this method
        """
        column_ids = [quote_identifier(x) for x in column_names]
        write_format = fmt if fmt else self.write_format
        headers = {'Content-Type': 'application/octet-stream'}
        if compression:
            headers['Content-Encoding'] = compression
        params = {'query': f"INSERT INTO {table} ({', '.join(column_ids)}) FORMAT {write_format}",
                  'database': self.database}
        if isinstance(insert_block, str):
            insert_block = insert_block.encode()
        params.update(self._validate_settings(settings or {}))
        response = self._raw_request(insert_block, params, headers, error_handler=status_handler)
        logger.debug('Insert response code: %d, content: %s', response.status_code, response.content)

    def command(self,
                cmd,
                parameters: Optional[Union[Sequence, Dict[str, Any]]] = None,
                data: Union[str, bytes] = None,
                settings: Optional[Dict] = None,
                use_database: int = True) -> Union[str, int, Sequence[str]]:
        """
        See BaseClient doc_string for this method
        """
        cmd = finalize_query(cmd, parameters, self.server_tz)
        headers = {}
        params = {}
        payload = None
        if isinstance(data, str):
            headers['Content-Type'] = 'text/plain; charset=utf-8'
            payload = data.encode()
        elif isinstance(data, bytes):
            headers['Content-Type'] = 'application/octet-stream'
            payload = data
        if payload is None:
            if not cmd:
                raise ProgrammingError('Command sent without query or recognized data') from None
            payload = cmd
        elif cmd:
            params['query'] = cmd
        if use_database:
            params['database'] = self.database
        params.update(self._validate_settings(settings or {}))
        method = 'POST' if payload else 'GET'
        response = self._raw_request(payload, params, headers, method)
        result = response.content.decode()[:-1].split('\t')
        if len(result) == 1:
            try:
                return int(result[0])
            except ValueError:
                return result[0]
        return result

    def _error_handler(self, response: Response = None, retried: bool = False) -> None:
        err_str = f'HTTPDriver for {self.url} returned response code {response.status_code})'
        if response.content:
            err_msg = response.content.decode(errors='backslashreplace')
            logger.error(str(err_msg))
            err_str = f':{err_str}\n {err_msg[0:240]}'
        raise OperationalError(err_str) if retried else DatabaseError(err_str) from None

    def _raw_request(self,
                     data,
                     params: Dict[str, Any],
                     headers: Optional[Dict[str, Any]] = None,
                     method: str = 'POST',
                     retries: int = 0,
                     error_handler: Callable = None) -> Response:
        if isinstance(data, str):
            data = data.encode()
        attempts = 0
        while True:
            attempts += 1
            try:
                response: Response = self.session.request(method, self.url,
                                                          headers=headers,
                                                          timeout=(self.connect_timeout, self.read_timeout),
                                                          data=data,
                                                          params=params)
            except RequestException as ex:
                rex_context = ex.__context__
                if rex_context and isinstance(rex_context.__context__, RemoteDisconnected):
                    # See https://github.com/psf/requests/issues/4664
                    # The server closed the connection, probably because the Keep Alive has expired
                    # We should be safe to retry, as ClickHouse should not have processed anything on a connection
                    # that it killed.  We also only retry this once, as multiple disconnects are unlikely to be
                    # related to the Keep Alive settings
                    if attempts == 1:
                        logger.debug('Retrying remotely closed connection')
                        attempts = 0
                        continue
                logger.exception('Unexpected Http Driver Exception')
                raise OperationalError(f'Error executing HTTP request {self.url}') from ex
            if 200 <= response.status_code < 300:
                return response
            if response.status_code in (429, 503, 504):
                if attempts > retries:
                    self._error_handler(response, True)
                logger.debug('Retrying requests with status code %d', response.status_code)
            else:
                if error_handler:
                    error_handler(response)
                self._error_handler(response)

    def ping(self):
        """
        See BaseClient doc_string for this method
        """
        try:
            response = req_get(f'{self.url}/ping', timeout=3)
            return 200 <= response.status_code < 300
        except RequestException:
            logger.debug('ping failed', exc_info=True)
            return False

    def raw_query(self,
                  query: str,
                  parameters: Optional[Union[Sequence, Dict[str, Any]]] = None,
                  settings: Optional[Dict[str, Any]] = None,
                  fmt: str = None):
        """
        See BaseClient doc_string for this method
        """
        final_query = finalize_query(query, parameters, self.server_tz)
        if fmt:
            final_query += f'\n FORMAT {fmt}'
        return self._raw_request(final_query, self._validate_settings(settings or {})).content


def reset_connections():
    """
    Used for tests to force new connection by resetting the singleton HttpAdapter
    """

    global default_adapter  # pylint: disable=global-statement
    default_adapter = KeepAliveAdapter(pool_connections=4, pool_maxsize=8, max_retries=0)
