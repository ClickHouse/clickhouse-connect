import logging
import json
import atexit
import re
import http as PyHttp
from http.client import RemoteDisconnected

from typing import Optional, Dict, Any, Sequence, Union, List
from requests import Session, Response, get as req_get
from requests.exceptions import RequestException

from clickhouse_connect.datatypes import registry
from clickhouse_connect.datatypes.base import ClickHouseType
from clickhouse_connect.driver.client import Client
from clickhouse_connect.driver.exceptions import DatabaseError, OperationalError, ProgrammingError
from clickhouse_connect.driver.httpadapter import KeepAliveAdapter
from clickhouse_connect.driver.native import NativeTransform
from clickhouse_connect.driver.query import QueryResult, DataResult, QueryContext, finalize_query, quote_identifier

logger = logging.getLogger(__name__)
columns_only_re = re.compile(r'LIMIT 0\s*$', re.IGNORECASE)

# Create a single HttpAdapter that will be shared by all client sessions.  This is intended to make
# the client as thread safe as possible while sharing a single connection pool.  For the same reason we
# don't call the Session.close() method from the client so the connection pool remains available
http_adapter = KeepAliveAdapter(pool_connections=4, pool_maxsize=8, max_retries=0)
atexit.register(http_adapter.close)

# Increase this number just to be safe when ClickHouse is returning progress headers
PyHttp._MAXHEADERS = 10000  # pylint: disable=protected-access


# pylint: disable=too-many-instance-attributes
class HttpClient(Client):
    valid_transport_settings = {'database', 'buffer_size', 'session_id', 'compress', 'decompress',
                                'session_timeout', 'session_check', 'query_id', 'quota_key', 'wait_end_of_query'}

    # pylint: disable=too-many-arguments,too-many-locals,too-many-branches,too-many-statements
    def __init__(self,
                 interface: str,
                 host: str,
                 port: int,
                 username: str,
                 password: str,
                 database: str,
                 compress: bool = True,
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
        :param kwargs: Optional clickhouse setting values (str/value) for every connection request
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
        session.mount(self.url, adapter=http_adapter)
        session.headers['User-Agent'] = client_name

        self.read_format = self.write_format = 'Native'
        self.column_inserts = True
        self.transform = NativeTransform()

        self.session = session
        self.connect_timeout = connect_timeout
        self.read_timeout = send_receive_timeout
        self.query_retries = query_retries
        settings = kwargs.copy()
        if send_progress:
            settings['send_progress_in_http_headers'] = '1'
            settings['wait_end_of_query'] = '1'
            if self.read_timeout > 10:
                progress_interval = (self.read_timeout - 5) * 1000
            else:
                progress_interval = 120000  # Two minutes
            settings['http_headers_progress_interval_ms'] = str(progress_interval)
        if compress:
            session.headers['Accept-Encoding'] = 'gzip'
            settings['enable_http_compression'] = '1'
        super().__init__(database=database, query_limit=query_limit, uri=self.url)
        self.session.params = self._validate_settings(settings, True)

    def client_setting(self, name, value):
        if isinstance(value, bool):
            value = '1' if value else '0'
        self.session.params[name] = str(value)

    def _prep_query(self, context: QueryContext):
        final_query = super()._prep_query(context)
        if context.is_insert:
            return final_query
        return f'{final_query}\n FORMAT {self.write_format}'

    def _query_with_context(self, context: QueryContext) -> QueryResult:
        headers = {'Content-Type': 'text/plain; charset=utf-8'}
        params = {'database': self.database}
        params.update(self._validate_settings(context.settings, True))
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
        return QueryResult(data_result.result, data_result.column_names, data_result.column_types,
                           response.headers.get('X-ClickHouse-Query-Id'), summary)

    def data_insert(self,
                    table: str,
                    column_names: Sequence[str],
                    data: Sequence[Sequence[Any]],
                    column_types: Sequence[ClickHouseType],
                    settings: Optional[Dict[str, Any]] = None,
                    column_oriented: bool = False):
        """
        See BaseClient doc_string for this method
        """
        insert_block = self.transform.build_insert(data, column_types=column_types, column_names=column_names,
                                                   column_oriented=column_oriented)
        self.raw_insert(table, column_names, insert_block, settings, self.write_format)

    def raw_insert(self, table: str,
                   column_names: Sequence[str],
                   insert_block: Union[str, bytes],
                   settings: Optional[Dict] = None,
                   fmt: Optional[str] = None):
        """
        See BaseClient doc_string for this method
        """
        column_ids = [quote_identifier(x) for x in column_names]
        write_format = fmt if fmt else self.write_format
        headers = {'Content-Type': 'application/octet-stream'}
        params = {'query': f"INSERT INTO {table} ({', '.join(column_ids)}) FORMAT {write_format}",
                  'database': self.database}
        if isinstance(insert_block, str):
            insert_block = insert_block.encode()
        params.update(self._validate_settings(settings, True))
        response = self._raw_request(insert_block, params, headers)
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
                raise ProgrammingError('Command sent without query or recognized data')
            payload = cmd
        elif cmd:
            params['query'] = cmd
        if use_database:
            params['database'] = self.database
        params.update(self._validate_settings(settings, True))
        method = 'POST' if payload else 'GET'
        response = self._raw_request(payload, params, headers, method)
        result = response.content.decode('utf8')[:-1].split('\t')
        if len(result) == 1:
            try:
                return int(result[0])
            except ValueError:
                return result[0]
        return result

    def _raw_request(self,
                     data,
                     params: Dict[str, Any],
                     headers: Optional[Dict[str, Any]] = None,
                     method: str = 'POST',
                     retries: int = 0) -> Response:
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
            err_str = f'HTTPDriver url {self.url} returned response code {response.status_code})'
            logger.error(err_str)
            if response.content:
                err_msg = response.content.decode(errors='backslashreplace')
                logger.error(str(err_msg))
                err_str = f':{err_str}\n {err_msg[0:240]}'
            if response.status_code not in (429, 503, 504):
                raise DatabaseError(err_str)
            if attempts > retries:
                raise OperationalError(err_str)

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
        return self._raw_request(final_query, self._validate_settings(settings, True)).content


def reset_connections():
    """
    Used for tests to force new connection by resetting the singleton HttpAdapter
    """

    global http_adapter  # pylint: disable=global-statement
    http_adapter = KeepAliveAdapter(pool_connections=4, pool_maxsize=8, max_retries=0)
