import logging
import json
import atexit
import re
from typing import Optional, Dict, Any, Sequence, Union, List
from requests import Session, Response
from requests.adapters import HTTPAdapter, Retry
from requests.exceptions import RequestException

from clickhouse_connect.datatypes import registry
from clickhouse_connect.driver import native
from clickhouse_connect.driver import rowbinary
from clickhouse_connect.datatypes.base import ClickHouseType
from clickhouse_connect.driver.client import Client
from clickhouse_connect.driver.exceptions import DatabaseError, OperationalError, ProgrammingError
from clickhouse_connect.driver.query import QueryResult, DataResult

logger = logging.getLogger(__name__)
columns_only_re = re.compile(r'LIMIT 0\s*$', re.IGNORECASE)

# Create a single HttpAdapter that will be shared by all client sessions.  This is intended to make
# the client as thread safe as possible while sharing a single connection pool.  For the same reason we
# don't call the Session.close() method from the client so the connection pool remains available
retry = Retry(total=2, status_forcelist=[429, 503, 504],
              method_whitelist=['HEAD', 'GET', 'OPTIONS', 'POST'],
              backoff_factor=1.5)
http_adapter = HTTPAdapter(pool_connections=4, pool_maxsize=8, max_retries=retry)
atexit.register(http_adapter.close)


# pylint: disable=too-many-instance-attributes
class HttpClient(Client):
    # pylint: disable=too-many-arguments,too-many-locals
    def __init__(self, interface: str, host: str, port: int, username: str, password: str, database: str,
                 compress: bool = True, data_format: str = 'native', query_limit: int = 5000,
                 connect_timeout: int = 10, send_receive_timeout=60, client_name: str = 'clickhouse-connect',
                 verify: bool = True, ca_cert: str = None, client_cert: str = None, client_cert_key: str = None,
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
        :param data_format: Dataformat -- either 'native' or 'rb' (RowBinary)
        :param query_limit: Default LIMIT on returned rows
        :param connect_timeout:  Timeout in seconds for the http connection
        :param send_receive_timeout: Read timeout in seconds for http connection
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

        if compress:
            session.headers['Accept-Encoding'] = 'gzip, br'
        if data_format == 'native':
            self.read_format = self.write_format = 'Native'
            self.build_insert = native.build_insert
            self.parse_response = native.parse_response
            self.column_inserts = True
        elif data_format in ('row_binary', 'rb'):
            self.read_format = 'RowBinaryWithNamesAndTypes'
            self.write_format = 'RowBinary'
            self.build_insert = rowbinary.build_insert
            self.parse_response = rowbinary.parse_response
            self.column_inserts = False
        self.session = session
        self.connect_timeout = connect_timeout
        self.read_timeout = send_receive_timeout
        self.common_settings = {}
        super().__init__(database=database, query_limit=query_limit, uri=self.url, settings=kwargs)

    def _apply_settings(self, settings: Dict[str, Any] = None):
        valid_settings = self._validate_settings(settings)
        for key, value in valid_settings.items():
            if isinstance(value, bool):
                self.common_settings[key] = '1' if value else '0'
            else:
                self.common_settings[key] = str(value)

    def _format_query(self, query: str) -> str:
        query = query.strip()
        if query.upper().startswith('INSERT ') and 'VALUES' in query.upper():
            return query
        if not query.strip().endswith(self.read_format):
            query += f' FORMAT {self.read_format}'
        return query

    def exec_query(self, query: str, settings: Optional[Dict] = None, use_none: bool = True, ) -> QueryResult:
        """
        See BaseClient doc_string for this method
        """
        headers = {'Content-Type': 'text/plain'}
        params = self.common_settings.copy()
        params['database'] = self.database
        if settings:
            params.update(settings)
        columns_only = columns_only_re.search(query)
        if columns_only:
            final_query = query + ' FORMAT JSON'
        else:
            final_query = self._format_query(query)
        response = self._raw_request(final_query, params=params, headers=headers)
        if columns_only:
            json_result = json.loads(response.content)
            names: List[str] = []
            types: List[ClickHouseType] = []
            for col in json_result['meta']:
                names.append(col['name'])
                types.append(registry.get_from_name(col['type']))
            data_result = DataResult([], tuple(names), tuple(types))
        else:
            data_result = self.parse_response(response.content, use_none)
        summary = {}
        if 'X-ClickHouse-Summary' in response.headers:
            try:
                summary = json.loads(response.headers['X-ClickHouse-Summary'])
            except json.JSONDecodeError:
                pass
        return QueryResult(data_result.result, data_result.column_names, data_result.column_types,
                           response.headers.get('X-ClickHouse-Query-Id'), summary)

    def data_insert(self, table: str, column_names: Sequence[str], data: Sequence[Sequence[Any]],
                    column_types: Sequence[ClickHouseType], settings: Optional[Dict] = None, column_oriented: bool = False):
        """
        See BaseClient doc_string for this method
        """
        headers = {'Content-Type': 'application/octet-stream'}
        params = self.common_settings.copy()
        params['query'] = f"INSERT INTO {table} ({', '.join(column_names)}) FORMAT {self.write_format}"
        params['database'] = self.database
        if settings:
            params.update(settings)
        insert_block = self.build_insert(data, column_types=column_types, column_names=column_names, column_oriented=column_oriented)
        response = self._raw_request(insert_block, params=params, headers=headers)
        logger.debug('Insert response code: %d, content: %s', response.status_code, response.content)

    def exec_command(self, cmd, use_database: bool = True, settings: Optional[Dict] = None) -> Union[str, int, Sequence[str]]:
        """
        See BaseClient doc_string for this method
        """
        headers = {'Content-Type': 'text/plain'}
        params = self.common_settings.copy()
        params['query'] = cmd
        if use_database:
            params['database'] = self.database
        if settings:
            params.update(settings)
        result = self._raw_request(params=params, headers=headers).content.decode('utf8')[:-1].split('\t')
        if len(result) == 1:
            try:
                return int(result[0])
            except ValueError:
                return result[0]
        return result

    def _raw_request(self, data=None, method='POST', params: Optional[Dict] = None, headers: Optional[Dict] = None):
        try:
            response: Response = self.session.request(method, self.url,
                                                      headers=headers,
                                                      timeout=(self.connect_timeout, self.read_timeout),
                                                      data=data,
                                                      params=params)
        except RequestException as ex:
            logger.exception('Unexpected Http Driver Exception')
            raise OperationalError(f'Error executing HTTP request {self.url}') from ex
        if 200 <= response.status_code < 300:
            return response
        err_str = f'HTTPDriver url {self.url} returned response code {response.status_code})'
        logger.error(err_str)
        if response.content:
            logger.error(str(response.content))
            err_str = f':{err_str}\n {str(response.content)[0:240]}'
        raise DatabaseError(err_str)

    def ping(self):
        """
        See BaseClient doc_string for this method
        """
        try:
            response = self.session.get(f'{self.url}/ping', timeout=3)
            return 200 <= response.status_code < 300
        except RequestException:
            logger.debug('ping failed', exc_info=True)
            return False


def reset_connections():
    """
    Used for tests to force new connection by resetting the singleton HttpAdapter
    """

    global http_adapter  # pylint: disable=global-statement
    http_adapter = HTTPAdapter(pool_connections=4, pool_maxsize=8, max_retries=retry)
