import logging
import json
import atexit
from typing import Optional, Dict, Any, Sequence, Collection
from requests import Session, Response
from requests.adapters import HTTPAdapter, Retry
from requests.exceptions import RequestException

from clickhouse_connect.driver import native
from clickhouse_connect.driver import rowbinary
from clickhouse_connect.datatypes.base import ClickHouseType
from clickhouse_connect.driver import BaseClient
from clickhouse_connect.driver.exceptions import DatabaseError, OperationalError
from clickhouse_connect.driver.query import QueryResult

logger = logging.getLogger(__name__)

# Create a single HttpAdapter that will be shared by all client sessions.  This is intended to make
# the client as thread safe as possible while sharing a single connection pool.  For the same reason we
# don't call the Session.close() method from the client so the connection pool remains available
retry = Retry(total=2, status_forcelist=[429, 503, 504],
              method_whitelist=['HEAD', 'GET', 'OPTIONS', 'POST'],
              backoff_factor=1.5)
http_adapter = HTTPAdapter(pool_connections=4, pool_maxsize=8, max_retries=retry)
atexit.register(http_adapter.close)


# pylint: disable=too-many-instance-attributes
class HttpClient(BaseClient):
    # pylint: disable=too-many-arguments
    def __init__(self, scheme: str, host: str, port: int, username: str, password: str, database: str,
                 validate: bool = True, compress: bool = True, data_format: str = 'native', query_limit: int = 5000):
        super().__init__(database, query_limit)

        self.url = f'{scheme}://{host}:{port}'
        self.session = Session()
        self.session.auth = (username, password if password else '') if username else None
        self.session.stream = True
        self.session.max_redirects = 3

        # Remove the default session adapters, they are not used and this avoids issues with their connection pools
        self.session.adapters.pop('http://').close()
        self.session.adapters.pop('https://').close()
        self.session.mount(self.url, adapter=http_adapter)

        if compress:
            self.session.headers['Accept-Encoding'] = 'gzip, br'
        if validate:
            self.command('SELECT 1')
        if data_format == 'native':
            self.read_format = self.write_format = 'Native'
            self.build_insert = native.build_insert
            self.parse_response = native.parse_response
        elif data_format in ('row_binary', 'rb'):
            self.read_format = 'RowBinaryWithNamesAndTypes'
            self.write_format = 'RowBinary'
            self.build_insert = rowbinary.build_insert
            self.parse_response = rowbinary.parse_response

    def format_query(self, query: str) -> str:
        if not query.strip().endswith(self.read_format):
            query += f' FORMAT {self.read_format}'
        return query

    def exec_query(self, query: str, use_none: bool = True) -> QueryResult:
        headers = {'Content-Type': 'text/plain'}
        params = {'database': self.database}
        response = self.raw_request(self.format_query(query), params=params, headers=headers)
        data_result = self.parse_response(response.raw, use_none)
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
                           summary)

    def data_insert(self, table: str, column_names: Sequence[str], data: Collection[Collection[Any]],
                    column_types: Sequence[ClickHouseType]):
        headers = {'Content-Type': 'application/octet-stream'}
        params = {'query': f"INSERT INTO {table} ({', '.join(column_names)}) FORMAT {self.write_format}"}
        insert_block = self.build_insert(data, column_types=column_types, column_names=column_names)
        response = self.raw_request(insert_block, params=params, headers=headers)
        logger.debug('Insert response code: {}, content: {}', response.status_code, response.content)

    def command(self, cmd: str):
        headers = {'Content-Type': 'text/plain'}
        return self.raw_request(params={'query': cmd}, headers=headers).content.decode('utf8')[:-1]

    def raw_request(self, data=None, method='post', params: Optional[Dict] = None, headers: Optional[Dict] = None):
        try:
            response: Response = self.session.request(method, self.url,
                                                      headers=headers,
                                                      timeout=(10, 60),
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
        raise DatabaseError(err_str)

    def ping(self):
        try:
            response = self.session.get(f'{self.url}/ping', timeout=3)
            return 200 <= response.status_code < 300
        except RequestException:
            logger.debug('ping failed', exc_info=True)
            return False
