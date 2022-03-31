import logging
import json
from typing import Optional, Dict, Any, Sequence, Collection

import requests
from clickhouse_connect.datatypes.base import ClickHouseType
from clickhouse_connect.driver import BaseDriver
from clickhouse_connect.driver.exceptions import ServerError, DriverError
from clickhouse_connect.driver.query import QueryResult

logger = logging.getLogger(__name__)


class HttpDriver(BaseDriver):
    def __init__(self, scheme: str, host: str, port: int, username: str, password: str, database: str,
                 compress: bool = True, data_format: str = 'native', query_limit: int =5000):
        super().__init__(database, query_limit)
        self.params = {}
        self.headers = {}
        self.compress = compress
        self.url = f'{scheme}://{host}:{port}'
        self.auth = (username, password if password else '') if username else None
        if data_format == 'native':
            self.read_format = self.write_format = 'Native'
            import clickhouse_connect.driver.native
            self.build_insert = clickhouse_connect.driver.native.build_insert
            self.parse_response = clickhouse_connect.driver.native.parse_response
        elif data_format in ('row_binary', 'rb'):
            self.read_format = 'RowBinaryWithNamesAndTypes'
            import clickhouse_connect.driver.rowbinary
            self.write_format = 'RowBinary'
            self.build_insert = clickhouse_connect.driver.rowbinary.build_insert
            self.parse_response = clickhouse_connect.driver.rowbinary.parse_response

    def format_query(self, query: str) -> str:
        if not query.strip().endswith(self.read_format):
            query += f' FORMAT {self.read_format}'
        return query

    def exec_query(self, query: str, use_none: bool = True) -> QueryResult:
        headers = {'Content-Type': 'text/plain'}
        params = {'database': self.database}
        if self.compress:
            headers['Accept-Encoding'] = 'br, gzip'
        response = self.raw_request(self.format_query(query), params=params, headers=headers)
        result_set, column_names, column_types = self.parse_response(response.content, use_none)
        summary = {}
        if 'X-ClickHouse-Summary' in response.headers:
            try:
                summary = json.loads(response.headers['X-ClickHouse-Summary'])
            except json.JSONDecodeError:
                pass
        return QueryResult(result_set,
                           column_names,
                           column_types,
                           response.headers.get('X-ClickHouse-Query-Id'),
                           summary)

    def data_insert(self, table: str, column_names: Sequence[str], data: Collection[Collection[Any]],
                    column_types: Sequence[ClickHouseType]):
        headers = {'Content-Type': 'application/octet-stream'}
        params = {'query': f"INSERT INTO {table} ({', '.join(column_names)}) FORMAT {self.write_format}"}
        insert_block = self.build_insert(data, column_types=column_types, column_names=column_names)
        response = self.raw_request(insert_block, params=params, headers=headers)
        logger.debug(f'Insert response code: {response.status_code}, content: {str(response.content)}')

    def command(self, cmd: str):
        headers = {'Content-Type': 'text/plain'}
        return self.raw_request(params={'query': cmd}, headers=headers).content.decode('utf8')[:-1]

    def raw_request(self, data=None, method='post', params: Optional[Dict] = None, headers: Optional[Dict] = None):
        req_headers = self.headers
        if headers:
            req_headers.update(headers)
        try:
            response: requests.Response = requests.request(method, self.url,
                                                           auth=self.auth,
                                                           headers=req_headers,
                                                           timeout=(10, 60),
                                                           data=data,
                                                           params=params)
        except Exception:
            logger.exception("Unexpected Http Driver Exception")
            raise DriverError(f"Error executing HTTP request {self.url}")
        if 200 <= response.status_code < 300:
            return response
        err_str = f"HTTPDriver url {self.url} returned response code {response.status_code})"
        logger.error(err_str)
        if response.content:
            logger.error(str(response.content, encoding='utf8'))
        raise ServerError(err_str)

    def ping(self) -> bool:
        try:
            response = requests.request('GET', self.url + '/ping')
            return 200 <= response.status_code < 300
        except requests.exceptions.RequestException:
            logger.debug('ping failed', exc_info=True)
            return False
