import logging
import json
from typing import Iterable, Optional, Dict, Any

import requests

from clickhouse_connect.datatypes.base import ClickHouseType
from clickhouse_connect.driver import BaseDriver
from clickhouse_connect.driver.exceptions import ServerError, DriverError
from clickhouse_connect.driver.query import QueryResult
from clickhouse_connect.driver.rowbinary import build_insert
from clickhouse_connect.driver.native import parse_response

logger = logging.getLogger(__name__)

format_str = ' FORMAT Native'


def format_query(query:str) -> str:
    if not query.strip().endswith(format_str):
        query += format_str
    return query


class HttpDriver(BaseDriver):
    def __init__(self, scheme: str, host:str, port: int, username:str, password: str, database: str,
                 compress: bool = True, **kwargs):
        super().__init__(database, **kwargs)
        self.params = {}
        self.headers = {}
        self.compress = compress
        self.url = '{}://{}:{}'.format(scheme, host, port)
        self.auth = (username, password) if username else None

    def query(self, query:str) -> QueryResult:
        headers = {'Content-Type': 'text/plain'}
        params = {'database': self.database}
        if self.compress:
            params['enable_http_compression'] = '1'
            headers['Accept-Encoding'] = 'gzip, br'
        response = self.raw_request(format_query(query), params=params, headers=headers)
        result_set, column_names, column_types = parse_response(response.content)
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

    def data_insert(self, table:str, column_names: Iterable[str], data: Iterable[Iterable[Any]],
                    column_types: Iterable[ClickHouseType]):
        headers = {'Content-Type': 'application/octet-stream'}
        params = {'query':  f"INSERT INTO {table} ({', '.join(column_names)}) FORMAT RowBinary"}
        insert_block = build_insert(data, column_types=column_types)
        response = self.raw_request(insert_block, params=params, headers=headers)
        logger.debug(f'Insert response code: {response.status_code}, content: {str(response.content)}')

    def command(self, cmd:str):
        headers = {'Content-Type': 'text/plain'}
        return self.raw_request(params={'query': cmd}, headers=headers).content.decode('utf8')[:-1]

    def raw_request(self, data=None, method='post', params: Optional[Dict] = None, headers: Optional[Dict] = None):
        try:
            req_headers = self.headers
            if headers:
                req_headers.update(headers)
            response:requests.Response = requests.request(method, self.url,
                                     auth=self.auth,
                                     headers=req_headers,
                                     timeout=(10, 60),
                                     data=data,
                                     params=params)
            if 200 <= response.status_code < 300:
                return response
            err_str = f"HTTPDriver url {self.url} returned response code {response.status_code})"
            logger.error(err_str)
            if response.content:
                logger.error(str(response.content))
            raise ServerError(err_str)
        except Exception:
            logger.exception("Unexpected Http Driver Exception")
            raise DriverError(f"Error executing HTTP request {self.url}")

    def ping(self) -> bool:
        try:
            response = requests.request('get', self.url + '/ping')
            return 200 <= response.status_code < 300
        except requests.exceptions.RequestException:
            return False
