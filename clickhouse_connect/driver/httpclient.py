import json
import logging
import re
import uuid
from base64 import b64encode
from typing import Optional, Dict, Any, Sequence, Union, List, Callable, Generator, BinaryIO
from urllib.parse import urlencode

from urllib3 import Timeout
from urllib3.exceptions import HTTPError
from urllib3.poolmanager import PoolManager
from urllib3.response import HTTPResponse

from clickhouse_connect import common
from clickhouse_connect.datatypes import registry
from clickhouse_connect.datatypes.base import ClickHouseType
from clickhouse_connect.driver.ctypes import RespBuffCls
from clickhouse_connect.driver.client import Client
from clickhouse_connect.driver.common import dict_copy, coerce_bool, coerce_int
from clickhouse_connect.driver.compression import available_compression
from clickhouse_connect.driver.exceptions import DatabaseError, OperationalError, ProgrammingError
from clickhouse_connect.driver.httputil import ResponseSource, get_pool_manager, get_response_data, default_pool_manager
from clickhouse_connect.driver.insert import InsertContext
from clickhouse_connect.driver.query import QueryResult, QueryContext, quote_identifier, bind_query
from clickhouse_connect.driver.transform import NativeTransform

logger = logging.getLogger(__name__)
columns_only_re = re.compile(r'LIMIT 0\s*$', re.IGNORECASE)


# pylint: disable=too-many-instance-attributes
class HttpClient(Client):
    params = {}
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
                 client_name: Optional[str] = None,
                 send_progress: bool = True,
                 verify: bool = True,
                 ca_cert: Optional[str] = None,
                 client_cert: Optional[str] = None,
                 client_cert_key: Optional[str] = None,
                 session_id: Optional[str] = None,
                 settings: Optional[Dict[str, Any]] = None,
                 pool_mgr: Optional[PoolManager] = None):
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
         certificate is trusted by the operating system.  To trust the maintained list of "global" public root
         certificates maintained by the Python 'certifi' package, set ca_cert to 'certifi'
        :param client_cert: File path to a TLS Client certificate in .pem format.  This file should contain any
          applicable intermediate certificates
        :param client_cert_key: File path to the private key for the Client Certificate.  Required if the private key
          is not included the Client Certificate key file
        :param session_id ClickHouse session id.  If not specified and the common setting 'autogenerate_session_id'
          is True, the client will generate a UUID1 session id
        :param settings Optional dictionary of ClickHouse setting values (str/value) for every connection request
        :param pool_mgr Optional urllib3 PoolManager for this client.  Useful for creating separate connection
          pools for multiple client endpoints for applications with many clients
        """
        self.url = f'{interface}://{host}:{port}'
        self.headers = {}
        ch_settings = settings or {}
        self.http = pool_mgr
        if interface == 'https':
            if client_cert:
                if not username:
                    raise ProgrammingError('username parameter is required for Mutual TLS authentication')
                self.headers['X-ClickHouse-User'] = username
                self.headers['X-ClickHouse-SSL-Certificate-Auth'] = 'on'
            verify = coerce_bool(verify)
            if not self.http and (ca_cert or client_cert or not verify):
                self.http = get_pool_manager(ca_cert=ca_cert,
                                             client_cert=client_cert,
                                             verify=verify,
                                             client_cert_key=client_cert_key)
        if not self.http:
            self.http = default_pool_manager

        if not client_cert and username:
            self.headers['Authorization'] = 'Basic ' + b64encode(f'{username}:{password}'.encode()).decode()
        self.headers['User-Agent'] = common.build_client_name(client_name)
        self._read_format = self._write_format = 'Native'
        self._transform = NativeTransform()

        connect_timeout, send_receive_timeout = coerce_int(connect_timeout), coerce_int(send_receive_timeout)
        self.timeout = Timeout(connect=connect_timeout, read=send_receive_timeout)
        self.query_retries = coerce_int(query_retries)
        self.http_retries = 1

        if coerce_bool(send_progress):
            ch_settings['wait_end_of_query'] = '1'
            # We can't actually read the progress headers, but we enable them so ClickHouse sends data
            # to keep the connection alive when waiting for long-running queries that don't return data
            # Accordingly we make sure it's always less than the read timeout
            ch_settings['send_progress_in_http_headers'] = '1'
            progress_interval = min(120000, (send_receive_timeout - 5) * 1000)
            ch_settings['http_headers_progress_interval_ms'] = str(progress_interval)

        if session_id:
            ch_settings['session_id'] = session_id
        elif 'session_id' not in ch_settings and common.get_setting('autogenerate_session_id'):
            ch_settings['session_id'] = str(uuid.uuid1())

        if coerce_bool(compress):
            compression = ','.join(available_compression)
            self.write_compression = available_compression[0]
        elif compress and compress not in ('False', 'false', '0'):
            if compress not in available_compression:
                raise ProgrammingError(f'Unsupported compression method {compress}')
            compression = compress
            self.write_compression = compress
        else:
            compression = None

        super().__init__(database=database, query_limit=coerce_int(query_limit), uri=self.url)

        comp_setting = self.server_settings['enable_http_compression']
        # We only set the header for the query method so no need to modify headers or settings here
        if comp_setting and (comp_setting.value == '1' or comp_setting.readonly != 1):
            self.compression = compression
        self.params = self._validate_settings(ch_settings)

    def set_client_setting(self, key, value):
        str_value = self._validate_setting(key, value, common.get_setting('invalid_setting_action'))
        if str_value is not None:
            self.params[key] = str_value

    def get_client_setting(self, key) -> Optional[str]:
        values = self.params.get(key)
        return values[0] if values else None

    def _prep_query(self, context: QueryContext):
        final_query = super()._prep_query(context)
        if context.is_insert:
            return final_query
        return f'{final_query}\n FORMAT {self._write_format}'

    def _query_with_context(self, context: QueryContext) -> QueryResult:
        headers = {'Content-Type': 'text/plain; charset=utf-8'}
        params = {}
        if self.database:
            params['database'] = self.database
        params.update(context.bind_params)
        params.update(self._validate_settings(context.settings))
        if columns_only_re.search(context.uncommented_query):
            response = self._raw_request(f'{context.final_query}\n FORMAT JSON',
                                         params, headers, retries=self.query_retries)
            json_result = json.loads(response.data)
            # ClickHouse will respond with a JSON object of meta, data, and some other objects
            # We just grab the column names and column types from the metadata sub object
            names: List[str] = []
            types: List[ClickHouseType] = []
            for col in json_result['meta']:
                names.append(col['name'])
                types.append(registry.get_from_name(col['type']))
            return QueryResult([], None, tuple(names), tuple(types))

        if self.compression:
            headers['Accept-Encoding'] = self.compression
            params['enable_http_compression'] = '1'
        response = self._raw_request(self._prep_query(context), params, headers, stream=True,
                                     retries=self.query_retries)
        byte_source = RespBuffCls(ResponseSource(response))  # pylint: disable=not-callable
        query_result = self._transform.parse_response(byte_source, context)
        if 'X-ClickHouse-Summary' in response.headers:
            try:
                summary = json.loads(response.headers['X-ClickHouse-Summary'])
                query_result.summary = summary
            except json.JSONDecodeError:
                pass
        query_result.query_id = response.headers.get('X-ClickHouse-Query-Id')
        return query_result

    def data_insert(self, context: InsertContext):
        """
        See BaseClient doc_string for this method
        """
        if context.empty:
            logger.debug('No data included in insert, skipping')
            return
        if context.compression is None:
            context.compression = self.write_compression
        block_gen = self._transform.build_insert(context)

        def error_handler(response: HTTPResponse):
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
                        self._write_format,
                        context.compression,
                        error_handler)
        context.data = None

    def raw_insert(self, table: str,
                   column_names: Optional[Sequence[str]] = None,
                   insert_block: Union[str, bytes, Generator[bytes, None, None], BinaryIO] = None,
                   settings: Optional[Dict] = None,
                   fmt: Optional[str] = None,
                   compression: Optional[str] = None,
                   status_handler: Optional[Callable] = None):
        """
        See BaseClient doc_string for this method
        """
        write_format = fmt if fmt else self._write_format
        headers = {'Content-Type': 'application/octet-stream'}
        if compression:
            headers['Content-Encoding'] = compression
        cols = f" ({', '.join([quote_identifier(x) for x in column_names])})" if column_names is not None else ''
        params = {'query': f'INSERT INTO {table}{cols} FORMAT {write_format}'}
        if self.database:
            params['database'] =  self.database
        params.update(self._validate_settings(settings or {}))
        response = self._raw_request(insert_block, params, headers, error_handler=status_handler)
        logger.debug('Insert response code: %d, content: %s', response.status, response.data)

    def command(self,
                cmd,
                parameters: Optional[Union[Sequence, Dict[str, Any]]] = None,
                data: Union[str, bytes] = None,
                settings: Optional[Dict] = None,
                use_database: int = True) -> Union[str, int, Sequence[str]]:
        """
        See BaseClient doc_string for this method
        """
        cmd, params = bind_query(cmd, parameters, self.server_tz)
        headers = {}
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
        if use_database and self.database:
            params['database'] = self.database
        params.update(self._validate_settings(settings or {}))
        method = 'POST' if payload else 'GET'
        response = self._raw_request(payload, params, headers, method)
        result = response.data.decode()[:-1].split('\t')
        if len(result) == 1:
            try:
                return int(result[0])
            except ValueError:
                return result[0]
        return result

    def _error_handler(self, response: HTTPResponse, retried: bool = False) -> None:
        err_str = f'HTTPDriver for {self.url} returned response code {response.status})'
        err_content = get_response_data(response)
        if err_content:
            err_msg = err_content.decode(errors='backslashreplace')
            logger.error(err_msg)
            err_str = f':{err_str}\n {err_msg[0:240]}'
        raise OperationalError(err_str) if retried else DatabaseError(err_str) from None

    def _raw_request(self,
                     data,
                     params: Dict[str, str],
                     headers: Optional[Dict[str, Any]] = None,
                     method: str = 'POST',
                     retries: int = 0,
                     stream: bool = False,
                     error_handler: Callable = None) -> HTTPResponse:
        if isinstance(data, str):
            data = data.encode()
        headers = dict_copy(self.headers, headers)
        url = f'{self.url}?{urlencode(dict_copy(self.params, params))}'
        attempts = 0
        while True:
            attempts += 1
            try:
                response: HTTPResponse = self.http.request(method, url,
                                                           headers=headers,
                                                           timeout=self.timeout,
                                                           body=data,
                                                           retries=self.http_retries,
                                                           preload_content=not stream)
            except HTTPError as ex:
                if isinstance(ex.__context__, ConnectionResetError):
                    # The server closed the connection, probably because the Keep Alive has expired
                    # We should be safe to retry, as ClickHouse should not have processed anything on a connection
                    # that it killed.  We also only retry this once, as multiple disconnects are unlikely to be
                    # related to the Keep Alive settings
                    if attempts == 1:
                        logger.debug('Retrying remotely closed connection')
                        continue
                logger.exception('Unexpected Http Driver Exception')
                raise OperationalError(f'Error executing HTTP request {self.url}') from ex
            if 200 <= response.status < 300:
                return response
            if response.status in (429, 503, 504):
                if attempts > retries:
                    self._error_handler(response, True)
                logger.debug('Retrying requests with status code %d', response.status)
            else:
                if error_handler:
                    error_handler(response)
                self._error_handler(response)

    def ping(self):
        """
        See BaseClient doc_string for this method
        """
        try:
            response = self.http.request('GET', f'{self.url}/ping', timeout=3)
            return 200 <= response.status < 300
        except HTTPError:
            logger.debug('ping failed', exc_info=True)
            return False

    def raw_query(self,
                  query: str,
                  parameters: Optional[Union[Sequence, Dict[str, Any]]] = None,
                  settings: Optional[Dict[str, Any]] = None,
                  fmt: str = None) -> bytes:
        """
        See BaseClient doc_string for this method
        """
        final_query, bind_params = bind_query(query, parameters, self.server_tz)
        if fmt:
            final_query += f'\n FORMAT {fmt}'
        params = self._validate_settings(settings or {})
        params.update(bind_params)
        return self._raw_request(final_query, params).data
