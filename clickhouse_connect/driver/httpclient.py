import io
import logging
import uuid
from base64 import b64encode
from collections.abc import Callable, Generator, Sequence
from typing import Any, BinaryIO, cast

from urllib3 import Timeout
from urllib3.poolmanager import PoolManager
from urllib3.response import HTTPResponse

from clickhouse_connect import common
from clickhouse_connect.driver.backend.http_sync import HttpSyncBackend
from clickhouse_connect.driver.backend.httpcommon import (
    add_integration_tag,
    apply_http_server_settings,
    auth_failed_ex_code,  # noqa: F401  (compatibility re-export)
    columns_only_re,  # noqa: F401  (compatibility re-export)
    embed_insert_query,
    ex_header,  # noqa: F401  (compatibility re-export)
    ex_tag_header,  # noqa: F401  (compatibility re-export)
    negotiate_compression,
    parse_command_body,
    summary_from_headers,
)
from clickhouse_connect.driver.backend.models import QueryRuntime
from clickhouse_connect.driver.binding import bind_query, use_form_encoding
from clickhouse_connect.driver.client import Client
from clickhouse_connect.driver.common import coerce_bool, coerce_int, dict_add, dict_copy
from clickhouse_connect.driver.ctypes import RespBuffCls
from clickhouse_connect.driver.exceptions import ProgrammingError
from clickhouse_connect.driver.external import ExternalData
from clickhouse_connect.driver.httputil import (
    ResponseSource,  # noqa: F401  (compatibility re-export)
    check_env_proxy,
    default_pool_manager,
    get_pool_manager,
    get_proxy_manager,
)
from clickhouse_connect.driver.insert import InsertContext
from clickhouse_connect.driver.query import QueryContext, QueryResult, TzMode, TzSource, returns_empty_string_on_empty_body
from clickhouse_connect.driver.summary import QuerySummary
from clickhouse_connect.driver.transform import NativeTransform

logger = logging.getLogger(__name__)


class HttpClient(Client):
    params: dict[str, str] = {}
    valid_transport_settings = {
        "database",
        "buffer_size",
        "session_id",
        "compress",
        "decompress",
        "session_timeout",
        "session_check",
        "query_id",
        "quota_key",
        "wait_end_of_query",
        "client_protocol_version",
        "role",
    }
    optional_transport_settings = {"send_progress_in_http_headers", "http_headers_progress_interval_ms", "enable_http_compression"}
    _owns_pool_manager = False

    # R0917: too-many-positional-arguments

    def __init__(
        self,
        interface: str,
        host: str,
        port: int,
        username: str,
        password: str,
        database: str | None,
        access_token: str | None = None,
        token_provider: Callable[[], str] | None = None,
        compress: bool | str = True,
        query_limit: int = 0,
        query_retries: int = 2,
        connect_timeout: int = 10,
        send_receive_timeout: int = 300,
        client_name: str | None = None,
        verify: bool | str = True,
        ca_cert: str | None = None,
        client_cert: str | None = None,
        client_cert_key: str | None = None,
        session_id: str | None = None,
        settings: dict[str, Any] | None = None,
        pool_mgr: PoolManager | None = None,
        http_proxy: str | None = None,
        https_proxy: str | None = None,
        server_host_name: str | None = None,
        tz_source: TzSource | None = None,
        tz_mode: str | None = None,
        show_clickhouse_errors: bool | None = None,
        autogenerate_session_id: bool | None = None,
        autogenerate_query_id: bool | None = None,
        tls_mode: str | None = None,
        proxy_path: str = "",
        form_encode_query_params: bool = False,
        rename_response_column: str | None = None,
        headers: dict[str, str] | None = None,
    ):
        """
        Create an HTTP ClickHouse Connect client
        See clickhouse_connect.get_client for parameters
        """
        proxy_path = proxy_path.lstrip("/")
        if proxy_path:
            proxy_path = "/" + proxy_path
        self.url = f"{interface}://{host}:{port}{proxy_path}"
        client_headers: dict[str, str] = {}
        self.params = dict_copy(HttpClient.params)
        ch_settings = dict_copy(settings, self.params)
        pool = pool_mgr
        if interface == "https":
            if isinstance(verify, str) and verify.lower() == "proxy":
                verify = True
                tls_mode = tls_mode or "proxy"
            if not https_proxy:
                https_proxy = check_env_proxy("https", host, port)
            verify = coerce_bool(verify)
            if client_cert and (tls_mode is None or tls_mode == "mutual"):
                if not username:
                    raise ProgrammingError("username parameter is required for Mutual TLS authentication")
                client_headers["X-ClickHouse-User"] = username
                client_headers["X-ClickHouse-SSL-Certificate-Auth"] = "on"

            if not pool and (server_host_name or ca_cert or client_cert or not verify or https_proxy):
                options: dict[str, Any] = {"verify": verify}
                dict_add(options, "ca_cert", ca_cert)
                dict_add(options, "client_cert", client_cert)
                dict_add(options, "client_cert_key", client_cert_key)
                if server_host_name:
                    if options["verify"]:
                        options["assert_hostname"] = server_host_name
                    options["server_hostname"] = server_host_name
                pool = get_pool_manager(https_proxy=https_proxy, **options)
                self._owns_pool_manager = True
        if not pool:
            if not http_proxy:
                http_proxy = check_env_proxy("http", host, port)
            if http_proxy:
                pool = get_proxy_manager(host, http_proxy)
            else:
                pool = default_pool_manager()

        if token_provider:
            access_token = token_provider()
        if access_token:
            client_headers["Authorization"] = f"Bearer {access_token}"
        elif (not client_cert or tls_mode in ("strict", "proxy")) and username:
            client_headers["Authorization"] = "Basic " + b64encode(f"{username}:{password}".encode()).decode()

        self._reported_libs: set[str] = set()
        client_headers["User-Agent"] = common.build_client_name(client_name)
        if headers:
            client_headers.update(headers)
        self._write_format = "Native"
        self._transform = NativeTransform()

        # There are use cases when the client needs to disable timeouts.
        if connect_timeout is not None:
            connect_timeout = coerce_int(connect_timeout)
        if send_receive_timeout is not None:
            send_receive_timeout = coerce_int(send_receive_timeout)
        self._rename_response_column = rename_response_column

        # allow to override the global autogenerate_session_id setting via the constructor params
        _autogenerate_session_id = (
            common.get_setting("autogenerate_session_id") if autogenerate_session_id is None else autogenerate_session_id
        )

        if session_id:
            ch_settings["session_id"] = session_id
        elif "session_id" not in ch_settings and _autogenerate_session_id:
            ch_settings["session_id"] = str(uuid.uuid4())

        compression, write_compression = negotiate_compression(compress)
        if write_compression:
            self.write_compression = write_compression

        # The backend owns transport state. The params dict is shared by
        # reference with this facade, so it is mutated in place, never rebound.
        self._backend = HttpSyncBackend(
            url=self.url,
            pool_manager=pool,
            owns_pool_manager=self._owns_pool_manager,
            headers=client_headers,
            params=self.params,
            timeout=Timeout(connect=connect_timeout, read=send_receive_timeout),
            server_host_name=server_host_name,
            token_provider=token_provider,
            # allow to override the global autogenerate_query_id setting via the constructor params
            autogenerate_query_id=(common.get_setting("autogenerate_query_id") if autogenerate_query_id is None else autogenerate_query_id),
            read_format="Native",
            form_encode_query_params=form_encode_query_params,
        )
        self._initial_settings = settings
        # Stashed for _init_common_settings, which needs the discovered server
        # settings and so runs as part of the connect step inside super().__init__
        self._ch_settings = ch_settings
        self._negotiated_compression = compression
        self._send_receive_timeout = send_receive_timeout
        super().__init__(
            database=database,
            uri=self.url,
            query_limit=query_limit,
            query_retries=query_retries,
            server_host_name=server_host_name,
            tz_source=tz_source,
            tz_mode=cast(TzMode | None, tz_mode),
            show_clickhouse_errors=show_clickhouse_errors,
            autoconnect=True,
        )

    def _init_common_settings(self, tz_source: TzSource) -> None:
        super()._init_common_settings(tz_source)
        self.params.update(self._validate_settings(self._ch_settings))
        apply_http_server_settings(self, self._backend, self._negotiated_compression, self._send_receive_timeout)

    @property
    def http(self) -> PoolManager:
        return cast(PoolManager, self._backend.http)

    @http.setter
    def http(self, pool_manager: PoolManager) -> None:
        self._backend.http = pool_manager

    @property
    def headers(self) -> dict[str, str]:
        return self._backend.headers

    @headers.setter
    def headers(self, value: dict[str, str]) -> None:
        self._backend.headers = value

    @property
    def timeout(self) -> Timeout:
        return self._backend.timeout

    @timeout.setter
    def timeout(self, value: Timeout) -> None:
        self._backend.timeout = value

    @property
    def http_retries(self) -> int:
        return self._backend.http_retries

    @http_retries.setter
    def http_retries(self, value: int) -> None:
        self._backend.http_retries = value

    @property
    def show_clickhouse_errors(self) -> bool:  # type: ignore[override]
        return self._backend.show_clickhouse_errors

    @show_clickhouse_errors.setter
    def show_clickhouse_errors(self, value: bool) -> None:
        self._backend.show_clickhouse_errors = value

    @property
    def _autogenerate_query_id(self) -> bool:
        return self._backend.autogenerate_query_id

    @_autogenerate_query_id.setter
    def _autogenerate_query_id(self, value: bool) -> None:
        self._backend.autogenerate_query_id = value

    @property
    def _token_provider(self) -> Callable[[], str] | None:
        return self._backend.token_provider

    @property
    def form_encode_query_params(self) -> bool:
        return self._backend.form_encode_query_params

    @form_encode_query_params.setter
    def form_encode_query_params(self, value: bool) -> None:
        self._backend.form_encode_query_params = value

    @property
    def _read_format(self) -> str:
        return self._backend.read_format

    @_read_format.setter
    def _read_format(self, value: str) -> None:
        self._backend.read_format = value

    @property
    def compression(self) -> str | None:  # type: ignore[override]
        return self._backend.compression

    @compression.setter
    def compression(self, value: str | None) -> None:
        self._backend.compression = value

    def set_client_setting(self, key: str, value: Any) -> None:
        str_value = self._validate_setting(key, value, common.get_setting("invalid_setting_action"))
        if str_value is not None:
            self.params[key] = str_value

    def get_client_setting(self, key: str) -> str | None:
        return self.params.get(key)

    def set_access_token(self, access_token: str) -> None:
        self._backend.set_access_token(access_token)

    def _query_with_context(self, context: QueryContext) -> QueryResult:
        context.rename_response_column = self._rename_response_column
        if self.protocol_version:
            context.block_info = True
        runtime = QueryRuntime(
            database=self.database,
            protocol_version=self.protocol_version,
            settings=self._validate_settings(context.settings),
            retries=self.query_retries,
        )
        execution = self._backend.execute_query(context, runtime, self._prep_query(context))
        if execution.columns is not None:
            return self._columns_only_result(context, execution.columns)
        byte_source = RespBuffCls(execution.source)
        response_tz = self._check_tz_change(execution.response_tz_name)
        if response_tz is not None:
            context.set_response_tz(response_tz)
        query_result = self._transform.parse_response(byte_source, context)
        query_result.summary = execution.summary
        return cast(QueryResult, query_result)

    def data_insert(self, context: InsertContext) -> QuerySummary:
        """
        See BaseClient doc_string for this method
        """
        if context.empty:
            logger.debug("No data included in insert, skipping")
            return QuerySummary()

        def error_handler(resp: HTTPResponse):
            # If we actually had a local exception when building the insert, throw that instead
            if context.insert_exception:
                ex = context.insert_exception
                context.insert_exception = None
                raise ex
            self._error_handler(resp)

        headers = {"Content-Type": "application/octet-stream"}
        if context.compression is None:
            context.compression = self.write_compression
        if isinstance(context.compression, str):
            headers["Content-Encoding"] = context.compression
        block_gen = self._transform.build_insert(context)

        def rebuild_block_gen():
            context.current_row = 0
            context.current_block = 0
            return self._transform.build_insert(context)

        params = {}
        if self.database:
            params["database"] = self.database
        params.update(self._validate_settings(context.settings))
        headers = dict_copy(headers, context.transport_settings)
        try:
            response = self._raw_request(
                block_gen,
                params,
                headers,
                error_handler=error_handler,
                server_wait=False,
                retry_body=rebuild_block_gen,
            )
            logger.debug("Context insert response code: %d, content: %s", response.status, response.data)
            return QuerySummary(self._summary(response))
        finally:
            context.data = None

    def raw_insert(
        self,
        table: str | None = None,
        column_names: Sequence[str] | None = None,
        insert_block: str | bytes | Generator[bytes, None, None] | BinaryIO | None = None,
        settings: dict | None = None,
        fmt: str | None = None,
        compression: str | None = None,
        transport_settings: dict[str, str] | None = None,
    ) -> QuerySummary:
        """
        See BaseClient doc_string for this method
        """
        params = {}
        headers = {"Content-Type": "application/octet-stream"}
        if compression:
            headers["Content-Encoding"] = compression
        if table:
            insert_block, query_param = embed_insert_query(
                table, column_names, fmt if fmt else self._write_format, compression, insert_block
            )
            if query_param:
                params["query"] = query_param
        if self.database:
            params["database"] = self.database
        params.update(self._validate_settings(settings or {}))
        headers = dict_copy(headers, transport_settings)
        response = self._raw_request(insert_block, params, headers, server_wait=False)
        logger.debug("Raw insert response code: %d, content: %s", response.status, response.data)
        return QuerySummary(self._summary(response))

    @staticmethod
    def _summary(response: HTTPResponse):
        return summary_from_headers(response.headers)

    def command(
        self,
        cmd: str,
        parameters: Sequence | dict[str, Any] | None = None,
        data: str | bytes | None = None,
        settings: dict | None = None,
        use_database: bool = True,
        external_data: ExternalData | None = None,
        transport_settings: dict[str, str] | None = None,
    ) -> str | int | Sequence[str] | QuerySummary:
        """
        See BaseClient doc_string for this method
        """
        bound_cmd, bind_params = bind_query(cmd, parameters, self.server_tz)
        runtime = QueryRuntime(
            database=self.database if use_database else None,
            settings=self._validate_settings(settings or {}),
        )
        execution = self._backend.execute_command(bound_cmd, bind_params, data, external_data, runtime, transport_settings)
        if execution.body:
            return parse_command_body(execution.body)
        if returns_empty_string_on_empty_body(bound_cmd):
            return ""
        return QuerySummary(execution.summary)

    def _error_handler(self, response: HTTPResponse, retried: bool = False) -> None:
        self._backend.error_handler(response, retried)

    def _raw_request(
        self,
        data,
        params: dict[str, str],
        headers: dict[str, Any] | None = None,
        method: str = "POST",
        retries: int = 0,
        stream: bool = False,
        server_wait: bool = True,
        fields: dict[str, tuple] | None = None,
        error_handler: Callable | None = None,
        retry_body: Callable[[], Any] | None = None,
    ) -> HTTPResponse:
        return self._backend.request(
            data,
            params,
            headers=headers,
            method=method,
            retries=retries,
            stream=stream,
            server_wait=server_wait,
            fields=fields,
            error_handler=error_handler,
            retry_body=retry_body,
        )

    def raw_query(
        self,
        query: str,
        parameters: Sequence | dict[str, Any] | None = None,
        settings: dict[str, Any] | None = None,
        fmt: str | None = None,
        use_database: bool = True,
        external_data: ExternalData | None = None,
        transport_settings: dict[str, str] | None = None,
    ) -> bytes:
        """
        See BaseClient doc_string for this method
        """
        body, params, fields = self._prep_raw_query(query, parameters, settings, fmt, use_database, external_data)
        return self._raw_request(body, params, fields=fields, headers=transport_settings, retries=self.query_retries).data

    def raw_stream(
        self,
        query: str,
        parameters: Sequence | dict[str, Any] | None = None,
        settings: dict[str, Any] | None = None,
        fmt: str | None = None,
        use_database: bool = True,
        external_data: ExternalData | None = None,
        transport_settings: dict[str, str] | None = None,
    ) -> io.IOBase:
        """
        See BaseClient doc_string for this method
        """
        body, params, fields = self._prep_raw_query(query, parameters, settings, fmt, use_database, external_data)
        return self._raw_request(
            body,
            params,
            fields=fields,
            stream=True,
            server_wait=False,
            headers=transport_settings,
            retries=self.query_retries,
        )

    def _prep_raw_query(
        self,
        query: str,
        parameters: Sequence | dict[str, Any] | None,
        settings: dict[str, Any] | None,
        fmt: str | None,
        use_database: bool,
        external_data: ExternalData | None,
    ):
        if fmt:
            query += f"\n FORMAT {fmt}"
        final_query, bind_params = bind_query(query, parameters, self.server_tz)
        params = self._validate_settings(settings or {})
        if use_database and self.database:
            params["database"] = self.database
        form_fields: dict[str, Any] = {}
        fields: dict[str, Any] | None = form_fields
        use_form = use_form_encoding(final_query, bind_params, self.form_encode_query_params)
        # Setup query body
        if external_data and not use_form and isinstance(final_query, bytes):
            raise ProgrammingError("Binary query cannot be placed in URL when using External Data; enable form encoding.")
        # Setup additional query parameters and body
        body: str | bytes = b""
        if use_form:
            form_fields["query"] = final_query
            form_fields.update(bind_params)
            if external_data:
                params.update(external_data.query_params)
                form_fields.update(external_data.form_data)
        elif external_data:
            params.update(bind_params)
            # Guaranteed str: the check above raises if external_data and not use_form and bytes
            assert isinstance(final_query, str)
            params["query"] = final_query
            params.update(external_data.query_params)
            fields = external_data.form_data
        else:
            params.update(bind_params)
            body = final_query
            fields = None
        return body, params, fields

    def _add_integration_tag(self, name: str):
        """
        Dynamically adds a product (like pandas or sqlalchemy) to the User-Agent string details section.
        """
        add_integration_tag(self.headers, self._reported_libs, name)

    def ping(self) -> bool:
        """
        See BaseClient doc_string for this method
        """
        return self._backend.ping()

    def close_connections(self) -> None:
        self._backend.close_connections()

    def close(self) -> None:
        self._backend.close()
