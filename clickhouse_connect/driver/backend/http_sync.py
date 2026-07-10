"""Synchronous HTTP transport backend built on urllib3.

Owns request execution, retry and auth-refresh policy, pool lifecycle, ping,
and HTTP error handling. During the 1.x transition the headers and params
dicts are shared by reference with the HttpClient facade, which must mutate
them in place rather than rebinding.
"""

from __future__ import annotations

import logging
import time
import uuid
from collections.abc import Callable
from typing import Any, cast
from urllib.parse import urlencode

from urllib3 import Timeout
from urllib3.exceptions import HTTPError
from urllib3.poolmanager import PoolManager
from urllib3.response import HTTPResponse

from clickhouse_connect.driver.backend.httpcommon import (
    auth_failed_ex_code,
    build_http_error,
    ex_header,
    retryable_http_statuses,
)
from clickhouse_connect.driver.common import dict_copy
from clickhouse_connect.driver.exceptions import OperationalError, ProgrammingError
from clickhouse_connect.driver.httputil import all_managers, check_conn_expiration, get_response_data

logger = logging.getLogger(__name__)

_REMOTE_CLOSE_ERRORS = (ConnectionResetError, BrokenPipeError)


class HttpSyncBackend:
    def __init__(
        self,
        *,
        url: str,
        pool_manager: PoolManager,
        owns_pool_manager: bool,
        headers: dict[str, str],
        params: dict[str, str],
        timeout: Timeout,
        server_host_name: str | None,
        token_provider: Callable[[], str] | None,
        autogenerate_query_id: bool,
        http_retries: int = 1,
    ):
        self.url = url
        self.http = pool_manager
        self.owns_pool_manager = owns_pool_manager
        self.headers = headers
        self.params = params
        self.timeout = timeout
        self.server_host_name = server_host_name
        self.token_provider = token_provider
        self.autogenerate_query_id = autogenerate_query_id
        self.http_retries = http_retries
        self.show_clickhouse_errors = True
        self.send_progress: bool | None = None
        self.progress_interval: str | None = None
        self._active_session: str | None = None

    def set_access_token(self, access_token: str) -> None:
        auth_header = self.headers.get("Authorization")
        if auth_header and not auth_header.startswith("Bearer"):
            raise ProgrammingError("Cannot set access token when a different auth type is used")
        self.headers["Authorization"] = f"Bearer {access_token}"

    def error_handler(self, response: HTTPResponse, retried: bool = False) -> None:
        """
        Handles HTTP errors. Tries to be robust and provide maximum context.
        """
        try:
            full_body = ""
            try:
                full_body = get_response_data(response).decode(errors="backslashreplace")
            except Exception:
                logger.warning("Failed to read error response body", exc_info=True)
        finally:
            response.close()
        raise build_http_error(
            response.status,
            response.headers.get(ex_header),
            full_body,
            self.show_clickhouse_errors,
            self.url,
            retried,
        ) from None

    def request(
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
        if isinstance(data, str):
            data = data.encode()
        headers = dict_copy(self.headers, headers)
        attempts = 0
        auth_retried = False
        final_params = {}
        if server_wait:
            final_params["wait_end_of_query"] = "1"
        # We can't actually read the progress headers, but we enable them so ClickHouse sends something
        # to keep the connection alive when waiting for long-running queries and (2) to get summary information
        # if not streaming
        if self.send_progress:
            final_params["send_progress_in_http_headers"] = "1"
        if self.progress_interval:
            final_params["http_headers_progress_interval_ms"] = self.progress_interval
        final_params = dict_copy(self.params, final_params)
        final_params = dict_copy(final_params, params)

        if self.autogenerate_query_id and "query_id" not in final_params:
            final_params["query_id"] = str(uuid.uuid4())

        url = f"{self.url}?{urlencode(final_params)}"
        kwargs: dict[str, Any] = {"headers": headers, "timeout": self.timeout, "retries": self.http_retries, "preload_content": not stream}
        if self.server_host_name:
            kwargs["assert_same_host"] = False
            kwargs["headers"].update({"Host": self.server_host_name})
        if fields:
            kwargs["fields"] = fields
        else:
            kwargs["body"] = data
        check_conn_expiration(cast(PoolManager, self.http))
        query_session = final_params.get("session_id")
        while True:
            attempts += 1
            if query_session:
                if query_session == self._active_session:
                    raise ProgrammingError(
                        "Attempt to execute concurrent queries within the same session. "
                        + "Please use a separate client instance per thread/process."
                    )
                # There is a race condition here when using multiprocessing -- in that case the server will
                # throw an error instead, but in most cases this more helpful error will be thrown first
                self._active_session = query_session
            try:
                response: HTTPResponse = cast(HTTPResponse, cast(PoolManager, self.http).request(method, url, **kwargs))
            except HTTPError as ex:
                # Always allow at least one retry on a clean connection error so a single stale
                # keep-alive socket doesn't surface to the caller, and additionally honor the
                # retries budget when it is larger (e.g. query_retries for reads), so that
                # bursts of stale pooled connections can be drained before giving up.
                max_attempts = max(2, retries + 1)
                remote_close = isinstance(ex.__context__, _REMOTE_CLOSE_ERRORS) or isinstance(ex.__cause__, _REMOTE_CLOSE_ERRORS)
                if remote_close and attempts < max_attempts:
                    # The server closed the connection, probably because the Keep Alive has expired.
                    # We should be safe to retry, as ClickHouse should not have processed anything on
                    # a connection that it killed.
                    body = kwargs.get("body")
                    if retry_body is not None:
                        kwargs["body"] = retry_body()
                        logger.debug("Retrying remotely closed connection with rebuilt body (attempt %s/%s)", attempts, max_attempts)
                        time.sleep(0.1 * attempts)
                        continue
                    if body is None or isinstance(body, (bytes, bytearray, str)):
                        logger.debug("Retrying remotely closed connection (attempt %s/%s)", attempts, max_attempts)
                        time.sleep(0.1 * attempts)
                        continue
                logger.debug("Non-retryable HTTP transport error type=%s", type(ex).__name__)
                logger.warning("Unexpected Http Driver Exception")
                err_url = f" ({self.url})" if self.show_clickhouse_errors else ""
                raise OperationalError(f"Error {ex} executing HTTP request attempt {attempts}{err_url}") from ex
            finally:
                if query_session:
                    self._active_session = None  # Make sure we always clear this
            if 200 <= response.status < 300 and not response.headers.get(ex_header):
                return response
            if response.status in retryable_http_statuses:
                if attempts > retries:
                    self.error_handler(response, True)
                logger.debug("Retrying requests with status code %d", response.status)
            elif self.token_provider and not auth_retried and response.headers.get(ex_header) == auth_failed_ex_code:
                body = kwargs.get("body")
                if retry_body is None and not (body is None or isinstance(body, (bytes, bytearray, str))):
                    self.error_handler(response)  # non-replayable body, surface the auth error instead of retrying
                auth_retried = True
                self.set_access_token(self.token_provider())
                headers["Authorization"] = self.headers["Authorization"]
                if retry_body is not None:
                    kwargs["body"] = retry_body()
                response.close()
                logger.debug("Refreshing access token after authentication failure")
            elif error_handler is not None:
                error_handler(response)
            else:
                self.error_handler(response)

    def ping(self) -> bool:
        try:
            headers = dict_copy(self.headers)
            kwargs: dict[str, Any] = {"headers": headers, "timeout": 3, "preload_content": True}
            if self.server_host_name:
                kwargs["assert_same_host"] = False
                headers["Host"] = self.server_host_name
            response = cast(PoolManager, self.http).request("GET", f"{self.url}/ping", **kwargs)
            return 200 <= response.status < 300
        except HTTPError:
            logger.debug("ping failed", exc_info=True)
            return False

    def close_connections(self) -> None:
        cast(PoolManager, self.http).clear()

    def close(self) -> None:
        if self.owns_pool_manager:
            cast(PoolManager, self.http).clear()
            all_managers.pop(cast(PoolManager, self.http), None)
