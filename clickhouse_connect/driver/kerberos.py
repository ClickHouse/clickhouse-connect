import base64
import binascii
from typing import Protocol, cast

from clickhouse_connect.driver.exceptions import OperationalError
from clickhouse_connect.driver.options import check_spnego


class _SpnegoContext(Protocol):
    @property
    def complete(self) -> bool: ...

    def step(self, in_token: bytes | None = None) -> bytes | None: ...


class _SpnegoExceptions(Protocol):
    SpnegoError: type[Exception]


class _SpnegoModule(Protocol):
    exceptions: _SpnegoExceptions

    def client(self, *, hostname: str, service: str, protocol: str) -> _SpnegoContext: ...


def check_kerberos() -> _SpnegoModule:
    """Return pyspnego when Kerberos support is installed."""
    return cast(_SpnegoModule, check_spnego())


class KerberosAuthContext:
    """Kerberos client context for one HTTP request attempt."""

    def __init__(self, hostname: str, service: str = "HTTP") -> None:
        self._spnego = check_kerberos()
        try:
            self._context = cast(
                _SpnegoContext,
                self._spnego.client(hostname=hostname, service=service, protocol="kerberos"),
            )
            token = self._context.step()
        except (self._spnego.exceptions.SpnegoError, ImportError) as ex:
            raise OperationalError(f"Kerberos negotiation failed: {ex}") from ex
        if not token:
            raise OperationalError("Kerberos negotiation failed: no client authentication token was produced")
        self.authorization_header = "Negotiate " + base64.b64encode(token).decode()

    def validate_response(self, authenticate_header: str | None) -> None:
        """Consume the server AP-REP token and require mutual authentication."""
        if authenticate_header is None:
            raise OperationalError("Kerberos mutual authentication failed: successful response is missing the WWW-Authenticate header")

        scheme, separator, encoded_token = authenticate_header.partition(" ")
        encoded_token = encoded_token.strip()
        if scheme.lower() != "negotiate" or not separator or not encoded_token:
            raise OperationalError("Kerberos mutual authentication failed: WWW-Authenticate must contain a Negotiate response token")
        try:
            token = base64.b64decode(encoded_token, validate=True)
        except (binascii.Error, ValueError) as ex:
            raise OperationalError(
                "Kerberos mutual authentication failed: WWW-Authenticate contains an invalid Negotiate response token"
            ) from ex
        if not token:
            raise OperationalError("Kerberos mutual authentication failed: WWW-Authenticate contains an empty Negotiate response token")
        try:
            self._context.step(token)
        except self._spnego.exceptions.SpnegoError as ex:
            raise OperationalError(f"Kerberos mutual authentication failed: {ex}") from ex
        if not self._context.complete:
            raise OperationalError("Kerberos mutual authentication failed: the server response did not complete the context")
