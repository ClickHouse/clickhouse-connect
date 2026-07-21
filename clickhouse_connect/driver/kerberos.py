import base64

from clickhouse_connect.driver.exceptions import OperationalError
from clickhouse_connect.driver.options import check_spnego


def check_kerberos():
    """Feature-named alias for check_spnego(), for callers that only care whether Kerberos
    support is available, not the underlying spnego module itself."""
    return check_spnego()


def negotiate_auth_header(hostname: str, service: str = "HTTP") -> str:
    """
    Build a SPNEGO/Kerberos 'Negotiate' Authorization header value for the given hostname.

    ClickHouse authenticates each HTTP request independently (there is no server side session
    that carries a partially completed handshake to the next request), so a fresh token must be
    generated and sent with every request rather than negotiated once and reused.
    """
    spnego = check_spnego()
    try:
        ctx = spnego.client(hostname=hostname, service=service)
        token = ctx.step()
    except spnego.exceptions.SpnegoError as e:
        # pyspnego's own message already explains the actual cause (missing system Kerberos support,
        # no valid ticket, etc.) via its context annotations, so it is surfaced as-is rather than guessed at.
        raise OperationalError(f"Kerberos negotiation failed: {e}") from e
    return "Negotiate " + base64.b64encode(token).decode()
