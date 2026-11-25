"""Recovery for client-managed settings that a readonly session rejects.

The client decides once, at connect time, which convenience settings to attach
to every HTTP request. If the session later becomes readonly
(``SET readonly = 1``), the server rejects those settings with error 164
(READONLY) because they would modify a setting in readonly mode. This module
identifies that situation so a transport can drop the offending setting and
replay the request.
"""

from __future__ import annotations

import re
from collections.abc import Mapping

READONLY_ERROR_CODE = "164"

_READONLY_REJECTION_RE = re.compile(r"Cannot modify '([^']+)' setting in readonly mode")

# Settings the client adds on its own. The value is what the client sends by default;
# None means any value is client generated. A setting the user chose explicitly with a
# different value is never dropped, so the user still sees the server's error.
CLIENT_MANAGED_SETTINGS: dict[str, str | None] = {
    "cast_string_to_dynamic_use_inference": "1",
    "cancel_http_readonly_queries_on_client_close": "1",
    "date_time_input_format": "best_effort",
    "enable_http_compression": "1",
    "send_progress_in_http_headers": "1",
    "http_headers_progress_interval_ms": None,
}


def rejected_client_setting(error_code: str | None, body: str, sent_params: Mapping[str, str]) -> str | None:
    """Return the client-managed setting a readonly session rejected, or None.

    :param error_code: Value of the X-ClickHouse-Exception-Code response header
    :param body: Decoded error response body
    :param sent_params: The query parameters that were sent with the failed request
    """
    if error_code != READONLY_ERROR_CODE:
        return None
    match = _READONLY_REJECTION_RE.search(body)
    if not match:
        return None
    key = match.group(1)
    if key not in CLIENT_MANAGED_SETTINGS or key not in sent_params:
        return None
    default = CLIENT_MANAGED_SETTINGS[key]
    if default is not None and str(sent_params[key]) != default:
        return None
    return key
