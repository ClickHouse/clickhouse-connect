"""The clickhouse-client command line, as far as tests/clickhouse-test and the .sql tests use it."""

from __future__ import annotations

import re
from dataclasses import dataclass, field

# The command-line settings that the shim sends to the server. tests/clickhouse-test also adds many
# randomized settings to the command line. The shim drops them: they only change performance-related
# paths in the server, and some of them are client-only.
SERVER_SETTINGS = frozenset({"log_comment", "max_insert_threads", "max_threads", "send_logs_level"})

USAGE = """usage: clickhouse-client [options]

A clickhouse-client imitation that runs queries through clickhouse-connect, so that
the ClickHouse tests/clickhouse-test runner can drive it. If --query is not
given, the queries are read from stdin. Every ";"-separated statement runs.

Options:
  -h, --host HOST          Server host (default: localhost)
      --port PORT          Server port. The native TCP port of the test runner
                           (CLICKHOUSE_PORT_TCP) maps to CLICKHOUSE_PORT_HTTP.
  -u, --user USER          User name (default: default)
      --password PASSWORD  Password (default: empty)
  -d, --database DB        Database (default: default)
  -q, --query SQL          Queries to run
  -s, --secure             Use TLS (HTTPS)
      --param_NAME=VALUE   Query parameter
      --SETTING=VALUE      log_comment, max_insert_threads, max_threads and
                           send_logs_level go to the server; other settings
                           are ignored
  -n, --multiquery         Ignored
  -m, --multiline          Ignored
      --help               Show this help

Environment variables:
  CLICKHOUSE_CLIENT_CLI_LOG        Log file for troubleshooting
"""


class ArgsError(Exception):
    """A command line that the shim cannot read."""


@dataclass
class Options:
    host: str = "localhost"
    port: int = 0  # 0 when not given
    user: str = "default"
    password: str = ""
    database: str = "default"
    secure: bool = False
    query: str | None = None
    help: bool = False
    # The server settings that go with each query, in the order of the command line.
    settings: dict[str, str] = field(default_factory=dict)
    # The query parameters from --param_<name>=<value>.
    params: dict[str, str] = field(default_factory=dict)


# (long name, short name, has an argument)
_KNOWN_OPTIONS = (
    ("host", "h", True),
    ("port", None, True),
    ("user", "u", True),
    ("password", None, True),
    ("database", "d", True),
    ("query", "q", True),
    ("secure", "s", False),
    ("no-secure", None, False),
    # As in clickhouse-client: -n is --multiquery, -m is --multiline. The shim always runs every
    # statement of the input.
    ("multiquery", "n", False),
    ("multi-query", None, False),
    ("multiline", "m", False),
    ("help", None, False),
    # Accepted and ignored.
    ("config-file", "C", True),
    ("client_logs_file", None, True),
    ("allow_repeated_settings", None, False),
    ("fake-drop", None, False),
)
_BY_LONG = {o[0]: o for o in _KNOWN_OPTIONS}
_BY_SHORT = {o[1]: o for o in _KNOWN_OPTIONS if o[1]}

_NEGATIVE_NUMBER = re.compile(r"^-(?:\d+\.?\d*|\.\d+)(?:[eE][+-]?\d+)?$")


def _set(opts: Options, name: str, value: str) -> None:
    if name == "host":
        opts.host = value
    elif name == "port":
        try:
            opts.port = int(value)
        except ValueError:
            raise ArgsError(f"bad --port value {value!r}") from None
    elif name == "user":
        opts.user = value
    elif name == "password":
        opts.password = value
    elif name == "database":
        opts.database = value
    elif name == "query":
        opts.query = value
    elif name == "secure":
        opts.secure = True
    elif name == "no-secure":
        opts.secure = False
    elif name == "help":
        opts.help = True


def parse_args(argv: list[str]) -> Options:
    opts = Options()
    i = 0
    while i < len(argv):
        arg = argv[i]
        i += 1
        if arg == "--":
            break
        if arg.startswith("--"):
            name, has_value, value = arg[2:].partition("=")
            if not name:
                continue
            known = _BY_LONG.get(name)
            if known is not None:
                if not known[2]:
                    _set(opts, known[0], "")
                    continue
                if not has_value:
                    if i >= len(argv):
                        raise ArgsError(f"option --{name} needs a value")
                    value = argv[i]
                    i += 1
                _set(opts, known[0], value)
                continue
            # Any other long option is a setting (--max_threads=1) or a query parameter
            # (--param_x=1). The value is optional.
            if not has_value and i < len(argv):
                nxt = argv[i]
                if not nxt.startswith("-") or _NEGATIVE_NUMBER.match(nxt):
                    value, has_value = nxt, "="
                    i += 1
            if not has_value:
                value = "1"
            if name.startswith("param_"):
                opts.params[name[len("param_") :]] = value
                continue
            setting = name.replace("-", "_")
            if setting in SERVER_SETTINGS:
                opts.settings[setting] = value
        elif arg.startswith("-") and len(arg) > 1:
            known = _BY_SHORT.get(arg[1:])
            if known is None:
                # Unknown short options are ignored, as the other language ports do.
                continue
            if not known[2]:
                _set(opts, known[0], "")
                continue
            if i >= len(argv):
                raise ArgsError(f"option {arg} needs a value")
            _set(opts, known[0], argv[i])
            i += 1
        # Positional arguments (such as `client` of `clickhouse client`) are ignored.
    return opts


def extract_from_config(argv: list[str]) -> str:
    """Answer `clickhouse extract-from-config --key <key>`, which tests/clickhouse-test runs to read the
    server config. Only listen_host has an answer; for other keys the output is empty, as for a key that
    is not in the config."""
    key = ""
    i = 0
    while i < len(argv):
        a = argv[i]
        if a == "--key" and i + 1 < len(argv):
            key = argv[i + 1]
            i += 1
        elif a.startswith("--key="):
            key = a[len("--key=") :]
        i += 1
    return "127.0.0.1\n" if key == "listen_host" else ""
