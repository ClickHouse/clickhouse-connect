"""The troubleshooting log of one invocation (CLICKHOUSE_CLIENT_CLI_LOG)."""

from __future__ import annotations

import datetime
import logging
import os

LOG_PATH_ENV = "CLICKHOUSE_CLIENT_CLI_LOG"


class InvocationLog:
    """Collects troubleshooting lines for one invocation and appends them to the file named by
    CLICKHOUSE_CLIENT_CLI_LOG. Without that variable, it does nothing. The lines are written in one write,
    so the lines of parallel invocations do not mix."""

    def __init__(self) -> None:
        self.path = os.environ.get(LOG_PATH_ENV, "")
        self.lines: list[str] = []
        self.add("=== clickhouse-client invocation", datetime.datetime.now(datetime.timezone.utc).isoformat())

    def add(self, key: str, value: str) -> None:
        if self.path:
            self.lines.append(f"{key}={value.replace(chr(10), chr(92) + 'n')}\n")

    def flush(self) -> None:
        # A log that cannot be written must not change the result of a test, so a write error is
        # ignored; the missing lines in the log show it.
        if not self.path:
            return
        try:
            with open(self.path, "a", encoding="utf-8", errors="backslashreplace") as f:
                f.write("".join(self.lines))
        except OSError:
            pass


class LogHandler(logging.Handler):
    """Sends the log records of clickhouse-connect (and Python warnings) to the invocation log. Without it,
    Python writes warnings to stderr, and tests/clickhouse-test fails a .sql test that writes to stderr."""

    def __init__(self, log: InvocationLog):
        super().__init__(logging.DEBUG)
        self.log = log

    def emit(self, record: logging.LogRecord) -> None:
        try:
            self.log.add(f"log.{record.levelname.lower()}", f"{record.name}: {record.getMessage()}")
        except Exception:  # noqa: BLE001 - logging must never fail the statement
            pass
