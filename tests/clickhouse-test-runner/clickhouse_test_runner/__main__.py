"""Imitate clickhouse-client well enough for the ClickHouse core test runner (tests/clickhouse-test) to drive
it, and run every query through clickhouse-connect.

See ../README.md for how to run the upstream SQL tests with it.
"""

from __future__ import annotations

import logging
import sys
import traceback
from typing import BinaryIO, TextIO

from .args import USAGE, ArgsError, extract_from_config, parse_args
from .log import InvocationLog, LogHandler


def main(argv: list[str], stdin: BinaryIO, stdout: BinaryIO, stderr: TextIO) -> int:
    if argv and argv[0] == "extract-from-config":
        stdout.write(extract_from_config(argv[1:]).encode())
        return 0

    log = InvocationLog()
    try:
        return _run(argv, stdin, stdout, stderr, log)
    finally:
        log.flush()


def _run(argv: list[str], stdin: BinaryIO, stdout: BinaryIO, stderr: TextIO, log: InvocationLog) -> int:
    log.add("argv", " ".join(argv))
    try:
        opts = parse_args(argv)
    except ArgsError as ex:
        stderr.write(f"Error: {ex}\n\n{USAGE}")
        return 1
    if opts.help:
        stdout.write(USAGE.encode())
        return 0

    if opts.query is not None:
        text = opts.query
    else:
        # Test files can hold bytes that are not UTF-8. They stay as surrogate escapes; clickhouse-connect
        # takes the query as str.
        text = stdin.read().decode("utf-8", "surrogateescape")
    if not text.strip():
        stderr.write("Error: no query. Use --query or write the queries to stdin.\n")
        return 1

    # Without a handler, Python writes the warnings of clickhouse-connect to stderr, and the test runner
    # fails a .sql test that writes to stderr. They go to the troubleshooting log instead.
    root = logging.getLogger()
    handler = LogHandler(log)
    root.addHandler(handler)
    root.setLevel(logging.INFO)
    logging.captureWarnings(True)
    try:
        return _run_statements(opts, text, stdout, stderr, log)
    finally:
        root.removeHandler(handler)
        logging.captureWarnings(False)


def _run_statements(opts, text: str, stdout: BinaryIO, stderr: TextIO, log: InvocationLog) -> int:

    # Imported here: extract-from-config and --help do not need the client.
    from . import runner as runner_mod
    from .split import split_statements

    statements = split_statements(text)
    log.add("statements", str(len(statements)))
    if not statements:
        return 0

    runner = runner_mod.Runner(opts, stdout, log)
    try:
        runner.run(statements)
    except Exception as ex:  # noqa: BLE001 - every error stops the program, as in clickhouse-client
        log.add("error", f"{type(ex).__name__}: {ex}")
        log.add("traceback", traceback.format_exc())
        return report_error(stderr, ex, runner_mod)
    finally:
        try:
            runner.close()
        except Exception as ex:  # noqa: BLE001 - the result of the statements is already written
            log.add("close_error", repr(ex))
    return 0


def report_error(stderr: TextIO, ex: BaseException, runner_mod) -> int:
    """Write the error to stderr and return the exit code. As in clickhouse-client, the exit code is the error
    code modulo 256 when it is a server error."""
    code = 0
    if isinstance(ex, runner_mod.HintError):
        stderr.write(f"Error: {ex}\n")
        code = ex.code
    else:
        server_code = runner_mod.error_code(ex)
        if server_code is not None:
            stderr.write(f"Received exception from server:\n{runner_mod.server_message(ex)}\n")
            code = server_code
        else:
            stderr.write(f"Error: {type(ex).__name__}: {ex}\n")
    return code % 256 if code % 256 else 1


def entry() -> None:
    sys.exit(main(sys.argv[1:], sys.stdin.buffer, sys.stdout.buffer, sys.stderr))


if __name__ == "__main__":
    entry()
