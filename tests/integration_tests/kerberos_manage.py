"""Stands up (and tears down) a real Kerberos KDC and Kerberos-configured ClickHouse instance
(the "kerberos_kdc" and "kerberos_clickhouse" services in docker-compose.yml) for
test_kerberos.py. See "Run the Kerberos integration tests" in CONTRIBUTING.md for prerequisites.

Used automatically by test_kerberos.py's kerberos_env fixture when CLICKHOUSE_CONNECT_TEST_KERBEROS
is set. Can also be run by hand for manual/exploratory use:

    python -m tests.integration_tests.kerberos_manage setup
    python -m tests.integration_tests.kerberos_manage teardown
"""

import os
import subprocess
import sys
import tempfile
import time

from tests.helpers import PROJECT_ROOT_DIR

COMPOSE_PROJECT_NAME = "clickhouse-connect-kerberos"
KDC_CONTAINER = f"{COMPOSE_PROJECT_NAME}-kdc"
LOCAL_KEYTAB_PATH = os.path.join(tempfile.gettempdir(), "kuser.keytab")
CONTAINER_KEYTAB_PATH = "/tmp/keytab/kuser.keytab"
KRB5_CONFIG_PATH = str(PROJECT_ROOT_DIR / "tests/integration_tests/kerberos_conf/kerberos_client_krb5.conf")
CLICKHOUSE_HOST = "server1.clickhouse.test"
CLICKHOUSE_PORT = 8124


def _compose_env():
    env = os.environ.copy()
    env["COMPOSE_PROJECT_NAME"] = COMPOSE_PROJECT_NAME
    return env


def _compose(*args):
    compose_file = str(PROJECT_ROOT_DIR / "docker-compose.yml")
    subprocess.run(["docker", "compose", "-f", compose_file, "--profile", "kerberos", *args], env=_compose_env(), check=True)


def setup():
    _compose("up", "-d", "--wait", "kerberos_kdc", "kerberos_clickhouse")

    for _ in range(30):
        result = subprocess.run(
            ["docker", "exec", KDC_CONTAINER, "test", "-f", CONTAINER_KEYTAB_PATH],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        if result.returncode == 0:
            break
        time.sleep(1)
    else:
        raise RuntimeError(f"The KDC did not provision keytabs within 30s. Check 'docker logs {KDC_CONTAINER}' for what went wrong.")

    subprocess.run(["docker", "cp", f"{KDC_CONTAINER}:{CONTAINER_KEYTAB_PATH}", LOCAL_KEYTAB_PATH], check=True)

    os.environ["KRB5_CONFIG"] = KRB5_CONFIG_PATH
    subprocess.run(["kinit", "-k", "-t", LOCAL_KEYTAB_PATH, "kuser@TEST.CLICKHOUSE.TECH"], check=True)

    _wait_for_kerberos_auth()


def _wait_for_kerberos_auth():
    # docker compose --wait only confirms the container's own healthcheck (the HTTP port
    # responding), which can pass a moment before ClickHouse's Kerberos acceptor context is fully
    # ready -- causing the very first real auth attempt to occasionally fail. Retrying an actual
    # negotiated request here (via curl, already relying on the kinit ticket obtained above, same
    # as kinit itself relies on external krb5 tooling) avoids handing pytest a cold first attempt.
    url = f"http://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/?query=SELECT+1"
    deadline = time.monotonic() + 10
    while True:
        result = subprocess.run(["curl", "-fsS", "--negotiate", "-u", ":", url], stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
        if result.returncode == 0 and result.stdout.strip() == b"1":
            return
        if time.monotonic() >= deadline:
            raise RuntimeError(f"Kerberos auth against {CLICKHOUSE_HOST}:{CLICKHOUSE_PORT} did not succeed within 10s.")
        time.sleep(0.5)


def teardown():
    _compose("down", "--volumes")


if __name__ == "__main__":
    actions = {"setup": setup, "teardown": teardown}
    if len(sys.argv) != 2 or sys.argv[1] not in actions:
        print(f"usage: python -m tests.integration_tests.kerberos_manage {{{'|'.join(actions)}}}", file=sys.stderr)
        sys.exit(1)
    actions[sys.argv[1]]()
