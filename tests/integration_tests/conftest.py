import sys
from pathlib import Path
from subprocess import Popen, PIPE
from typing import Iterator
from time import sleep

from pytest import fixture
from clickhouse_connect.driver import BaseClient
from clickhouse_connect import create_client


@fixture(scope='session', autouse=True, name='test_driver')
def test_driver_fixture(request) -> Iterator[BaseClient]:
    host = request.config.getoption('host')
    port = request.config.getoption('port')
    docker = request.config.getoption('docker', True)
    if not port:
        port = 10723 if docker else 8123
    driver = create_client(host=host, port=port, validate=False)
    yield driver
    driver.close()


@fixture(scope='session', autouse=True, name='docker')
def clickhouse_container_fixture(request, test_driver: BaseClient) -> Iterator[None]:
    if not request.config.getoption('docker', True):
        yield
        return
    compose_file = f'{Path(__file__).parent}/docker-compose.yml'
    run_cmd(['docker-compose', '-f', compose_file, 'down', '-v'])
    sys.stderr.write('Starting docker compose')
    up_result = run_cmd(['docker-compose', '-f', compose_file, 'up', '-d'])
    if up_result[0]:
        raise Exception(f'Failed to start docker: {up_result[2]}')
    sleep(5)
    for _ in range(30):
        if test_driver.ping():
            break
        sleep(1)
    else:
        raise Exception('Failed to ping ClickHouse server after 30 seconds')
    yield
    down_result = run_cmd(['docker-compose', '-f', compose_file, 'down', '-v'])
    if down_result[0]:
        sys.stderr.write(f'Warning -- failed to cleanly bring down docker compose: {down_result[2]}')
    else:
        sys.stderr.write('Successfully stopped docker compose')


def run_cmd(cmd):
    with Popen(cmd, stdout=PIPE, stderr=PIPE) as popen:
        stdout, stderr = popen.communicate()
        return popen.returncode, stdout, stderr
