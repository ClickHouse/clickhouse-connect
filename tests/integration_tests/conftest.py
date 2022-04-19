import sys
from pathlib import Path
from subprocess import Popen, PIPE
from typing import Iterator
from time import sleep

from pytest import fixture
from clickhouse_connect.driver import BaseClient
from clickhouse_connect import create_client
from clickhouse_connect.driver.exceptions import ClickHouseError


@fixture(scope='session', autouse=True, name='test_driver')
def test_driver_fixture(request) -> Iterator[BaseClient]:
    use_docker = request.config.getoption('docker', True)
    compose_file = f'{Path(__file__).parent}/docker-compose.yml'
    if use_docker:
        run_cmd(['docker-compose', '-f', compose_file, 'down', '-v'])
        sys.stderr.write('Starting docker compose')
        up_result = run_cmd(['docker-compose', '-f', compose_file, 'up', '-d'])
        if up_result[0]:
            raise Exception(f'Failed to start docker: {up_result[2]}')
        sleep(5)
    host = request.config.getoption('host')
    port = request.config.getoption('port')
    if not port:
        port = 10723 if use_docker else 8123
    tries = 0
    while True:
        tries += 1
        try:
            driver = create_client(host=host, port=port)
            break
        except ClickHouseError as ex:
            if tries > 15:
                raise Exception('Failed to connect to ClickHouse server after 30 seconds') from ex
            sleep(1)
    yield driver
    if use_docker:
        down_result = run_cmd(['docker-compose', '-f', compose_file, 'down', '-v'])
        if down_result[0]:
            sys.stderr.write(f'Warning -- failed to cleanly bring down docker compose: {down_result[2]}')
        else:
            sys.stderr.write('Successfully stopped docker compose')


def run_cmd(cmd):
    with Popen(cmd, stdout=PIPE, stderr=PIPE) as popen:
        stdout, stderr = popen.communicate()
        return popen.returncode, stdout, stderr
