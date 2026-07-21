## Contributing guidelines

ClickHouse-connect is an open-source project, and we welcome any contributions from the community. 
Please share your ideas, contribute to the codebase, and help us maintain up-to-date documentation.

## Setting up your environment

### Prerequisites

* Python 3.11+
* Docker and the [Compose plugin](https://docs.docker.com/compose/install/)

### Create a fork of the repository and clone it

```bash
git clone https://github.com/[YOUR_USERNAME]/clickhouse-connect
cd clickhouse-connect
```

### Add PYTHONPATH

Add the project directory to the `PYTHONPATH` environment variable to make the driver sources are available for import.

```bash
export PYTHONPATH="/absolute/path/to/clickhouse-connect"
```

### Prepare a new virtual environment

You could either use PyCharm for that, or follow [the instructions on the official website](https://docs.python.org/3/tutorial/venv.html) and set it up via the command line.

### Install dependencies

```bash
python -m pip install --upgrade pip
pip install setuptools wheel
pip install -r tests/test_requirements.txt
pre-commit install
```

### Run the setup script

The driver uses several Cython extensions that provide additional performance improvements 
(see the [clickhouse_connect/driverc](clickhouse_connect/driverc) directory).
To compile the extensions, run the following command:

```bash
python setup.py build_ext --inplace
```

Additionally, this command is required to provide SQLAlchemy entrypoints:

```bash
python setup.py develop
```

### Add /etc/hosts entry

Required for TLS and Kerberos tests.
The generated certificates assume TLS requests use `server1.clickhouse.test` as the hostname.
See [test_tls.py](tests/integration_tests/test_tls.py) and [test_kerberos.py](tests/integration_tests/test_kerberos.py) for more details.

```bash
sudo -- sh -c "echo 127.0.0.1 server1.clickhouse.test >> /etc/hosts"
```

### PyCharm setup

If you use PyCharm as your IDE, make sure that you have `clickhouse-connect` added to the project structure as a source path. 
Go to Settings -> Project (clickhouse-connect) -> Project structure, right click on `clickhouse-connect` folder, and mark it as "Sources".

## Testing

### Start ClickHouse in Docker

The tests will require two ClickHouse instances to be running. 
One should have a default plain authentication (for integration tests), and the other should have a TLS configuration (for tls tests only).

The integration tests will start and stop the ClickHouse instance automatically. 
However, this adds a few seconds to each run, and this might not be ideal when you run a single test (using PyCharm, for example). 
To disable this behavior, set the `CLICKHOUSE_CONNECT_TEST_DOCKER` environment variable to `0`.

```bash
export CLICKHOUSE_CONNECT_TEST_DOCKER=0
```

The easiest way to start all the required ClickHouse instances is to use the provided Docker Compose file (the integrations tests [setup script](tests/integration_tests/conftest.py) uses the same file).

```bash
docker compose up -d
```

### Run the tests

The project uses [pytest](https://docs.pytest.org/) as a test runner. 
To run all the tests (unit and integration) execute the following command:

```bash
pytest tests
```

If you need to run the unit tests only:

```bash
pytest tests/unit_tests
```

Or the integration tests only:

```bash
pytest tests/integration_tests 
```

### Run the TLS integration tests

These tests require the `CLICKHOUSE_CONNECT_TEST_TLS` environment variable to be set to `1`; otherwise, they will be skipped. 
Additionally, the TLS ClickHouse instance should be running (see [docker-compose.yml](docker-compose.yml)).

```bash
CLICKHOUSE_CONNECT_TEST_TLS=1 pytest tests/integration_tests/test_tls.py
```

### Run the Kerberos integration tests

These tests require the `CLICKHOUSE_CONNECT_TEST_KERBEROS` environment variable to be set to `1`; otherwise, they will be skipped.
Unlike the other test instances, the Kerberos KDC and ClickHouse instance (the `kerberos_kdc` and `kerberos_clickhouse` services in
[docker-compose.yml](docker-compose.yml)) are behind a `kerberos` Compose profile rather than started by a plain `docker compose up -d`,
since they also need an extra host-side step (obtaining a real Kerberos ticket) that Docker Compose cannot do for you. This walks
through setting them up from scratch.

Install the system Kerberos client and development packages (needed to build the `gssapi`/`krb5` Python packages):

```bash
# Debian/Ubuntu
sudo apt-get install gcc python3-dev libkrb5-dev krb5-user

# CentOS/RHEL/Fedora
sudo dnf install gcc python3-devel krb5-devel krb5-workstation

# Arch Linux
sudo pacman -S gcc krb5
```

Make sure you've added the `server1.clickhouse.test` `/etc/hosts` entry from
["Add /etc/hosts entry"](#add-etchosts-entry) above.

The rest (starting a KDC and a Kerberos-configured ClickHouse instance, obtaining a ticket, and tearing it all
back down afterward) is handled automatically by a fixture in
[`test_kerberos.py`](tests/integration_tests/test_kerberos.py), via
[`kerberos_manage.py`](tests/integration_tests/kerberos_manage.py), which uses the fixtures vendored under
[`tests/integration_tests/kerberos_conf`](tests/integration_tests/kerberos_conf).

Run from the repo root:

```bash
CLICKHOUSE_CONNECT_TEST_KERBEROS=1 pytest tests/integration_tests/test_kerberos.py
```

To stand up (or tear down) the same environment by hand, outside of pytest -- for example, to poke at it manually
with `curl --negotiate` -- run:

```bash
python -m tests.integration_tests.kerberos_manage setup
python -m tests.integration_tests.kerberos_manage teardown
```

### Running the integration tests with ClickHouse Cloud

If you want to run the tests using your ClickHouse Cloud instance instead of the local ClickHouse instance running in Docker, you will need a few additional environment variables.

```bash
export CLICKHOUSE_CONNECT_TEST_CLOUD=1
export CLICKHOUSE_CONNECT_TEST_PORT=8443
export CLICKHOUSE_CONNECT_TEST_HOST='instance.clickhouse.cloud'
export CLICKHOUSE_CONNECT_TEST_PASSWORD='secret'
```

Then, you should be able to run the tests as usual:

```bash
pytest tests/integration_tests
```

## Style Guide

The project uses [Ruff](https://docs.astral.sh/ruff/) for linting and formatting.
It is always a good idea to run the linter before committing the changes, as this is a mandatory CI check. For example:

```bash
pip install ruff
ruff format --check clickhouse_connect tests examples setup.py
ruff check clickhouse_connect tests examples setup.py
```

To auto-fix issues:

```bash
ruff format clickhouse_connect tests examples setup.py
ruff check --fix clickhouse_connect tests examples setup.py
```

The project also uses [mypy](https://mypy-lang.org/) for type checking:

```bash
mypy
```

If you ran `pre-commit install` during setup, both `ruff` and `mypy` run automatically on `git commit`.

### Git blame

Bulk formatting commits are listed in `.git-blame-ignore-revs`. To configure git blame to skip them:

```bash
git config blame.ignoreRevsFile .git-blame-ignore-revs
```
