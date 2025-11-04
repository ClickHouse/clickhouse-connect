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

Required for TLS tests.
The generated certificates assume TLS requests use `server1.clickhouse.test` as the hostname.
See [test_tls.py](tests/integration_tests/test_tls.py) for more details.

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

The project uses [PyLint](https://pypi.org/project/pylint/) to enforce the code style. 
It is always a good idea to run the linter before committing the changes, as this is a mandatory CI check. For example:

```bash
pip install pylint
pylint clickhouse_connect
pylint tests
```
