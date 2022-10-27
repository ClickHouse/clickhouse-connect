import os
import pathlib
import sys

from typing import Iterator

from unittest import mock
from pytest_mock import MockFixture
from pytest import fixture
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

if sys.version_info[1] not in (8, 9):
    collect_ignore_glob = ['test*.py']


@fixture(scope='session', autouse=True)
def mock_settings_env_vars() -> Iterator[None]:
    path = str(pathlib.Path(__file__).parent.resolve().joinpath('ss_test_config.py'))
    with mock.patch.dict(os.environ, {'SUPERSET_CONFIG_PATH': path}):
        yield


@fixture(scope='session')
# pylint: disable=import-outside-toplevel
def superset_app(session_mocker: MockFixture) -> Iterator[None]:
    from superset.app import SupersetApp
    from superset.initialization import SupersetAppInitializer

    session = sessionmaker(bind=create_engine('sqlite://'))()
    session.remove = lambda: None

    session_mocker.patch('superset.security.SupersetSecurityManager.get_session', return_value=session)
    session_mocker.patch('superset.db.session', session)

    app = SupersetApp(__name__)
    app.config.from_object('superset.config')

    SupersetAppInitializer(app).init_app()
    with app.app_context():
        yield
