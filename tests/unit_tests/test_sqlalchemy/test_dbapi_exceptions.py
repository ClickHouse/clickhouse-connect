import pytest
from sqlalchemy import exc as sqlalchemy_exc

from clickhouse_connect import dbapi
from clickhouse_connect.driver import exceptions as driver_exc


@pytest.mark.parametrize(
    "name",
    [
        "Warning",
        "Error",
        "InterfaceError",
        "DatabaseError",
        "DataError",
        "OperationalError",
        "IntegrityError",
        "InternalError",
        "ProgrammingError",
        "NotSupportedError",
    ],
)
def test_dbapi_exports_driver_exception_hierarchy(name):
    assert getattr(dbapi, name) is getattr(driver_exc, name)


@pytest.mark.parametrize(
    ("driver_error", "sqlalchemy_error"),
    [
        (driver_exc.Error, sqlalchemy_exc.DBAPIError),
        (driver_exc.InterfaceError, sqlalchemy_exc.InterfaceError),
        (driver_exc.DatabaseError, sqlalchemy_exc.DatabaseError),
        (driver_exc.DataError, sqlalchemy_exc.DataError),
        (driver_exc.OperationalError, sqlalchemy_exc.OperationalError),
        (driver_exc.IntegrityError, sqlalchemy_exc.IntegrityError),
        (driver_exc.InternalError, sqlalchemy_exc.InternalError),
        (driver_exc.ProgrammingError, sqlalchemy_exc.ProgrammingError),
        (driver_exc.NotSupportedError, sqlalchemy_exc.NotSupportedError),
        (driver_exc.StreamFailureError, sqlalchemy_exc.OperationalError),
    ],
)
def test_sqlalchemy_maps_driver_exception(driver_error, sqlalchemy_error):
    original = driver_error("test failure")

    assert isinstance(original, dbapi.Error)
    wrapped = sqlalchemy_exc.DBAPIError.instance("SELECT 13", None, original, dbapi.Error)

    assert type(wrapped) is sqlalchemy_error
    assert wrapped.orig is original


def test_sqlalchemy_keeps_non_driver_exception_as_statement_error():
    original = ValueError("test failure")

    wrapped = sqlalchemy_exc.DBAPIError.instance("SELECT 13", None, original, dbapi.Error)

    assert type(wrapped) is sqlalchemy_exc.StatementError
    assert wrapped.orig is original
