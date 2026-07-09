from sqlalchemy import select, text
from sqlalchemy.engine import Engine

_get_max_threads = "SELECT getSetting('max_threads') AS v"


def test_statement_settings_text(test_engine: Engine):
    with test_engine.begin() as conn:
        # Pick a target that always differs from the server default so the assertion is meaningful.
        baseline = int(conn.execute(text(_get_max_threads)).scalar_one())
        target = baseline + 1

        stmt = text(_get_max_threads).execution_options(settings={"max_threads": target})
        assert int(conn.execute(stmt).scalar_one()) == target

        # Setting is per-statement; a plain query reverts to the baseline.
        assert int(conn.execute(text(_get_max_threads)).scalar_one()) == baseline


def test_statement_settings_select(test_engine: Engine):
    with test_engine.begin() as conn:
        baseline = int(conn.execute(select(text("getSetting('max_threads') AS v"))).scalar_one())
        target = baseline + 1

        stmt = select(text("getSetting('max_threads') AS v")).execution_options(settings={"max_threads": target})
        assert int(conn.execute(stmt).scalar_one()) == target


def test_connection_settings(test_engine: Engine):
    with test_engine.begin() as conn:
        baseline = int(conn.execute(text(_get_max_threads)).scalar_one())
        target = baseline + 1

        # Connection.execution_options is generative in SQLAlchemy 1.4, so use the returned connection.
        c = conn.execution_options(settings={"max_threads": target})
        assert int(c.execute(text(_get_max_threads)).scalar_one()) == target
