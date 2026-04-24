import sqlalchemy
from sqlalchemy import Column, Integer, MetaData, create_engine
from sqlalchemy.orm import Query, Session, sessionmaker

from clickhouse_connect.cc_sqlalchemy import Cursor, Table
from clickhouse_connect.cc_sqlalchemy.engines import MergeTree
from clickhouse_connect.dbapi.cursor import Cursor as DbapiCursor


def test_cursor_reexport_identity():
    assert Cursor is DbapiCursor


def test_table_reexport_identity():
    assert Table is sqlalchemy.Table


def test_table_with_clickhouse_engine_smoke():
    metadata = MetaData()
    engine = MergeTree(order_by="id")
    table = Table(
        "test",
        metadata,
        Column("id", Integer),
        clickhouse_engine=engine,
    )
    assert table.name == "test"
    assert table.kwargs["clickhouse_engine"] is engine


def test_sessionmaker_accepts_custom_session_and_query():
    class CustomQuery(Query):
        pass

    class CustomSession(Session):
        pass

    engine = create_engine("clickhouse://localhost")
    factory = sessionmaker(bind=engine, class_=CustomSession, query_cls=CustomQuery)
    session = factory()
    try:
        assert isinstance(session, CustomSession)
        assert session.bind is engine
    finally:
        session.close()
