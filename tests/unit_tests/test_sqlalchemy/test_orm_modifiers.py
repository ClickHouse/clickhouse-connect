from sqlalchemy import Column, select
from sqlalchemy.dialects import registry
from sqlalchemy.orm import aliased, declarative_base

# Import sql module so Select.<modifier> monkey-patches are installed.
import clickhouse_connect.cc_sqlalchemy.sql  # noqa: F401
from clickhouse_connect.cc_sqlalchemy import dialect_name
from clickhouse_connect.cc_sqlalchemy.datatypes.sqltypes import Bool, String, UInt32

dialect = registry.load(dialect_name)()


Base = declarative_base()


class Event(Base):
    __tablename__ = "events"
    id = Column(UInt32, primary_key=True)
    user_id = Column(UInt32)
    active = Column(Bool)
    name = Column(String)


def compile_sql(stmt):
    return str(stmt.compile(dialect=dialect, compile_kwargs={"literal_binds": True}))


def test_orm_final_renders():
    stmt = select(Event.id, Event.name).where(Event.id > 0).final()
    sql = compile_sql(stmt)
    assert "FINAL" in sql


def test_orm_final_renders_for_aliased_entity():
    event_alias = aliased(Event, name="e")
    stmt = select(event_alias.id, event_alias.name).final()
    sql = compile_sql(stmt)
    assert "FROM `events` AS `e` FINAL" in sql


def test_orm_sample_renders():
    stmt = select(Event.id, Event.name).sample(0.1)
    sql = compile_sql(stmt)
    assert "SAMPLE 0.1" in sql


def test_orm_prewhere_renders():
    stmt = select(Event.id, Event.name).prewhere(Event.id > 0)
    sql = compile_sql(stmt)
    assert "PREWHERE" in sql


def test_orm_limit_by_no_limit_renders():
    stmt = select(Event.id, Event.name).limit_by([Event.name], 1)
    sql = compile_sql(stmt)
    assert "LIMIT 1 BY" in sql


def test_orm_limit_by_with_limit_renders():
    stmt = select(Event.id, Event.name).limit_by([Event.name], 1).limit(10)
    sql = compile_sql(stmt)
    assert "LIMIT 1 BY" in sql
    assert "LIMIT 10" in sql
    assert sql.index("LIMIT 1 BY") < sql.index("LIMIT 10")


def test_orm_combined_modifiers_render_in_order():
    stmt = select(Event.id, Event.name).where(Event.id > 5).final().prewhere(Event.name == "foo").limit_by([Event.user_id], 3).limit(100)
    sql = compile_sql(stmt)
    assert "FINAL" in sql
    assert "PREWHERE" in sql
    assert "LIMIT 3 BY" in sql
    assert "LIMIT 100" in sql
    assert sql.index("FINAL") < sql.index("PREWHERE") < sql.index("LIMIT 3 BY") < sql.index("LIMIT 100")


def test_orm_chained_prewheres_compose():
    stmt = select(Event.id).prewhere(Event.id == 1).prewhere(Event.active == True)  # noqa: E712
    sql = compile_sql(stmt)
    assert "PREWHERE" in sql
    prewhere_section = sql.split("PREWHERE", 1)[1]
    assert "AND" in prewhere_section
