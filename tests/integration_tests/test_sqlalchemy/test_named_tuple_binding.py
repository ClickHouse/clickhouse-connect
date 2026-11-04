import pytest
import sqlalchemy as sa

from clickhouse_connect.cc_sqlalchemy.datatypes.base import sqla_type_from_name
from clickhouse_connect.cc_sqlalchemy.datatypes.sqltypes import UInt32


@pytest.mark.parametrize("server_side_params", [False, True])
@pytest.mark.parametrize("route", ["single", "native", "multivalues", "fallback"])
def test_named_tuple_insert(test_engine, table_context, test_db, server_side_params, route):
    engine = sa.create_engine(test_engine.url, server_side_params=server_side_params)
    type_name = "Tuple(protocol UInt8, port UInt16)"
    with table_context("sqlalchemy_named_tuple", ["id UInt32", f"value {type_name}"]):
        table = sa.Table(
            "sqlalchemy_named_tuple",
            sa.MetaData(schema=test_db),
            sa.Column("id", UInt32),
            sa.Column("value", sqla_type_from_name(type_name)),
        )
        rows = [{"id": key, "value": {"port": key, "protocol": 13}} for key in (13, 79)]
        try:
            with engine.connect() as conn:
                if route == "single":
                    for row in rows:
                        conn.execute(table.insert(), row)
                elif route == "multivalues":
                    conn.execute(table.insert().values(rows))
                elif route == "fallback":
                    conn.execute(table.insert().values(id=sa.bindparam("id", type_=UInt32()) + 0), rows)
                else:
                    conn.execute(table.insert(), rows)
                assert conn.execute(sa.select(table).order_by(table.c.id)).all() == [
                    (key, {"protocol": 13, "port": key}) for key in (13, 79)
                ]
                assert conn.execute(sa.select(table.c.id).where(table.c.value == rows[1]["value"])).all() == [(79,)]
                literal = sa.select(sa.cast(sa.literal(rows[0]["value"], table.c.value.type), table.c.value.type))
                compiled = str(literal.compile(engine, compile_kwargs={"literal_binds": True}))
                assert conn.exec_driver_sql(compiled).scalar_one() == rows[0]["value"]
        finally:
            engine.dispose()
