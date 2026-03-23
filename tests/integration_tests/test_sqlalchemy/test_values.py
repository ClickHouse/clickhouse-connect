import sqlalchemy as db
from sqlalchemy.engine import Engine

from clickhouse_connect.cc_sqlalchemy.datatypes.sqltypes import DateTime


def test_values_round_trip_multi_column(test_engine: Engine):
    with test_engine.begin() as conn:
        values_clause = db.values(
            db.column("id", db.Integer),
            db.column("name", db.String),
            name="v",
        ).data([(17, "user_1"), (29, "user_2")])

        rows = conn.execute(
            db.select(values_clause.c.id, values_clause.c.name).select_from(values_clause).order_by(values_clause.c.id)
        ).fetchall()

        assert [(row.id, row.name) for row in rows] == [(17, "user_1"), (29, "user_2")]


def test_values_round_trip_single_column(test_engine: Engine):
    with test_engine.begin() as conn:
        values_clause = db.values(
            db.column("score", db.Integer),
            name="v",
        ).data([(17,), (29,)])

        total = conn.execute(db.select(db.func.sum(values_clause.c.score)).select_from(values_clause)).scalar()

        assert total == 46


def test_values_round_trip_type_name_with_quotes(test_engine: Engine):
    with test_engine.begin() as conn:
        values_clause = db.values(
            db.column("event_ts", DateTime("UTC")),
            name="v",
        ).data([("2024-01-02 03:04:05",)])

        value = conn.execute(db.select(values_clause.c.event_ts).select_from(values_clause)).scalar()

        assert str(value).startswith("2024-01-02 03:04:05")


def test_values_cte_round_trip(test_engine: Engine):
    with test_engine.begin() as conn:
        values_clause = (
            db.values(
                db.column("id", db.Integer),
                name="v",
            )
            .data([(17,), (29,)])
            .cte("input_rows")
        )

        value = conn.execute(db.select(db.func.max(values_clause.c.id)).select_from(values_clause)).scalar()

        assert value == 29
