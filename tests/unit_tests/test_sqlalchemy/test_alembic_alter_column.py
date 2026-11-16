from io import StringIO

import pytest
from alembic.util import CommandError
from sqlalchemy import text

from clickhouse_connect.cc_sqlalchemy.alembic import ClickHouseImpl
from clickhouse_connect.cc_sqlalchemy.datatypes.sqltypes import UInt32
from clickhouse_connect.cc_sqlalchemy.dialect import ClickHouseDialect


def _alter_column(**kw) -> list[str]:
    buffer = StringIO()
    impl = ClickHouseImpl(ClickHouseDialect(), None, True, None, buffer, {})
    impl.alter_column("events", "payload", **kw)
    return [line for line in (part.strip() for part in buffer.getvalue().split(";")) if line]


def test_default_removal_alone_emits_remove_default():
    statements = _alter_column(server_default=None, existing_type=UInt32())
    assert statements == ["ALTER TABLE `events` MODIFY COLUMN `payload` REMOVE DEFAULT"]


def test_default_removal_does_not_require_existing_type():
    statements = _alter_column(server_default=None)
    assert statements == ["ALTER TABLE `events` MODIFY COLUMN `payload` REMOVE DEFAULT"]


def test_default_removal_with_type_change_modifies_then_removes():
    statements = _alter_column(server_default=None, type_=UInt32())
    assert statements == [
        "ALTER TABLE `events` MODIFY COLUMN `payload` UInt32",
        "ALTER TABLE `events` MODIFY COLUMN `payload` REMOVE DEFAULT",
    ]


def test_default_removal_honors_if_exists_and_settings():
    statements = _alter_column(
        server_default=None,
        existing_type=UInt32(),
        if_exists=True,
        clickhouse_settings={"alter_sync": 2},
    )
    assert statements == ["ALTER TABLE `events` MODIFY COLUMN IF EXISTS `payload` REMOVE DEFAULT SETTINGS alter_sync = 2"]


def test_omitted_default_leaves_type_change_without_remove():
    statements = _alter_column(type_=UInt32())
    assert statements == ["ALTER TABLE `events` MODIFY COLUMN `payload` UInt32"]


def test_replacing_default_renders_inline_without_remove():
    statements = _alter_column(server_default=text("'13'"), existing_type=UInt32())
    assert statements == ["ALTER TABLE `events` MODIFY COLUMN `payload` UInt32 DEFAULT '13'"]


@pytest.mark.parametrize("prefix", ["clickhouse", "clickhousedb"])
@pytest.mark.parametrize(
    "prop, keyword",
    [("materialized", "MATERIALIZED"), ("alias", "ALIAS"), ("codec", "CODEC"), ("ttl", "TTL")],
)
def test_property_removal_emits_remove_keyword(prefix, prop, keyword):
    statements = _alter_column(existing_type=UInt32(), **{f"{prefix}_{prop}": None})
    assert statements == [f"ALTER TABLE `events` MODIFY COLUMN `payload` REMOVE {keyword}"]


def test_property_removal_does_not_require_existing_type():
    statements = _alter_column(clickhouse_codec=None)
    assert statements == ["ALTER TABLE `events` MODIFY COLUMN `payload` REMOVE CODEC"]


@pytest.mark.parametrize(
    "prop, value, fragment",
    [
        ("codec", "LZ4", "CODEC(LZ4)"),
        ("materialized", text("`id` * 3"), "MATERIALIZED `id` * 3"),
        ("ttl", text("now() + INTERVAL 1 DAY"), "TTL now() + INTERVAL 1 DAY"),
    ],
)
def test_property_value_renders_inside_modify_column(prop, value, fragment):
    statements = _alter_column(existing_type=UInt32(), **{f"clickhouse_{prop}": value})
    assert statements == [f"ALTER TABLE `events` MODIFY COLUMN `payload` UInt32 {fragment}"]


@pytest.mark.parametrize("prefix", ["clickhouse", "clickhousedb"])
@pytest.mark.parametrize(
    ("prop", "value", "fragment"),
    [
        ("codec", "LZ4", "CODEC(LZ4)"),
        ("alias", text("`id` + 1"), "ALIAS `id` + 1"),
        ("materialized", text("`id` * 3"), "MATERIALIZED `id` * 3"),
        ("ttl", text("ts + INTERVAL 1 DAY"), "TTL ts + INTERVAL 1 DAY"),
    ],
)
def test_property_set_without_existing_type(prefix, prop, value, fragment):
    assert _alter_column(**{f"{prefix}_{prop}": value}) == [f"ALTER TABLE `events` MODIFY COLUMN `payload` {fragment}"]


@pytest.mark.parametrize("prop", ["codec", "alias", "materialized", "ttl"])
@pytest.mark.parametrize("canonical", [None, "13"])
def test_explicit_canonical_property_wins_over_legacy(prop, canonical):
    kwargs = {f"clickhousedb_{prop}": canonical, f"clickhouse_{prop}": text("79")}
    assert _alter_column(**kwargs) == _alter_column(**{f"clickhousedb_{prop}": canonical})


def test_multiple_removals_follow_deterministic_order():
    statements = _alter_column(
        server_default=None,
        clickhouse_ttl=None,
        clickhouse_codec=None,
        clickhouse_alias=None,
        clickhouse_materialized=None,
    )
    assert statements == [
        "ALTER TABLE `events` MODIFY COLUMN `payload` REMOVE DEFAULT",
        "ALTER TABLE `events` MODIFY COLUMN `payload` REMOVE MATERIALIZED",
        "ALTER TABLE `events` MODIFY COLUMN `payload` REMOVE ALIAS",
        "ALTER TABLE `events` MODIFY COLUMN `payload` REMOVE CODEC",
        "ALTER TABLE `events` MODIFY COLUMN `payload` REMOVE TTL",
    ]


def test_type_change_with_removal_emits_modify_before_removals():
    statements = _alter_column(type_=UInt32(), server_default=None, clickhouse_codec=None)
    assert statements == [
        "ALTER TABLE `events` MODIFY COLUMN `payload` UInt32",
        "ALTER TABLE `events` MODIFY COLUMN `payload` REMOVE DEFAULT",
        "ALTER TABLE `events` MODIFY COLUMN `payload` REMOVE CODEC",
    ]


def test_schema_qualified_and_quoted_identifiers():
    buffer = StringIO()
    impl = ClickHouseImpl(ClickHouseDialect(), None, True, None, buffer, {})
    impl.alter_column("event log", "pay load", schema="my db", server_default=None, existing_type=UInt32())
    assert buffer.getvalue().strip() == "ALTER TABLE `my db`.`event log` MODIFY COLUMN `pay load` REMOVE DEFAULT;"


@pytest.mark.parametrize(
    "kw",
    [{"nullable": True}, {"server_default": text("'13'")}],
    ids=["nullable", "default-value"],
)
def test_value_change_without_existing_type_raises(kw):
    with pytest.raises(CommandError, match="requires existing_type"):
        _alter_column(**kw)
