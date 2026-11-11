from io import StringIO
from pathlib import Path

import pytest
import sqlalchemy as sa
from alembic.autogenerate import Rewriter
from alembic.autogenerate.api import render_python_code
from alembic.operations import Operations, ops
from alembic.runtime.migration import MigrationContext
from sqlalchemy import Column

from clickhouse_connect.cc_sqlalchemy import alembic as ch_alembic
from clickhouse_connect.cc_sqlalchemy.alembic import operations
from clickhouse_connect.cc_sqlalchemy.datatypes.sqltypes import UInt32
from clickhouse_connect.cc_sqlalchemy.dialect import ClickHouseDialect
from tests.unit_tests.test_sqlalchemy.test_alembic_skip_indexes import _compare as compare_indexes

INDEX = ch_alembic.ClickHouseIndex("value_idx", "value", "minmax", 1)
PROJECTION = ch_alembic.ClickHouseProjection("value_projection", "SELECT value")
PUBLIC_OPERATIONS = [
    ch_alembic.AddClickHouseIndexOp("events", "value_idx", "value", "minmax", granularity=1),
    ch_alembic.AddClickHouseIndexesOp("events", [INDEX]),
    ch_alembic.DropClickHouseIndexOp("events", "value_idx", existing_index=INDEX),
    ch_alembic.DropClickHouseIndexesOp("events", ["value_idx"]),
    ch_alembic.MaterializeClickHouseIndexOp("events", "value_idx"),
    ch_alembic.AddClickHouseProjectionOp("events", "value_projection", "SELECT value"),
    ch_alembic.AddClickHouseProjectionsOp("events", [PROJECTION]),
    ch_alembic.DropClickHouseProjectionOp("events", "value_projection", existing_projection=PROJECTION),
    ch_alembic.DropClickHouseProjectionsOp("events", ["value_projection"]),
    ch_alembic.MaterializeClickHouseProjectionOp("events", "value_projection"),
    ch_alembic.ModifyClickHouseTableSettingsOp(
        "events", {"min_bytes_for_wide_part": 13}, existing_settings={"min_bytes_for_wide_part": 79}
    ),
    ch_alembic.ResetClickHouseTableSettingsOp("events", ["min_bytes_for_wide_part"]),
    ch_alembic.CreateClickHouseViewOp("value_view", "SELECT 13"),
    ch_alembic.DropClickHouseViewOp("value_view"),
    ch_alembic.CreateClickHouseFunctionOp("value_function", "value -> value + 13"),
    ch_alembic.DropClickHouseFunctionOp("value_function"),
    ch_alembic.CreateClickHouseMaterializedViewOp("value_mv", "events", "SELECT 13"),
    ch_alembic.DropClickHouseMaterializedViewOp("value_mv"),
    ch_alembic.CreateClickHouseDictionaryOp(
        "value_dictionary",
        [Column("value", UInt32)],
        primary_key="value",
        source="CLICKHOUSE(TABLE 'events')",
        layout="FLAT",
        lifetime="13",
    ),
    ch_alembic.DropClickHouseDictionaryOp("value_dictionary"),
    ch_alembic.ReloadClickHouseDictionaryOp("value_dictionary"),
]


def offline_context(buffer):
    return MigrationContext.configure(dialect=ClickHouseDialect(), opts={"as_sql": True, "output_buffer": buffer})


@pytest.mark.parametrize("operation", PUBLIC_OPERATIONS, ids=lambda op: type(op).__name__)
def test_public_identity_invocation_rendering_and_rewriter(operation):
    cls = type(operation)
    assert cls.__name__ in operations.__all__
    assert cls.__name__ in ch_alembic.__all__
    assert getattr(operations, cls.__name__) is cls
    assert getattr(operations, "_" + cls.__name__) is cls
    direct, rendered = StringIO(), StringIO()
    Operations(offline_context(direct)).invoke(operation)
    context = MigrationContext.configure(dialect=ClickHouseDialect())
    code = render_python_code(ops.UpgradeOps([operation]), migration_context=context)
    exec(
        "\n".join(line[4:] for line in code.splitlines() if line.startswith("    ")),
        {
            "op": Operations(offline_context(rendered)),
            "sa": sa,
            "UInt32": UInt32,
            "ClickHouseIndex": ch_alembic.ClickHouseIndex,
            "ClickHouseProjection": ch_alembic.ClickHouseProjection,
        },
    )
    assert rendered.getvalue() == direct.getvalue()
    writer = Rewriter()
    seen = []

    @writer.rewrites(cls)
    def inspect_operation(context, revision, op):
        seen.append(op)
        return op

    script = ops.MigrationScript(None, ops.UpgradeOps([ops.ModifyTableOps("events", [operation])]), ops.DowngradeOps([]))
    writer(context, (), [script])
    assert seen == [operation]
    try:
        reverse = operation.reverse()
    except NotImplementedError:
        return
    for reversed_op in reverse.ops if isinstance(reverse, ops.OpContainer) else [reverse]:
        assert getattr(ch_alembic, type(reversed_op).__name__) is type(reversed_op)


def test_public_export_set_is_complete():
    assert len(PUBLIC_OPERATIONS) == 21
    assert {type(op) for op in PUBLIC_OPERATIONS} == {getattr(operations, name) for name in operations.__all__ if name.endswith("Op")}


@pytest.fixture
def recipe():
    root = Path(__file__).resolve().parents[3]
    texts = [(root / name).read_text() for name in ("docs/sqlalchemy.mdx", "clickhouse_connect/cc_sqlalchemy/alembic/WORKED_EXAMPLE.md")]
    snippets = [
        text.split("### Classify generated operations before rendering", 1)[1].split("```python\n", 1)[1].split("```", 1)[0]
        for text in texts
    ]
    assert snippets[0] == snippets[1]
    namespace = {}
    exec(snippets[0], namespace)
    return namespace


def test_recipe_classifies_generated_operations(monkeypatch, recipe):
    # Real comparison output: an approved addition and an undeclared-index removal.
    _, generated, _ = compare_indexes(monkeypatch, [ch_alembic.ClickHouseIndex("old_idx", "value", "minmax", 1)], [INDEX])
    classify = recipe["classify_tree"]
    assert [item[2] for item in classify(ops.UpgradeOps([generated]), "upgrade")] == ["rejected", "allowed"]
    assert [item[2] for item in classify(generated.reverse(), "downgrade")] == ["rejected", "review"]
    # Non-adjacent replacement, unknown SQL, and creation flags never pass by verb alone.
    replacement = ops.UpgradeOps(
        [
            ch_alembic.DropClickHouseIndexOp("events", INDEX.name),
            ops.ModifyTableOps("events", [ops.ExecuteSQLOp("SELECT 13")]),
            PUBLIC_OPERATIONS[0],
            ch_alembic.CreateClickHouseViewOp("value_view", "SELECT 13", or_replace=True),
            ch_alembic.AddClickHouseIndexesOp("events", [INDEX]),
        ]
    )
    assert [item[2] for item in classify(replacement, "upgrade")] == ["review", "rejected", "review", "review", "review"]
    script = ops.MigrationScript(None, replacement, ops.DowngradeOps([]))
    with pytest.raises(ValueError, match="Migration requires review"):
        recipe["review_revision"](None, (), [script])
    script.upgrade_ops = [ops.UpgradeOps([]), ops.UpgradeOps([])]
    with pytest.raises(ValueError, match="one operation stream"):
        recipe["review_revision"](None, (), [script])


def test_recipe_accepts_generated_addition_and_preserves_writer_imports(monkeypatch, recipe, caplog):
    caplog.set_level("INFO", logger="alembic.policy")
    _, generated, _ = compare_indexes(monkeypatch, [], [INDEX])
    script = ops.MigrationScript(None, ops.UpgradeOps([generated]), ops.DowngradeOps([generated.reverse()]))
    recipe["review_revision"](None, (), [script])
    assert "from clickhouse_connect import cc_sqlalchemy" in script.imports
    assert "upgrade: AddClickHouseIndexOp: allowed" in caplog.text
    assert "downgrade: DropClickHouseIndexOp: rejected" in caplog.text


@pytest.mark.parametrize(
    ("operation_class", "args", "after", "owner"),
    [
        (ch_alembic.AddClickHouseIndexOp, ("events", "value_idx", "value", "minmax"), "after_index", "_AddClickHouseIndexOp"),
        (
            ch_alembic.AddClickHouseProjectionOp,
            ("events", "value_projection", "SELECT value"),
            "after_projection",
            "_AddClickHouseProjectionOp",
        ),
    ],
)
@pytest.mark.parametrize("subclass", [False, True], ids=["exact-class", "subclass"])
def test_position_validation_message(operation_class, args, after, owner, subclass):
    if subclass:
        operation_class = type("CustomOperation", (operation_class,), {})
    with pytest.raises(ValueError) as excinfo:
        operation_class(*args, first=True, **{after: "previous"})
    assert str(excinfo.value) == f"{owner} cannot specify both first and after placement"


@pytest.mark.parametrize(("value", "verdict"), [(107374182400, "allowed"), (161061273600, "review"), (79, "review")])
def test_recipe_table_setting_value(recipe, value, verdict):
    setting = "max_bytes_to_merge_at_max_space_in_pool"
    operation = ch_alembic.ModifyClickHouseTableSettingsOp("events", {setting: value}, existing_settings={setting: 161061273600})
    upgrade = ops.UpgradeOps([ops.ModifyTableOps("events", [operation])])
    assert [item[2] for item in recipe["classify_tree"](upgrade, "upgrade")] == [verdict]
    script = ops.MigrationScript(None, upgrade, upgrade.reverse())
    if verdict == "allowed":
        recipe["review_revision"](None, (), [script])
        assert "from clickhouse_connect import cc_sqlalchemy" in script.imports
    else:
        with pytest.raises(ValueError, match="Migration requires review"):
            recipe["review_revision"](None, (), [script])
