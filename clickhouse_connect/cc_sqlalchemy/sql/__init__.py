from sqlalchemy import Table
from sqlalchemy.sql.selectable import FromClause, Select

from clickhouse_connect.driver.binding import quote_identifier

# Dialect name used for non-rendering statement hints that only serve to
# differentiate cache keys when FINAL/SAMPLE modifiers are applied.
_CH_MODIFIER_DIALECT = "_ch_modifier"


def full_table(table_name: str, schema: str | None = None) -> str:
    if table_name.startswith("(") or "." in table_name or not schema:
        return quote_identifier(table_name)
    return f"{quote_identifier(schema)}.{quote_identifier(table_name)}"


def format_table(table: Table):
    return full_table(table.name, table.schema)


def _resolve_target(select_stmt: Select, table: FromClause | None, method_name: str) -> FromClause:
    """Resolve the target FROM clause for ClickHouse modifiers (FINAL/SAMPLE)."""
    if not isinstance(select_stmt, Select):
        raise TypeError(f"{method_name}() expects a SQLAlchemy Select instance")

    target = table
    if target is None:
        froms = select_stmt.get_final_froms()
        if not froms:
            raise ValueError(f"{method_name}() requires a table to apply the {method_name.upper()} modifier.")
        if len(froms) > 1:
            raise ValueError(f"{method_name}() is ambiguous for statements with multiple FROM clauses. Specify the table explicitly.")
        target = froms[0]

    # Unwrap ArrayJoin so FINAL/SAMPLE apply to the underlying table that the
    # compiler will actually render, not the ArrayJoin FromClause wrapper.
    from clickhouse_connect.cc_sqlalchemy.sql.clauses import ArrayJoin as _ArrayJoin

    while isinstance(target, _ArrayJoin):
        target = target.left

    if not isinstance(target, FromClause):
        raise TypeError("table must be a SQLAlchemy FromClause when provided")

    return target


def _target_cache_key(target: FromClause) -> str:
    """Stable string identifying a FROM target for cache key differentiation."""
    if hasattr(target, "fullname"):
        return target.fullname
    return target.name


def final(select_stmt: Select, table: FromClause | None = None) -> Select:
    """Apply the ClickHouse FINAL modifier to a select statement.

    FINAL forces ClickHouse to merge data parts before returning results,
    guaranteeing fully collapsed rows for ReplacingMergeTree, CollapsingMergeTree,
    and similar engines.

    Args:
        select_stmt: The SELECT statement to modify.
        table: The target table to apply FINAL to. Required when the query
            joins multiple tables, optional when there is a single FROM target.
    """
    target = _resolve_target(select_stmt, table, "final")
    ch_final = getattr(select_stmt, "_ch_final", set())

    if target in ch_final:
        return select_stmt

    # with_statement_hint creates a generative copy and adds a non-rendering
    # hint that participates in the statement cache key.
    hint_key = _target_cache_key(target)
    new_stmt = select_stmt.with_statement_hint(f"FINAL:{hint_key}", dialect_name=_CH_MODIFIER_DIALECT)
    new_stmt._ch_final = ch_final | {target}
    return new_stmt


def _select_final(self: Select, table: FromClause | None = None) -> Select:
    """
    Select.final() convenience wrapper around the module-level final() helper.
    """
    return final(self, table=table)


def sample(select_stmt: Select, sample_value: str | int | float, table: FromClause | None = None) -> Select:
    """Apply the ClickHouse SAMPLE modifier to a select statement.

    Args:
        select_stmt: The SELECT statement to modify.
        sample_value: The sample expression. Can be a float between 0 and 1
            for a fractional sample (e.g. 0.1 for 10%), an integer for an
            approximate row count, or a string for SAMPLE expressions like
            '1/10 OFFSET 1/2'.
        table: The target table to sample. Required when the query joins
            multiple tables, optional when there is a single FROM target.
    """
    target = _resolve_target(select_stmt, table, "sample")

    hint_key = _target_cache_key(target)
    new_stmt = select_stmt.with_statement_hint(f"SAMPLE:{hint_key}:{sample_value}", dialect_name=_CH_MODIFIER_DIALECT)
    ch_sample = dict(getattr(select_stmt, "_ch_sample", {}))
    ch_sample[target] = sample_value
    new_stmt._ch_sample = ch_sample
    return new_stmt


def _select_sample(self: Select, sample_value: str | int | float, table: FromClause | None = None) -> Select:
    """
    Select.sample() convenience wrapper around the module-level sample() helper.
    """
    return sample(self, sample_value=sample_value, table=table)


def _apply_array_join(select_stmt: Select, cols, alias, is_left: bool) -> Select:
    """Wrap the single FROM of a Select in an ARRAY JOIN / LEFT ARRAY JOIN clause.

    Returns a new Select whose FROM target has been replaced with an
    ArrayJoin FromClause. The wrapped source is hidden from the FROM list
    via ArrayJoin._hide_froms so the compiler does not render it twice.
    """
    from clickhouse_connect.cc_sqlalchemy.sql.clauses import array_join as make_array_join

    if not isinstance(select_stmt, Select):
        raise TypeError("array_join() expects a SQLAlchemy Select instance")

    if not cols:
        raise ValueError("array_join() requires at least one array column")

    froms = select_stmt.get_final_froms()
    if not froms:
        raise ValueError("array_join() requires the Select to have a FROM clause to wrap.")
    if len(froms) > 1:
        raise ValueError(
            "array_join() is ambiguous for statements with multiple FROM clauses. "
            "Use the module-level array_join(left, array_column, ...) with select_from() instead."
        )
    target = froms[0]

    columns = list(cols)
    if len(columns) == 1:
        array_column = columns[0]
        alias_arg = alias
    else:
        array_column = columns
        if alias is None:
            alias_arg = None
        elif isinstance(alias, (list, tuple)):
            alias_arg = list(alias)
        else:
            raise ValueError("alias must be a list/tuple matching the number of columns when multiple columns are provided")

    aj = make_array_join(target, array_column, alias=alias_arg, is_left=is_left)
    return select_stmt.select_from(aj)


def _select_array_join(self: Select, *cols, alias=None) -> Select:
    """Select.array_join(*cols, alias=None) — generative ARRAY JOIN helper."""
    return _apply_array_join(self, cols, alias, is_left=False)


def _select_left_array_join(self: Select, *cols, alias=None) -> Select:
    """Select.left_array_join(*cols, alias=None) — generative LEFT ARRAY JOIN helper."""
    return _apply_array_join(self, cols, alias, is_left=True)


# Monkey-patch the select class to add final and sample methods
Select.sample = _select_sample
Select.final = _select_final
Select.array_join = _select_array_join
Select.left_array_join = _select_left_array_join
