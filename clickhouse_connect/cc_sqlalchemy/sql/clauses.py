from typing import Optional

from sqlalchemy import true
from sqlalchemy.sql.base import Immutable
from sqlalchemy.sql.selectable import FromClause, Join
from sqlalchemy.sql.visitors import InternalTraversal


# pylint: disable=protected-access,too-many-ancestors,abstract-method,unused-argument
class ArrayJoin(Immutable, FromClause):
    """Represents ClickHouse ARRAY JOIN clause"""

    __visit_name__ = "array_join"
    _is_from_container = True
    named_with_column = False
    _is_join = True

    def __init__(self, left, array_column, alias=None, is_left=False):
        """Initialize ARRAY JOIN clause

        Args:
            left: The left side (table or subquery)
            array_column: The array column to join
            alias: Optional alias for the joined array elements
            is_left: If True, use LEFT ARRAY JOIN instead of ARRAY JOIN
        """
        super().__init__()
        self.left = left
        self.array_column = array_column
        self.alias = alias
        self.is_left = is_left
        self._is_clone_of = None

    @property
    def selectable(self):
        """Return the selectable for this clause"""
        return self.left

    @property
    def _hide_froms(self):
        """Hide the left table from the FROM clause since it's part of the ARRAY JOIN"""
        return [self.left]

    @property
    def _from_objects(self):
        """Return all FROM objects referenced by this construct"""
        return self.left._from_objects

    def _clone(self, **kw):
        """Return a copy of this ArrayJoin"""
        c = self.__class__.__new__(self.__class__)
        c.__dict__ = self.__dict__.copy()
        c._is_clone_of = self
        return c

    def _copy_internals(self, clone=None, **kw):
        """Copy internal state for cloning

        This ensures that when queries are cloned (e.g., for subqueries, unions, or CTEs),
        the left FromClause and array_column references are properly deep-cloned.
        """
        def _default_clone(elem, **kwargs):
            return elem

        if clone is None:
            clone = _default_clone

        # Clone the left FromClause and array column to ensure proper
        #  reference handling in complex query scenarios
        self.left = clone(self.left, **kw)
        self.array_column = clone(self.array_column, **kw)


def array_join(left, array_column, alias=None, is_left=False):
    """Create an ARRAY JOIN clause

    Args:
        left: The left side (table or subquery)
        array_column: The array column to join
        alias: Optional alias for the joined array elements
        is_left: If True, use LEFT ARRAY JOIN instead of ARRAY JOIN

    Returns:
        ArrayJoin: An ArrayJoin clause element

    Example:
        from clickhouse_connect.cc_sqlalchemy.sql.clauses import array_join

        # Basic ARRAY JOIN
        query = select(table).select_from(array_join(table, table.c.tags))

        # LEFT ARRAY JOIN with alias
        query = select(table).select_from(
            array_join(table, table.c.tags, alias='tag', is_left=True)
        )
    """
    return ArrayJoin(left, array_column, alias, is_left)


_VALID_STRICTNESS = frozenset({None, "ALL", "ANY", "SEMI", "ANTI", "ASOF"})
_VALID_DISTRIBUTION = frozenset({None, "GLOBAL"})


# pylint: disable=too-many-ancestors,abstract-method
class ClickHouseJoin(Join):
    """A SQLAlchemy Join subclass that supports ClickHouse strictness and distribution modifiers.

    ClickHouse JOIN syntax: [GLOBAL] [ALL|ANY|SEMI|ANTI|ASOF] [INNER|LEFT|RIGHT|FULL|CROSS] JOIN

    Strictness modifiers control how multiple matches are handled:
        - ALL: return all matching rows (default, standard SQL behavior)
        - ANY: return only the first match per left row
        - SEMI: acts as an allowlist on join keys, no Cartesian product
        - ANTI: acts as a denylist on join keys, no Cartesian product
        - ASOF: time-series join, finds the closest match

    Distribution modifier:
        - GLOBAL: broadcasts the right table to all nodes in distributed queries

    Note: RIGHT JOIN is achieved by swapping table order, which is standard SQLAlchemy behavior.
    ASOF JOIN requires the last ON condition to be an inequality which is validated by
    the ClickHouse server, not here. Not all strictness/join type combinations are supported
    by every join algorithm and the server will report unsupported combinations.
    """

    __visit_name__ = "join"

    _traverse_internals = Join._traverse_internals + [
        ("strictness", InternalTraversal.dp_string),
        ("distribution", InternalTraversal.dp_string),
        ("_is_cross", InternalTraversal.dp_boolean),
    ]

    def __init__(self, left, right, onclause=None, isouter=False, full=False, strictness=None, distribution=None, _is_cross=False):
        if strictness is not None:
            strictness = strictness.upper()
        if distribution is not None:
            distribution = distribution.upper()

        if strictness not in _VALID_STRICTNESS:
            raise ValueError(f"Invalid strictness {strictness!r}. Must be one of: ALL, ANY, SEMI, ANTI, ASOF")
        if distribution not in _VALID_DISTRIBUTION:
            raise ValueError(f"Invalid distribution {distribution!r}. Must be: GLOBAL")
        if _is_cross and strictness is not None:
            raise ValueError("Strictness modifiers cannot be used with CROSS JOIN")
        if _is_cross and (isouter or full):
            raise ValueError("CROSS JOIN cannot be combined with isouter or full")
        if strictness in ("SEMI", "ANTI") and not isouter:
            raise ValueError(f"{strictness} JOIN requires isouter=True (LEFT) or swapped table order (RIGHT)")
        if strictness == "ASOF" and full:
            raise ValueError("ASOF is not supported with FULL joins")

        super().__init__(left, right, onclause, isouter, full)
        self.strictness = strictness
        self.distribution = distribution
        self._is_cross = _is_cross


def ch_join(
    left,
    right,
    onclause=None,
    *,
    isouter=False,
    full=False,
    cross=False,
    strictness: Optional[str] = None,
    distribution: Optional[str] = None,
):
    """Create a ClickHouse JOIN with optional strictness and distribution modifiers.

    Args:
        left: The left side table or selectable.
        right: The right side table or selectable.
        onclause: The ON clause expression. When omitted, SQLAlchemy will
            attempt to infer the join condition from foreign key relationships.
        isouter: If True, render a LEFT OUTER JOIN.
        full: If True, render a FULL OUTER JOIN.
        cross: If True, render a CROSS JOIN. Cannot be combined with an
            explicit onclause or strictness modifiers.
        strictness: ClickHouse strictness modifier — "ALL", "ANY", "SEMI", "ANTI", or "ASOF".
        distribution: ClickHouse distribution modifier "GLOBAL".

    Returns:
        ClickHouseJoin: A join element with ClickHouse modifiers.
    """
    if cross:
        if onclause is not None:
            raise ValueError("cross=True conflicts with an explicit onclause")
        onclause = true()
    return ClickHouseJoin(left, right, onclause, isouter, full,
                          strictness, distribution, _is_cross=cross)
