import pytest
from sqlalchemy import except_, intersect, literal, select, text, union
from sqlalchemy.engine import Engine


@pytest.mark.parametrize(
    "operator, left_count, right_count, expected",
    [
        pytest.param(union, 1, 1, [13], id="union"),
        pytest.param(intersect, 2, 2, [13], id="intersect"),
        pytest.param(except_, 2, 1, [], id="except"),
    ],
)
def test_compound_select_distinct_semantics(test_engine: Engine, operator, left_count, right_count, expected):
    left = select(literal(13).label("value")).select_from(text(f"numbers({left_count})"))
    right = select(literal(13).label("value")).select_from(text(f"numbers({right_count})"))

    with test_engine.connect() as conn:
        values = conn.execute(operator(left, right)).scalars().all()

    assert values == expected
