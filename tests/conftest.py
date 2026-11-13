import os
import time
from datetime import timezone

import pytest

from clickhouse_connect.datatypes.format import clear_all_formats
from clickhouse_connect.driver import tzutil

os.environ["TZ"] = "UTC"
if hasattr(time, "tzset"):
    time.tzset()


@pytest.fixture(autouse=True)
def clean_global_state():
    clear_all_formats()
    tzutil.local_tz = timezone.utc


@pytest.fixture(
    params=[
        # the server reads a block comment as a token separator, so this is a SELECT and query_limit applies
        ("SELECT/*c*/number FROM numbers(9)", [(0,), (1,)]),
        # the query already has a LIMIT, so the client must not append its own
        ("SELECT number FROM numbers(9)/*c*/LIMIT 1", [(0,)]),
        ("SELECT number FROM numbers(9) LIMIT/*c*/1", [(0,)]),
        # comments of the server lexer hide a LIMIT, so query_limit applies
        ("SELECT number FROM numbers(9) // LIMIT 5", [(0,), (1,)]),
        ("SELECT number FROM numbers(9) # LIMIT 5", [(0,), (1,)]),
        ("SELECT number FROM numbers(9) #! LIMIT 5", [(0,), (1,)]),
        ("SELECT number FROM numbers(9) // LIMIT 0", [(0,), (1,)]),
        ("SELECT number FROM numbers(9) /* a /* b */ LIMIT 0 */", [(0,), (1,)]),
        # quoted text is not a clause, so a real LIMIT after it still counts and a LIMIT inside it does not
        ("SELECT number AS `a--b` FROM numbers(9) LIMIT 1", [(0,)]),
        ("SELECT number, $$--$$ AS tag FROM numbers(9) LIMIT 1", [(0, "--")]),
        ("SELECT number FROM numbers(9) WHERE toString(number) != 'a\\'b-- LIMIT 0' LIMIT 1", [(0,)]),
        ("SELECT number FROM numbers(9) WHERE toString(number) != ' LIMIT 1'", [(0,), (1,)]),
        ("SELECT number AS `LIMIT 0--` FROM numbers(9)", [(0,), (1,)]),
        ("SELECT number FROM numbers(9) LIMIT 1", [(0,)]),
    ]
)
def query_limit_comment_case(request):
    return request.param
