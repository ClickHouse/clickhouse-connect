import re
from typing import Optional

from sqlalchemy import Table
from sqlalchemy.sql.compiler import RESERVED_WORDS

reserved_words = RESERVED_WORDS | set('index')
identifier_re = re.compile(r'^[a-zA-Z_][0-9a-zA-Z_]*$')


def quote_id(v: str) -> str:
    if v in reserved_words or not identifier_re.match(v):
        return f'`{v}`'
    return v


def full_table(table_name: str, schema: Optional[str] = None) -> str:
    if table_name.startswith('(') or '.' in table_name or not schema:
        return quote_id(table_name)
    return f'{quote_id(schema)}.{quote_id(table_name)}'


def format_table(table: Table):
    return full_table(table.name, table.schema)
