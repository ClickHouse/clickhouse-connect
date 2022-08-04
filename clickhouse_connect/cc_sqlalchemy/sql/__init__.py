from typing import Optional

from sqlalchemy import Table


def quote_id(v: str) -> str:
    return f'`{v}`'


def full_table(table_name: str, schema: Optional[str] = None) -> str:
    if table_name.startswith('(') or '.' in table_name or not schema:
        return quote_id(table_name)
    return f'{quote_id(schema)}.{quote_id(table_name)}'


def format_table(table: Table):
    return full_table(table.name, table.schema)
