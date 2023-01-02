from typing import Optional, Sequence, Dict, Any

from clickhouse_connect.driver import Client
from clickhouse_connect.driver.query import quote_identifier


def insert_file(client: Client,
                table: str,
                file_path: str,
                fmt: Optional[str] = None,
                column_names: Optional[Sequence[str]] = None,
                database: Optional[str] = None,
                settings: Optional[Dict[str, Any]] = None):
    full_table = f'{quote_identifier(database)}.{quote_identifier(table)}' if database else quote_identifier(table)
    if not fmt:
        fmt = 'CSV' if column_names else 'CSVWithNames'
    with open(file_path, 'rb') as file:
        client.raw_insert(full_table, column_names=column_names, insert_block=file, fmt=fmt, settings=settings)
