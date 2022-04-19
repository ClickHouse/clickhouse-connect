from typing import Optional

from clickhouse_connect.cc_sqlalchemy.datatypes.base import sqla_type_from_name


def full_table(table_name: str, schema: Optional[str]) -> str:
    if table_name.startswith('(') or '.' in table_name or not schema:
        return table_name
    return f'`{schema}`.`{table_name}`'


def get_columns(connection, table_name, schema=None, **_kwargs):
    result_set = connection.execute(f'DESCRIBE TABLE {full_table(table_name, schema)}')
    columns = []
    for row in result_set:
        sqla_type = sqla_type_from_name(row.type)
        col = {'name': row.name,
               'type': sqla_type,
               'nullable': sqla_type.nullable,
               'autoincrement': False,
               'default': row.default_expression,
               'default_type': row.default_type,
               'comment': row.comment,
               'codec_expression': row.codec_expression,
               'ttl_expression': row.ttl_expression}
        columns.append(col)
    return columns
