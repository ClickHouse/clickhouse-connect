from clickhouse_connect import driver_name
from clickhouse_connect.cc_sqlalchemy.datatypes.base import schema_types
from clickhouse_connect.cc_sqlalchemy.sql import final
from clickhouse_connect.cc_sqlalchemy.sql.clauses import array_join, ArrayJoin
from clickhouse_connect.cc_sqlalchemy.ddl.dictionary import Dictionary

# pylint: disable=invalid-name
dialect_name = driver_name
ischema_names = schema_types

# Compatibility aliases for clickhouse-sqlalchemy
CH_DIALECT = dialect_name
ClickhouseDictionary = Dictionary

__all__ = ['dialect_name', 'CH_DIALECT', 'ischema_names', 'array_join', 'ArrayJoin', 'final', 'Dictionary', 'ClickhouseDictionary']
