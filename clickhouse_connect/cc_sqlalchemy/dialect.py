from sqlalchemy.engine.default import DefaultDialect

from clickhouse_connect import driver_name, dbapi
from clickhouse_connect.cc_sqlalchemy.datatypes.base import sqla_type_from_name
from clickhouse_connect.cc_sqlalchemy.ddl.compiler import ChDDLCompiler
from clickhouse_connect.cc_sqlalchemy import ischema_names


class ClickHouseDialect(DefaultDialect):
    name = driver_name
    driver = 'connect'

    default_schema_name = 'default'
    supports_native_decimal = True
    supports_native_boolean = True
    returns_unicode_strings = True
    postfetch_lastrowid = False
    ddl_compiler = ChDDLCompiler
    description_encoding = None
    max_identifier_length = 127
    ischema_names = ischema_names

    @classmethod
    def dbapi(cls):
        return dbapi

    def initialize(self, connection):
        pass

    @staticmethod
    def get_schema_names(connection, **_):
        return [row.name for row in connection.execute('SHOW DATABASES')]

    @staticmethod
    def has_database(connection, db_name):
        return (connection.execute(f"SELECT name FROM system.databases WHERE name = '{db_name}'")).rowcount > 0

    @staticmethod
    def get_table_names(connection, schema=None, **kw):
        st = 'SHOW TABLES'
        if schema:
            st += ' FROM ' + schema
        return [row.name for row in connection.execute(st)]

    def get_columns(self, connection, table_name, schema=None, **kwargs):
        if table_name.startswith('(') or '.' in table_name or not schema:
            table = table_name
        else:
            table = '.'.join((schema, table_name))
        rows = [(row.name, sqla_type_from_name(row.type)) for row in connection.execute(f'DESCRIBE TABLE {table}')]
        return [{'name': name,
                 'type': sqla_type,
                 'nullable': sqla_type.nullable,
                 'autoincrement': False} for name, sqla_type in rows]

    def get_primary_keys(self, connection, table_name, schema=None, **kwargs):
        return []

    def get_pk_constraint(self, conn, table_name, schema=None, **kw):
        return []

    def get_foreign_keys(self, connection, table_name, schema=None, **kw):
        return []

    def get_temp_table_names(self, connection, schema=None, **kw):
        return []

    def get_view_names(self, connection, schema=None, **kw):
        return []

    def get_temp_view_names(self, connection, schema=None, **kw):
        return []

    def get_view_definition(self, connection, view_name, schema=None, **kw):
        pass

    def get_indexes(self, connection, table_name, schema=None, **kw):
        return []

    def get_unique_constraints(self, connection, table_name, schema=None, **kw):
        return []

    def get_check_constraints(self, connection, table_name, schema=None, **kw):
        return []

    def has_table(self, connection, table_name, schema=None):
        if table_name.startswith('(') or '.' in table_name or not schema:
            table = table_name
        else:
            table = '.'.join((schema, table_name))
        rows = connection.execute(f'EXISTS TABLE {table}')
        return rows.next().result == 1

    def has_sequence(self, connection, sequence_name, schema=None):
        return False

    def do_begin_twophase(self, connection, xid):
        raise NotImplementedError

    def do_prepare_twophase(self, connection, xid):
        raise NotImplementedError

    def do_rollback_twophase(self, connection, xid, is_prepared=True, recover=False):
        raise NotImplementedError

    def do_commit_twophase(self, connection, xid, is_prepared=True, recover=False):
        raise NotImplementedError

    def do_recover_twophase(self, connection):
        raise NotImplementedError

    def set_isolation_level(self, dbapi_conn, level):
        pass

    def get_isolation_level(self, dbapi_conn):
        return None
