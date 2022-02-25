from sqlalchemy.engine.default import DefaultDialect

from clickhouse_connect import driver_name
from clickhouse_connect.dbapi import connector
from clickhouse_connect.datatypes import registry
from clickhouse_connect.sqlalchemy.datatypes import map_schema_types
from clickhouse_connect.superset.datatypes import map_generic_types

map_generic_types()


class ClickHouseDialect(DefaultDialect):
    name = driver_name

    returns_unicode_strings = True
    default_schema_name = 'default'
    description_encoding = None
    max_identifier_length = 127
    ischema_names = map_schema_types()

    @classmethod
    def dbapi(cls):
        return connector

    def initialize(self, connection):
        pass

    def get_schema_names(self, connection, **kwargs):
        return [row.name for row in connection.execute('SHOW DATABASES')]

    def get_table_names(self, connection, schema=None, **kw):
        st = 'SHOW TABLES'
        if schema:
            st += ' FROM ' + schema
        return [row.name for row in connection.execute(st)]

    def get_columns(self, connection, table_name, schema=None, **kwargs):
        if table_name.startswith('(') or '.' in table_name or not schema:
            table = table_name
        else:
            table = '.'.join((schema, table_name))
        rows = connection.execute('DESCRIBE TABLE {}'.format(table))
        return [{'name': row.name, 'type': registry.get_from_name(row.type).get_sqla_type()} for row in rows]

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
        rows = connection.execute('EXISTS TABLE {}'.format(table))
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
        raise NotImplementedError

    def get_isolation_level(self, dbapi_conn):
        return None

