from sqlalchemy.sql.compiler import IdentifierPreparer

from clickhouse_connect.cc_sqlalchemy.sql import quote_id


class ChIdentifierPreparer(IdentifierPreparer):

    quote_identifier = staticmethod(quote_id)

    def _requires_quotes(self, _value):
        return True
