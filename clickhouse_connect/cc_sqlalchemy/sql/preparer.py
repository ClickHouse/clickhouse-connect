from sqlalchemy.sql.compiler import IdentifierPreparer

from clickhouse_connect.driver.binding import quote_identifier


class ChIdentifierPreparer(IdentifierPreparer):

    quote_identifier = staticmethod(quote_identifier)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Disable percent doubling for ClickHouse
        # ClickHouse doesn't use % for parameter placeholders, so doubling
        # breaks string literals containing % (e.g., formatDateTime format strings)
        self._double_percents = False

    def _requires_quotes(self, _value):
        return True
