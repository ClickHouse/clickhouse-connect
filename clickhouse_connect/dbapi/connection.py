import httpx

from clickhouse_connect.dbapi.cursor import Cursor
from clickhouse_connect.dbapi.exceptions import OperationalError
from clickhouse_connect.driver.rbparser import parse_response


def format_query(query):
    if ' FORMAT ' not in query:
        query += ' FORMAT RowBinaryWithNamesAndTypes'
    return {'query': query}


class Connection:
    def __init__(self, *args, **kwargs):
        https = kwargs.get('secure', 'false').lower() == 'true'
        port = kwargs.get('port')
        if not port:
            port = 8443 if https else 8123
        scheme = 'https' if https else 'http'
        self.url = '{}://{}:{}'.format(scheme, kwargs.get('host'), port)
        self.username = kwargs.get('username', 'default')
        self.password = kwargs.get('password', '')
        self.client = httpx.Client(http2=True)

    def close(self):
        self.client.close()

    def commit(self):
        pass

    def rollback(self):
        pass

    def cursor(self):
        return Cursor(self)

    def query(self, query):
        params = format_query(query)
        try:
            response = self.client.get(self.url,
                                       auth=(self.username, self.password),
                                       params=params)
            if response.status_code == 200:
                return parse_response(response.content)
            raise OperationalError("Bad status code: {}".format( response.status_code))
        except Exception:
            raise OperationalError("Error executing HTTP request {}".format(self.url))
