from sqlalchemy.exc import DBAPIError

from click_alchemy.cursor import Cursor

import httpx

from click_alchemy.driver.rbparser import parse_response


def format_query(query):
    if ' FORMAT ' not in query:
        query += ' FORMAT RowBinaryWithNamesAndTypes'
    return {'query': query}


class Connection:
    def __init__(self, *args, **kwargs):
        self.url = 'http://{}:{}'.format(kwargs.get('host'), kwargs.get('port', 8123))
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
            raise DBAPIError(query, params, Exception("Bad status code: {}".format( response.status_code)))
        except Exception as ex:
            raise DBAPIError(query, params, ex)
