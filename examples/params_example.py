#!/usr/bin/env python3 -u

from datetime import datetime, timedelta

from clickhouse_connect.driver.binding import finalize_query

select_template = """
  SELECT
    formatDateTime(started_at, '%%m/%%d/%%Y', %(time_zone)s) AS date,
    formatDateTime(started_at, '%%I:%%M:%%S %%p', %(time_zone)s) AS time,
    format('{}path/link?name={}&dev_type={}', %(web_url)s, label, device_type) AS url,
    device_name,
    description
  FROM sessions
"""


def build_device_query(time_zone: str,
                       web_url: str,
                       client: str,
                       company_id: str = '',
                       device_id: str = '',
                       updated: bool = False,
                       start_time: datetime = None,
                       end_time: datetime = None):
    params = {'time_zone': time_zone,
              'web_url': web_url,
              'client': client
              }
    where_template = ' WHERE client = %(client)s'
    if company_id:
        where_template += ' AND company_id = %(company_id)s'
        params['company_id'] = company_id
    if device_id:
        where_template += ' AND dev_type = %(device_id)s'
        params['device_id'] = device_id
    if updated:
        where_template += ' AND updated = true'
    if start_time and end_time:
        where_template += ' AND started_at BETWEEN %(start_time)s AND %(end_time)s'
        params['start_time'] = start_time
        params['end_time'] = end_time
    full_query = select_template + where_template + ' ORDER BY started_at ASC'
    return finalize_query(full_query, params)


if __name__ == '__main__':
    start = datetime.now()
    end = start + timedelta(hours=1, minutes=20)
    print(build_device_query('UTC',
                             'https://example.com',

                             client='Client_0',
                             company_id='Company_1',
                             device_id='DEVICE_77',
                             start_time=start,
                             end_time=end
                             )
          )
