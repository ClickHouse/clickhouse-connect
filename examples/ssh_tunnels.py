#!/usr/bin/env python -u

#  An example of how to use a "dynamic/SOCKS5" ssh tunnel to reach a ClickHouse server
#  The ssh tunnel for this example was created with the following command:
#  ssh -f -N -D 1443 <jump host user>@<jump host> -i <ssh private key file>

#  This example requires installing the pysocks library:
#  pip install pysocks
#
#  Documentation for the SocksProxyManager here:  https://urllib3.readthedocs.io/en/stable/reference/contrib/socks.html
#  Note there are limitations for the urllib3 SOCKSProxyManager,

from urllib3.contrib.socks import SOCKSProxyManager

import clickhouse_connect
from clickhouse_connect.driver import httputil

options = httputil.get_pool_manager_options()
proxy_manager = SOCKSProxyManager('socks5h://localhost:1443', **options)

client = clickhouse_connect.get_client(host='play.clickhouse.com',
                                       user='play',
                                       password='clickhouse',
                                       port=443,
                                       pool_mgr=proxy_manager)

print(client.query('SHOW DATABASES').result_set)
client.close()
