#!/usr/bin/env python -u
import os

import clickhouse_connect


#  You can use an -L ssh tunnel directly, but to avoid HTTPS certificate errors you must add the
#  `server_host_name` argument to the get_client method

#  This example uses the following ssh tunnel command
#  ssh -f -N -L 1443:play.clickhouse.com:443 <jump host user>@<jump host> -i <ssh private key file>
def direct_tunnel():
    client = clickhouse_connect.get_client(host='localhost',
                                           user='play',
                                           password='clickhouse',
                                           port=1443,
                                           secure=True,
                                           server_host_name='play.clickhouse.com')
    print(client.query('SHOW DATABASES').result_set)
    client.close()


#  This example uses the Python sshtunnel library to create an ssh tunnel as above but within your Python code
#  `pip install sshtunnel` is required.  See the sshtunnel documentation for additional configuration options
#  https://sshtunnel.readthedocs.io/en/latest/

try:
    import sshtunnel  # pylint: disable=wrong-import-position
except ImportError:
    pass


def create_tunnel():
    server = sshtunnel.SSHTunnelForwarder(
        (os.environ.get('CLICKHOUSE_TUNNEL_JUMP_HOST'), 22),  # Create an ssh tunnel to your jump host/port
        ssh_username=os.environ.get('CLICKHOUSE_TUNNEL_USER', 'ubuntu'),  # Set the user for the remote/jump host
        ssh_pkey=os.environ.get('CLICKHOUSE_TUNNEL_KEY_FILE', '~/.ssh/id_rsa'),  # The private key file to use
        ssh_private_key_password=('CLICKHOUSE_TUNNEL_KEY_PASSWORD', None), # Private key password
        remote_bind_address=('play.clickhouse.com', 443),  # The ClickHouse server and port you want to reach
        local_bind_address=('localhost', 1443)  # The local address and port to bind the tunnel to
    )
    server.start()

    client = clickhouse_connect.get_client(host='localhost',
                                           user='play',
                                           password='clickhouse',
                                           port=1443,
                                           secure=True,
                                           verify=True,
                                           server_host_name='play.clickhouse.com')
    print(client.query('SHOW DATABASES').result_set)
    client.close()
    server.close()


#  An example of how to use a "dynamic/SOCKS5" ssh tunnel to reach a ClickHouse server
#  The ssh tunnel for this example was created with the following command:
#  ssh -f -N -D 1443 <jump host user>@<jump host> -i <ssh private key file>

#  This example requires installing the pysocks library:
#  pip install pysocks
#
#  Documentation for the SocksProxyManager here:  https://urllib3.readthedocs.io/en/stable/reference/contrib/socks.html
#  Note there are limitations for the urllib3 SOCKSProxyManager,
from urllib3.contrib.socks import SOCKSProxyManager  # pylint: disable=wrong-import-position,wrong-import-order
from clickhouse_connect.driver import httputil  # pylint: disable=wrong-import-position


def socks_proxy():
    options = httputil.get_pool_manager_options()
    proxy_manager = SOCKSProxyManager('socks5h://localhost:1443', **options)

    client = clickhouse_connect.get_client(host='play.clickhouse.com',
                                           user='play',
                                           password='clickhouse',
                                           port=443,
                                           pool_mgr=proxy_manager)

    print(client.query('SHOW DATABASES').result_set)
    client.close()


# Uncomment the option you want to test for local testing of your tunnel

# direct_tunnel()
create_tunnel()
# socks_proxy()
