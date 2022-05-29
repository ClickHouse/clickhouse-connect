import dev_config

SUPERSET_WEBSERVER_PORT = getattr(dev_config, 'SUPERSET_WEBSERVER_PORT', 8088)
SECRET_KEY = 'clickhouse_dev'
db_uri = getattr(dev_config, 'SQLALCHEMY_DATABASE_URI', None)
if db_uri:
    SQLALCHEMY_DATABASE_URI = db_uri
SIP_15_ENABLED = True

# Set this API key to enable Mapbox visualizations
MAPBOX_API_KEY = getattr(dev_config, 'MAPBOX_API_KEY', '')
PREFERRED_DATABASES = getattr(dev_config, 'PREFERRED_DATABASES', ['ClickHouse Connect', 'MySQL'])
