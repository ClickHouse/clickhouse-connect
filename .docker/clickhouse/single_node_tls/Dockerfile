# Keep the standalone default aligned with .github/clickhouse-versions.json.
ARG CLICKHOUSE_VERSION=26.3
FROM clickhouse/clickhouse-server:${CLICKHOUSE_VERSION}
COPY .docker/clickhouse/single_node_tls/certificates /etc/clickhouse-server/certs
RUN chown clickhouse:clickhouse -R /etc/clickhouse-server/certs \
    && chmod 600 /etc/clickhouse-server/certs/* \
    && chmod 755 /etc/clickhouse-server/certs
