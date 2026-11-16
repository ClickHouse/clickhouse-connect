---
name: update-test-matrix
description: Refresh the shared ClickHouse server release series in .github/clickhouse-versions.json from currently supported releases. Use when the user wants to update CI coverage for new ClickHouse releases.
---

# Update Test Matrix

`.github/clickhouse-versions.json` owns the server matrix and its default. The reusable
`.github/workflows/clickhouse-versions.yml` exposes both to the main, Rust binding,
and core publishing workflows. Keep release-series tags such as `26.3`: patch
updates are intentional. New series enter CI through a reviewed manifest change.

## Select supported versions

Fetch https://raw.githubusercontent.com/ClickHouse/ClickHouse/master/SECURITY.md.
Collect the supported rows as `major.minor` strings.

Classify those series using ClickHouse release tags:

```bash
gh api 'repos/ClickHouse/ClickHouse/releases?per_page=100' --paginate --jq '.[].tag_name'
```

Tags ending in `-lts` identify LTS series; others end in `-stable`. The `x.3` and
`x.8` LTS convention is a sanity check. Paginate further if a supported series
has no release in the first page.

Take the union of the three latest supported series and the two latest supported
LTS series. An LTS series can be in both groups. Deduplicate and sort numerically
in ascending order.

## Update the manifest and local fallbacks

Set `matrix` to the selected series and `default` to the oldest selected LTS.
All single-version jobs and the main matrix's extra include entries consume this
default automatically. Keep their expressions and the dependency profiles intact.

If the default changes, also update the two `CLICKHOUSE_CONNECT_TEST_CH_VERSION`
fallbacks in `docker-compose.yml` and the standalone `ARG CLICKHOUSE_VERSION`
default in `.docker/clickhouse/single_node_tls/Dockerfile`. Compose and Docker
cannot read the JSON directly, so check these fallbacks when changing the default. TLS
must continue to use the same selected version as the ordinary server.

The scheduled `clickhouse_ci.yml` intentionally tests Cloud and `head`. Leave it
as is when refreshing the supported matrix.

## Validate and summarize

Validate the affected workflows with `actionlint` when available. Check that the
manifest has unique release-series entries and its default belongs to the matrix.
Check `docker compose config` with
the override unset and with each selected series: the ordinary image and TLS
build argument must select the same version.

Report the supported series and their classification, the old and new matrix,
the default, and any local fallback updates. Do not commit unless the user has
authorized a commit.
