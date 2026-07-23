# clickhouse-connect-core

Compiled Rust codec for [clickhouse-connect](https://pypi.org/project/clickhouse-connect/). This wheel provides the `_ch_core` extension module that decodes ClickHouse `FORMAT Native` query results and encodes inserts.

This package is not meant to be used directly. Install it through the `rust` extra of the main driver:

```bash
pip install "clickhouse-connect[rust]"
```

Then enable the codec on a client:

```python
import clickhouse_connect

client = clickhouse_connect.get_client(host="localhost", native_codec="rust")
```

The codec is experimental and opt in. See the [clickhouse-connect documentation](https://clickhouse.com/docs/integrations/language-clients/python/rust-codec) for supported values, fallback rules, and version compatibility.

The `_ch_core` module has no public API contract. Its interface exists solely for the clickhouse-connect driver, which checks a binding API version at client creation and tells you when this wheel needs an upgrade.

Issues and source live in the [clickhouse-connect repository](https://github.com/ClickHouse/clickhouse-connect) under `rust/ch-core-py`. The decoding core itself is [ch-core-rs](https://github.com/ClickHouse/ch-core-rs).
