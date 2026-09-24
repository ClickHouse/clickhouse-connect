# ClickHouse Connect Core ChangeLog

This changelog covers the `clickhouse-connect-core` Python package, which provides the `_ch_core` Rust binding.
Its versions are independent of both `clickhouse-connect` and the bundled `ch-core-rs` crate.
Release dates are the first PyPI upload dates in UTC.
Driver integration changes and core version requirements belong in the [driver changelog](../../CHANGELOG.md).

## UNRELEASED

## 0.2.1, 2026-09-24

[PyPI release](https://pypi.org/project/clickhouse-connect-core/0.2.1/) | [Source](https://github.com/ClickHouse/clickhouse-connect/tree/5864cd3559244e1c730343389e996e4e80f08745/rust/ch-core-py)

### Improvements

- Added private, read-only column buffers for supported numeric, temporal, Array, and LowCardinality columns, including null masks. Exported views keep their decoded chunk alive after the batch or stream closes. These buffers let the driver construct NumPy and Pandas results without requiring PyArrow.

### Bug Fixes

- Musllinux wheels now compile for musl instead of GNU libc, fixing Rust codec imports on Alpine Linux and other musl-based systems.

### Compatibility

- Added column-buffer API 1 while retaining compatibility with drivers that use binding API 3. The bundled `ch-core-rs` remains at 0.2.0. The driver must adopt the new buffer API to remove its NumPy/Pandas dependency on PyArrow. Upgrading core alone does not change that requirement in older drivers.

## 0.2.0, 2026-09-02

[PyPI release](https://pypi.org/project/clickhouse-connect-core/0.2.0/) | [Source](https://github.com/ClickHouse/clickhouse-connect/tree/d1228f137093471f0f6184f97776897157f4e544/rust/ch-core-py)

### Improvements

- Added `MultiPoint` query decoding, insert encoding, and Arrow export, including `MultiPoint` values inside `Geometry` and containers. `MultiPoint` inserts require ClickHouse 26.8 or later.
- Geometry inserts now use the driver's Geometry member selection rules, including inside containers.

### Bug Fixes

- The bundled codec now rejects Native protocol revisions above its supported maximum of 54485 instead of attempting to decode or encode them.

### Compatibility

- Bumped the binding API from 2 to 3 and bundled `ch-core-rs` from 0.1.1 to [0.2.0](https://github.com/ClickHouse/ch-core-rs/blob/v0.2.0/CHANGELOG.md). The Geometry Arrow schema adds a `MultiPoint` child. This core release requires a driver that supports binding API 3.

## 0.1.0, 2026-08-12

[PyPI release](https://pypi.org/project/clickhouse-connect-core/0.1.0/) | [Source](https://github.com/ClickHouse/clickhouse-connect/tree/a2b48ed442ee4c8568873fb84ee50166eb4860dd/rust/ch-core-py)

### Improvements

- Initial experimental Rust binding, distributed separately from the driver and installed through `clickhouse-connect[rust]`.
- Added Native block and stream decoding, insert encoding, Python row and column conversion, and Arrow C Stream export.
- Published wheels for CPython 3.10 through 3.14 on Linux, macOS, and Windows.

### Compatibility

- Requires Python 3.10 or later. Provides binding API 2 and bundles `ch-core-rs` [0.1.1](https://github.com/ClickHouse/ch-core-rs/blob/v0.1.1/CHANGELOG.md), including its resumable streaming scan optimization.
