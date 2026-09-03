# Rust codec binding

This directory contains the PyO3 binding used by the optional ClickHouse
Connect Rust codec. The binding is published as the separate
`clickhouse-connect-core` wheel and provides the private `_ch_core` extension
module. ClickHouse Connect owns the supported Python API and checks binding
compatibility when a client selects the Rust codec.

For installation, configuration, supported operations, and known behavior
differences, see the [Rust codec documentation](../docs/rust-codec.mdx).

## Repository layout

| Path | Purpose |
|---|---|
| `Cargo.toml` | Rust workspace configuration |
| `Cargo.lock` | Reproducible dependency versions for binding builds |
| `ch-core-py/` | PyO3 binding crate and `clickhouse-connect-core` package metadata |
| `BINDING_ARCHITECTURE.md` | Binding ownership, memory, GIL, and streaming design |

The binding depends on `ch-core-rs` through the release tag pinned in
`ch-core-py/Cargo.toml`. Release and CI builds resolve that tag directly.

## Developing against a local core checkout

To develop against a local core checkout, add an untracked
`.cargo/config.toml` at this repository's root that patches the git source to
your working tree:

```toml
[patch."https://github.com/ClickHouse/ch-core-rs"]
ch-core-rs = { path = "/path/to/ch-core-rs" }
```

With the patch in place, builds compile the core working tree as-is, which is
the intended inner loop for core changes. Remove or ignore the patch to build
what the tag pins.

The committed `Cargo.lock` must record the git source and revision for
`ch-core-rs`. Any cargo resolution while the patch is active rewrites that
entry to the path form in your working tree. Discard those lock changes and
never commit them. To regenerate the lock legitimately, for example after a
repin, move the patch aside first:

```sh
mv .cargo/config.toml .cargo/config.toml.disabled
cd rust && CARGO_NET_GIT_FETCH_WITH_CLI=true cargo fetch
mv ../.cargo/config.toml.disabled ../.cargo/config.toml
```

## Build and test

```sh
uv pip install maturin pytest pyarrow
maturin develop --release -m rust/ch-core-py/Cargo.toml
python -m pytest rust/ch-core-py/tests/
```

The focused driver tests are:

```sh
python -m pytest \
  tests/unit_tests/test_driver/test_rustcodec.py \
  tests/unit_tests/test_driver/test_rustnumpy.py
```

With ClickHouse running on `localhost`, run the live integration coverage with:

```sh
python -m pytest tests/integration_tests/test_rust_codec.py
```

If the shell exports `VIRTUAL_ENV` pointing at a different environment than
the one on `PATH`, maturin installs into the exported one. Set
`VIRTUAL_ENV=/path/to/repo/.venv` explicitly when in doubt.

## Where to read next

- `BINDING_ARCHITECTURE.md`: the layered design, the intake and exit paths,
  GIL rules, the streaming pattern, and the practical performance guidance.
- The `ch-core-rs` repository: the core's own README, `ARCHITECTURE.md`,
  and `DECODER_CONTRACT.md`, the per-type wire and Arrow contract.
