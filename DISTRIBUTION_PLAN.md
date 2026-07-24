# Rust Codec Distribution Plan

How the Rust native codec is packaged, versioned, released, and supported across ch-core-rs, the Python binding, and clickhouse-connect.

## Decisions

- Users opt in with a single switch on the client. The Rust path uses the new, correct semantics. Known behavior differences from the Python codec are enumerated and documented. Features the Rust path does not implement fall back to the Python codec.
- ch-core-rs is open source on GitHub. It is not published to crates.io. Consumers depend on it as a git dependency pinned to a release tag. The repo is currently internal. Making it public is in progress and gates the first PyPI release.
- The core repo stays pure Rust with no language-specific dependencies. Each language client owns its binding in its own repo.
- The Python binding ships as a separate PyPI wheel, installed via the `rust` extra. It is not compiled into the clickhouse-connect wheel.
- Long term: deprecation warnings on the Python codec late in 1.x, Rust becomes the only codec in 2.0.

## Artifacts and repos

| Piece | Lives in | Ships as | Releases when |
|---|---|---|---|
| ch-core-rs | own public repo, pure Rust | git tags only | core logic changes |
| ch-core-py binding and the rustcodec.py seam | clickhouse-connect repo | `clickhouse-connect-core` wheel on PyPI, module name `_ch_core` | core repin or binding change |
| clickhouse-connect | clickhouse-connect repo | pure/cython package with a `rust` extra | driver features, seam or floor changes |

One repo can publish two PyPI artifacts. Living in the connect repo does not mean shipping in the connect wheel.

## The two seams

**Rust seam (ch-core-rs into the binding).** Compile time only. The core is statically linked into the extension when the wheel builds. Users never resolve it and no runtime skew is possible. Pin by git tag in ch-core-py's Cargo.toml and commit Cargo.lock for reproducible builds.

**Python seam (`clickhouse-connect-core` wheel and clickhouse-connect).** The only seam users see. clickhouse-connect pins a compatible range, for example `clickhouse-connect-core>=1.2,<1.3`. The pin floor encodes the oldest core wheel this driver knows how to drive. Bump the floor only when connect starts using a new binding capability.

## Packaging

- PyPI name `clickhouse-connect-core`, module name `_ch_core`. The name should be recognizable in a user's pip list.
- Install: `pip install clickhouse-connect[rust]`.
- Build with maturin, one wheel per platform per Python version (cp310 through cp314), matching the main package's matrix.
- Not abi3: the binding's hot paths use non-limited C API (`PyTuple_SET_ITEM`, `PyList_SET_ITEM`, presized dict construction) on purpose. Moving to the limited API would tax exactly the paths the codec exists to accelerate. Revisit only if the wheel matrix becomes a real maintenance cost.
- If the switch is enabled without the wheel installed, raise a clear error naming the install command.
- While ch-core-rs remains non-public, wheel builds need repo access from CI and the sdist cannot build for outside users. Making the repo public precedes the first PyPI release.

## Runtime handshake

- `_ch_core` exports a binding API version constant.
- rustcodec.py checks it at import. Too old raises a legible message naming the required `clickhouse-connect-core` version. Never a crash or silent misbehavior.
- The client's diagnostic output includes the `_ch_core` version alongside the driver version so bug reports arrive with both.

## Release workflows

**Core bugfix.** Fix in ch-core-rs, tag a patch release. In the connect repo bump the git tag in ch-core-py, publish a `clickhouse-connect-core` patch wheel. No clickhouse-connect release. Users run `pip install -U clickhouse-connect[rust]`.

**Transparent core improvement** (faster decode, internal wins). Same as a bugfix but a minor bump. Users get it for free with a wheel upgrade.

**User-facing core feature** (new setting, new type, new capability). Core minor bump, binding exposes it, `clickhouse-connect-core` minor release. Then a clickhouse-connect release that uses it and raises the pin floor. The connect release is the feature's public API.

**Driver-only change.** Normal clickhouse-connect release. The wheel is untouched.

## Issue handling

- Users file everything on clickhouse-connect. The core repo tracker is for maintainers and binding authors.
- If a root cause lands in ch-core-rs, fix it there but keep and close the loop in the original clickhouse-connect issue, noting the `clickhouse-connect-core` version that carries the fix.

## Core repo contract

- Semver on tags. No breaking changes on patch or minor.
- A changelog maintained per release. Downstream bindings in multiple languages will depend on reading it.
- No PyO3, napi, or other language-specific dependencies in the core crate. Language artifacts are built by the binding repos.
- CI tests the crate on supported platforms. It builds no wheels or language artifacts.

## Rollout

1. Next minor: ship the opt-in switch, the `rust` extra, the known-differences documentation, and the fallback behavior.
2. During 1.x: promote the Rust path as it proves out. Wheel-only releases carry core fixes and wins to opted-in users.
3. Late 1.x: DeprecationWarning on the Python codec path.
4. 2.0: Rust codec becomes the only codec. The differences list becomes the documented behavior.
