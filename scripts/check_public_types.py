from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys


def run_verifytypes(package: str, python: str | None) -> dict:
    env = dict(os.environ)
    pyright = "pyright"
    cwd = None
    if python:
        bindir = os.path.dirname(os.path.abspath(python))
        venv = os.path.dirname(bindir)
        env["VIRTUAL_ENV"] = venv
        env["PATH"] = bindir + os.pathsep + env["PATH"]
        pyright = os.path.join(bindir, "pyright")
        # Run outside any source checkout so pyright can only resolve the installed
        # package, never a sibling ./clickhouse_connect on disk.
        cwd = venv
    proc = subprocess.run(
        [pyright, "--verifytypes", package, "--ignoreexternal", "--outputjson"],
        capture_output=True,
        text=True,
        env=env,
        cwd=cwd,
    )
    # pyright exits nonzero whenever completeness < 100%, which is expected here,
    # so parse the JSON regardless of the exit code.
    try:
        return json.loads(proc.stdout)
    except json.JSONDecodeError:
        sys.stderr.write(proc.stdout)
        sys.stderr.write(proc.stderr)
        raise SystemExit("could not parse pyright --verifytypes output") from None


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("package", nargs="?", default="clickhouse_connect")
    ap.add_argument(
        "--max-untyped",
        type=int,
        required=True,
        help="Max exported public symbols allowed to lack a complete type (the ratchet baseline)",
    )
    ap.add_argument("--python", help="Interpreter of the environment where the package is installed")
    ap.add_argument("--top", type=int, default=30, help="How many untyped public symbols to list")
    args = ap.parse_args()

    data = run_verifytypes(args.package, args.python)
    summary = data.get("typeCompleteness")
    if not summary:
        raise SystemExit(
            "pyright reported no typeCompleteness section. The package is likely not "
            "installed in the target environment, or its py.typed marker is missing."
        )

    # Confirm we measured the installed package, not a stray copy. pyright resolution is
    # sensitive, so verify the resolved py.typed lives under the target environment.
    if args.python:
        venv = os.path.dirname(os.path.dirname(os.path.abspath(args.python)))
        resolved = summary.get("pyTypedPath") or summary.get("packageRootDirectory") or ""
        if not resolved.startswith(venv):
            raise SystemExit(
                f"pyright resolved {args.package} at {resolved or '<unknown>'}, not under the "
                f"target environment {venv}. Refusing to report a score for the wrong package."
            )

    score = round(summary["completenessScore"] * 100, 2)
    counts = summary["exportedSymbolCounts"]

    symbols = summary.get("symbols", [])
    untyped = [s for s in symbols if s.get("isExported") and not s.get("isTypeKnown", True)]
    # Gate on pyright's authoritative summary counts rather than the per-symbol flag.
    count = counts["withAmbiguousType"] + counts["withUnknownType"]

    print(f"public type completeness: {score}%  ({count} untyped, baseline {args.max_untyped})")
    print(
        f"  exported symbols: known={counts['withKnownType']} ambiguous={counts['withAmbiguousType']} unknown={counts['withUnknownType']}"
    )
    if untyped:
        print(f"  top {min(args.top, count)} public symbols missing complete types:")
        for s in untyped[: args.top]:
            message = (s.get("diagnostics") or [{}])[0].get("message", "")
            diag = message.splitlines()[0] if message else "type is partially unknown"
            print(f"    - {s['name']}: {diag}")

    if count > args.max_untyped:
        print(
            f"\nFAIL: {count} public symbols lack a complete type, above the baseline of "
            f"{args.max_untyped}. Public API was likely added or changed without full "
            "annotations. Annotate it, or raise --max-untyped only with justification.",
            file=sys.stderr,
        )
        return 1
    if count < args.max_untyped:
        print(f"\nOK, and you can tighten the ratchet: lower --max-untyped to {count}.")
        return 0
    print("\nOK: no new untyped public API.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
