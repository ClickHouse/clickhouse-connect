#!/usr/bin/env bash
# Runs the allowlisted upstream ClickHouse SQL tests through clickhouse-connect.
#
# Usage: scripts/run-upstream-tests.sh [<extra tests/clickhouse-test arguments>...]
#
# See ../README.md for the environment variables.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RUNNER_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
REPO_DIR="$(cd "${RUNNER_DIR}/../.." && pwd)"

UPSTREAM_CLICKHOUSE_DIR="${UPSTREAM_CLICKHOUSE_DIR:-${RUNNER_DIR}/.upstream/ClickHouse}"
UPSTREAM_TEST_LIST="${UPSTREAM_TEST_LIST:-${RUNNER_DIR}/allowlist.txt}"
CLICKHOUSE_TEST_RUNNER_PYTHON="${CLICKHOUSE_TEST_RUNNER_PYTHON:-python3}"
SHARD_INDEX="${SHARD_INDEX:-1}"
SHARD_TOTAL="${SHARD_TOTAL:-1}"
# clickhouse-connect speaks HTTP only. The protocol name selects the `only=<protocol>` lines of the
# allowlist.
protocol="http"

if [[ ! -f "${UPSTREAM_CLICKHOUSE_DIR}/tests/clickhouse-test" ]]; then
  echo "Error: ${UPSTREAM_CLICKHOUSE_DIR}/tests/clickhouse-test not found." >&2
  echo "Run scripts/fetch-upstream.sh, or set UPSTREAM_CLICKHOUSE_DIR to a checkout of ClickHouse/ClickHouse." >&2
  exit 1
fi

# The server and the tests must be of the same release: the `.reference` files of another release do
# not match the output of the server.
upstream_ref="$(cat "${UPSTREAM_CLICKHOUSE_DIR}/.upstream-ref" 2>/dev/null || echo unknown)"
server_host="${CLICKHOUSE_HOST:-localhost}"
server_url="http"'://'"${server_host}:${CLICKHOUSE_PORT_HTTP:-8123}/"
if ! server_version="$(curl -fsS --noproxy '*' --data-binary 'SELECT version()' "${server_url}")"; then
  echo "Error: cannot get the server version from ${server_url}. Start the server first." >&2
  exit 1
fi
if [[ "${upstream_ref}" != "v${server_version}-stable" && "${upstream_ref}" != "v${server_version}-lts" &&
  "${ALLOW_VERSION_MISMATCH:-0}" != "1" ]]; then
  echo "Error: the server is ${server_version}, but the upstream tests are ${upstream_ref}." >&2
  echo "Start the pinned server (see README.md) and run scripts/fetch-upstream.sh," >&2
  echo "or set ALLOW_VERSION_MISMATCH=1 to run anyway." >&2
  exit 1
fi
echo "Server ${server_version}, upstream tests ${upstream_ref}" >&2

# The shim runs clickhouse-connect from this checkout, with the Cython extensions that a normal install
# builds. `python setup.py build_ext --inplace` builds them; CLICKHOUSE_CONNECT_USE_C=0 runs the pure
# Python path instead.
if ! PYTHONPATH="${REPO_DIR}" "${CLICKHOUSE_TEST_RUNNER_PYTHON}" -c 'import clickhouse_connect' 2> /dev/null; then
  echo "Error: ${CLICKHOUSE_TEST_RUNNER_PYTHON} cannot import clickhouse_connect from ${REPO_DIR}. Run: pip install -e ." >&2
  exit 1
fi
if [[ "${CLICKHOUSE_CONNECT_USE_C:-1}" != "0" ]] &&
  ! PYTHONPATH="${REPO_DIR}" "${CLICKHOUSE_TEST_RUNNER_PYTHON}" -c 'import clickhouse_connect.driverc.dataconv' 2> /dev/null; then
  echo "Error: the Cython extensions of clickhouse-connect are not built. Run: python setup.py build_ext --inplace" >&2
  echo "(or set CLICKHOUSE_CONNECT_USE_C=0 to test the pure Python path)." >&2
  exit 1
fi

# Read the allowlist: one test name per line; '#' starts a comment.
#
# With KNOWN_FAILURES, read the known failures instead: the comment lines `# <test>  # [<id>]` of the
# groups in KNOWN_FAILURES (comma-separated IDs, or `all`). Use it to check whether a fix makes the
# tests of a group pass.
tests=()
if [[ -n "${KNOWN_FAILURES:-}" ]]; then
  while IFS= read -r line || [[ -n "${line}" ]]; do
    if [[ "${line}" =~ ^#[[:space:]]*([A-Za-z0-9_.-]+)[[:space:]]+#[[:space:]]*\[([a-z0-9-]+)\] ]]; then
      if [[ "${KNOWN_FAILURES}" == "all" || ",${KNOWN_FAILURES}," == *",${BASH_REMATCH[2]},"* ]]; then
        tests+=("${BASH_REMATCH[1]}")
      fi
    fi
  done < "${UPSTREAM_TEST_LIST}"
  echo "Selected ${#tests[@]} known failure(s) of ${KNOWN_FAILURES} from ${UPSTREAM_TEST_LIST}" >&2
else
  while IFS= read -r line || [[ -n "${line}" ]]; do
    # `<test>  # only=<protocol>: <reason>` runs over that protocol only.
    if [[ "${line}" =~ \#[[:space:]]*only=([a-z]+) && "${BASH_REMATCH[1]}" != "${protocol}" ]]; then
      continue
    fi
    line="${line%%#*}"
    line="${line#"${line%%[![:space:]]*}"}"
    line="${line%"${line##*[![:space:]]}"}"
    [[ -z "${line}" ]] && continue
    tests+=("${line}")
  done < "${UPSTREAM_TEST_LIST}"
  echo "Selected ${#tests[@]} test(s) from ${UPSTREAM_TEST_LIST}" >&2
fi

# Upstream renames and removes tests. A name that selects no test file runs nothing and fails nothing,
# so report it.
missing=()
for t in "${tests[@]}"; do
  if [[ ! -e "${UPSTREAM_CLICKHOUSE_DIR}/tests/queries/0_stateless/${t}.sql" &&
    ! -e "${UPSTREAM_CLICKHOUSE_DIR}/tests/queries/0_stateless/${t}.sql.j2" ]]; then
    missing+=("${t}")
  fi
done
if [[ ${#missing[@]} -gt 0 ]]; then
  msg="${#missing[@]} allowlisted test(s) are not in the upstream checkout (${upstream_ref}): ${missing[*]}"
  if [[ -n "${GITHUB_ACTIONS:-}" ]]; then echo "::warning::${msg}"; else echo "Warning: ${msg}" >&2; fi
fi

# Round-robin sharding: keep the tests at positions i where i % SHARD_TOTAL == SHARD_INDEX - 1.
if ! [[ "${SHARD_TOTAL}" =~ ^[1-9][0-9]*$ && "${SHARD_INDEX}" =~ ^[1-9][0-9]*$ ]] || ((SHARD_INDEX > SHARD_TOTAL)); then
  echo "Error: SHARD_INDEX and SHARD_TOTAL must be positive integers with SHARD_INDEX <= SHARD_TOTAL (got: ${SHARD_INDEX}/${SHARD_TOTAL})." >&2
  exit 1
fi
if ((SHARD_TOTAL > 1)); then
  sharded=()
  for i in "${!tests[@]}"; do
    if ((i % SHARD_TOTAL == SHARD_INDEX - 1)); then
      sharded+=("${tests[$i]}")
    fi
  done
  tests=("${sharded[@]}")
  echo "Shard ${SHARD_INDEX}/${SHARD_TOTAL}: ${#tests[@]} test(s)" >&2
fi

# tests/clickhouse-test runs ALL tests when it gets no test names.
if [[ ${#tests[@]} -eq 0 ]]; then
  echo "Error: no tests selected from ${UPSTREAM_TEST_LIST}. Refusing to run the full upstream suite." >&2
  exit 1
fi

# tests/clickhouse-test calls `clickhouse` and `clickhouse-client`; both names start the shim.
export PATH="${RUNNER_DIR}/bin:${PATH}"
export CLICKHOUSE_TEST_RUNNER_PYTHON
if [[ -n "${CLICKHOUSE_CLIENT_CLI_LOG:-}" ]]; then
  mkdir -p "$(dirname "${CLICKHOUSE_CLIENT_CLI_LOG}")"
  export CLICKHOUSE_CLIENT_CLI_LOG
fi

# shellcheck source=selectors.sh
source "${SCRIPT_DIR}/selectors.sh"
build_selectors "${tests[@]}"

cd "${UPSTREAM_CLICKHOUSE_DIR}"
runner=("${CLICKHOUSE_TEST_RUNNER_PYTHON}" tests/clickhouse-test --no-stateful --no-random-settings --no-random-merge-tree-settings)

retries="${RETRY_FAILED:-0}"
if ! [[ "${retries}" =~ ^[0-9]+$ ]]; then
  echo "Error: RETRY_FAILED must be a number of retries (got: '${retries}')." >&2
  exit 1
fi
if ((retries == 0)); then
  exec "${runner[@]}" "$@" -- "${selectors[@]}"
fi

# RETRY_FAILED=<n>: run the failed tests again, at most n times. A test that passes on a retry is
# reported as a warning; a test that fails on every attempt fails the run. Do not use this to grow or
# check the allowlist: a test that needs a retry is not reliable.

# run_attempt <log> runs the runner on the current selectors and sets attempt_status, attempt_failed and
# attempt_passed.
run_attempt() {
  local log="$1" t
  shift
  set +e
  "${runner[@]}" --max-failures-chain 1000000 "$@" -- "${selectors[@]}" 2>&1 | tee "${log}"
  attempt_status=${PIPESTATUS[0]}
  set -e
  attempt_failed=()
  while IFS= read -r t; do
    attempt_failed+=("${t}")
  done < <(sed -nE 's/^\[[ 0-9]+\/[ 0-9]+\] ([^ :]+): +\[ FAIL \].*/\1/p' "${log}" | sort -u)
  attempt_passed="$(sed -nE 's/^\[[ 0-9]+\/[ 0-9]+\] ([^ :]+): +\[ OK \].*/\1/p' "${log}" | sort -u | tr '\n' ' ')"
}

log="$(mktemp)"
trap 'rm -f "${log}"' EXIT
runner_args=("$@")
run_attempt "${log}" "${runner_args[@]}"
passed_on_retry=""
attempt=0
while ((attempt_status != 0 && attempt < retries)); do
  if [[ ${#attempt_failed[@]} -eq 0 ]]; then
    echo "Error: the run failed (exit code ${attempt_status}), but no test failed. See the output above." >&2
    exit "${attempt_status}"
  fi
  attempt=$((attempt + 1))
  echo "Retry ${attempt}/${retries}: ${#attempt_failed[@]} failed test(s): ${attempt_failed[*]}" >&2
  build_selectors "${attempt_failed[@]}"
  run_attempt "${log}" "${runner_args[@]}"
  passed_on_retry="${passed_on_retry}${attempt_passed}"
done

if [[ -n "${passed_on_retry}" ]]; then
  msg="These tests failed and then passed on a retry: ${passed_on_retry}"
  if [[ -n "${GITHUB_ACTIONS:-}" ]]; then echo "::warning::${msg}"; else echo "Warning: ${msg}" >&2; fi
fi
exit "${attempt_status}"
