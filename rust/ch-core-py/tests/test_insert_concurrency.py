"""Caller-owned containers can change while the encoder reads them."""

import faulthandler
import os
import queue
import subprocess
import sys
import sysconfig
import threading
import time
import traceback

import pytest


@pytest.mark.parametrize(
    "type_name,mutation",
    [
        ("JSON", "dict"),
        ("Map(String, Int64)", "dict"),
        ("JSON", "nested_dict"),
        ("Map(String, Array(String))", "nested_dict"),
        ("Array(Array(String))", "nested_list"),
        ("String", "scalar"),
        ("Nullable(String)", "nullable"),
        ("LowCardinality(String)", "scalar"),
        (f"Enum8('user_1'=13, '{'user_2' * 40}'=79)", "scalar"),
        ("Int64", "integer"),
        ("Variant(String, Int64)", "variant"),
        ("Tuple(value String)", "named_tuple"),
        ("Tuple(String)", "positional_tuple"),
        ("JSON", "bytearray"),
    ],
)
def test_concurrent_container_mutation(type_name, mutation):
    pytest.importorskip("_ch_core")
    env = os.environ.copy()
    # An explicit PYTHON_GIL=0 would hide an extension that enables the GIL.
    env.pop("PYTHON_GIL", None)
    result = subprocess.run(
        [sys.executable, "-X", "faulthandler", "-W", "error::RuntimeWarning", __file__, type_name, mutation],
        capture_output=True,
        text=True,
        timeout=150,
        env=env,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr


def _exercise_concurrent_encoding(type_name, mutation):
    free_threaded = bool(sysconfig.get_config_var("Py_GIL_DISABLED"))
    if free_threaded:
        assert not sys._is_gil_enabled()
    import _ch_core

    if free_threaded:
        assert not sys._is_gil_enabled(), "Importing _ch_core enabled the GIL"

    row_count = 2000
    if mutation == "dict":
        template = {f"k{j}": f"v{j}" if type_name == "JSON" else j + 13 for j in range(4)}
        rows = [dict(template) for _ in range(row_count)]
    elif mutation == "nested_dict":
        rows = [{"items": ["user_1"]} for _ in range(row_count)]
    elif mutation == "nested_list":
        rows = [[["user_1"]] for _ in range(row_count)]
    elif mutation == "named_tuple":
        rows = [{"value": "user_1"} for _ in range(row_count)]
    elif mutation == "positional_tuple":
        rows = [["user_1"] for _ in range(row_count)]
    elif mutation == "bytearray":
        rows = ['{"value":"user_1"}'] + [bytearray(b'{"value":"user_1"}') for _ in range(row_count - 1)]
    elif mutation == "integer":
        rows = [10013] * row_count
    else:
        rows = ["user_1"] * row_count

    # A real hang dumps every thread before the parent times out.
    faulthandler.dump_traceback_later(120, exit=True)
    stop = threading.Event()
    start = threading.Barrier(2)
    failures = queue.SimpleQueue()
    counts = [0, 0]
    # Concurrent readers convoy on the dict locks under CPU contention, so one reader does fixed work.
    passes = 100
    min_duration = 1.0

    def writer():
        index = 0
        while not stop.is_set():
            slot = index % row_count
            # Create fresh strings in different allocation size classes.
            value = f"user_{index % 2 + 1}" * (1 if index % 2 == 0 else 40)
            row = rows[slot]
            if mutation == "dict":
                # Replace the key table without deleting entries. On free-threaded
                # builds a dict with deleted slots makes dict.copy() spin in sched_yield.
                row.clear()
                row.update(template)
            elif mutation in ("nested_dict", "nested_list"):
                key = "items" if mutation == "nested_dict" else 0
                # Resize a nested list, then discard its last caller-owned reference.
                row[key][:] = [value] * (index % 4 + 1)
                row[key] = [f"user_{index % 2 + 1}"]
            elif mutation == "named_tuple":
                row["value"] = value
            elif mutation == "positional_tuple":
                row[0] = value
            elif mutation == "bytearray":
                if slot:
                    # Alternate lengths on each visit to this same bytearray.
                    label = "user_1" if (index // row_count) % 2 == 0 else "USER_22"
                    row[:] = f'{{"value":"{label}"}}'.encode()
            elif mutation == "integer":
                rows[slot] = int(f"100{13 if index % 2 == 0 else 79}")
            elif mutation == "nullable":
                rows[slot] = value if index % 3 else None
            elif mutation == "variant":
                rows[slot] = value if index % 2 else int("10013")
            else:
                rows[slot] = value
            index += 1
            counts[0] += 1

    def reader():
        deadline = time.monotonic() + min_duration
        while counts[1] < passes or time.monotonic() < deadline:
            encoded = _ch_core.encode_native_block(["v"], [type_name], [rows], row_count, None)
            decoded = list(_ch_core.ColBatch.decode_native(encoded).column_data(0))
            assert len(decoded) == row_count
            if mutation == "dict":
                for row in decoded:
                    assert len(row) in (0, 4) and all(row[key] == template[key] for key in row), row
            elif mutation in ("nested_dict", "nested_list"):
                key = "items" if mutation == "nested_dict" else 0
                assert all(1 <= len(row[key]) <= 4 and set(row[key]) <= {"user_1", "user_2", "user_2" * 40} for row in decoded)
            elif mutation == "named_tuple":
                assert all(row["value"] in ("user_1", "user_2" * 40) for row in decoded)
            elif mutation == "positional_tuple":
                assert all(row[0] in ("user_1", "user_2" * 40) for row in decoded)
            elif mutation == "bytearray":
                assert all(row["value"] in ("user_1", "USER_22") for row in decoded)
            elif mutation == "integer":
                assert set(decoded) <= {10013, 10079}
            elif mutation == "nullable":
                assert set(decoded) <= {None, "user_1", "user_2" * 40}
            elif mutation == "variant":
                assert set(decoded) <= {10013, "user_1", "user_2" * 40}
            else:
                assert set(decoded) <= {"user_1", "user_2" * 40}
            counts[1] += 1
            if not free_threaded:
                # Release the GIL so the writer gets scheduled.
                time.sleep(0)
        stop.set()

    def run(target):
        try:
            start.wait(timeout=10)
            target()
        except BaseException:
            failures.put(traceback.format_exc())
            stop.set()

    threads = [threading.Thread(target=run, args=(target,), name=name) for name, target in (("writer", writer), ("reader", reader))]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    faulthandler.cancel_dump_traceback_later()
    errors = []
    while not failures.empty():
        errors.append(failures.get())
    assert not errors, "".join(errors)
    assert all(counts), f"Writer or reader made no progress: {counts}"
    if free_threaded:
        assert not sys._is_gil_enabled(), "Encoding enabled the GIL"


if __name__ == "__main__":
    _exercise_concurrent_encoding(*sys.argv[1:])
