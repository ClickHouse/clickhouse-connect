import json
import os
import subprocess
import sys


def runtime_state(use_c: str | None = None):
    env = os.environ.copy()
    if use_c is None:
        env.pop("CLICKHOUSE_CONNECT_USE_C", None)
    else:
        env["CLICKHOUSE_CONNECT_USE_C"] = use_c
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            """
import json
from clickhouse_connect.driver.ctypes import RespBuffCls, data_conv, numpy_conv
print(json.dumps({
    "buffer": RespBuffCls.__module__,
    "data_conv": data_conv.__name__,
    "numpy_conv": numpy_conv.__name__,
}))
""",
        ],
        capture_output=True,
        check=True,
        env=env,
        text=True,
    )
    return json.loads(result.stdout)


def test_runtime_hook_enables_c_modules_by_default():
    assert runtime_state() == {
        "buffer": "clickhouse_connect.driverc.buffer",
        "data_conv": "clickhouse_connect.driverc.dataconv",
        "numpy_conv": "clickhouse_connect.driverc.npconv",
    }


def test_runtime_hook_can_be_disabled():
    assert runtime_state("0") == {
        "buffer": "clickhouse_connect.driver.buffer",
        "data_conv": "clickhouse_connect.driver.dataconv",
        "numpy_conv": "clickhouse_connect.driver.npconv",
    }
