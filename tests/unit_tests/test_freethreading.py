import os
import subprocess
import sys
import sysconfig

import pytest

pytest.importorskip("clickhouse_connect.driverc.buffer")

CHECK_SCRIPT = """
import sys
import warnings

warnings.filterwarnings("error", category=RuntimeWarning)
import clickhouse_connect.driverc.buffer
import clickhouse_connect.driverc.dataconv
import clickhouse_connect.driverc.npconv

sys.exit(0 if not sys._is_gil_enabled() else 1)
"""


@pytest.mark.skipif(
    sysconfig.get_config_var("Py_GIL_DISABLED") != 1,
    reason="requires a free-threaded Python build",
)
def test_c_modules_keep_gil_disabled():
    # Strip PYTHON_GIL so the check reflects the module declarations, not a CI override
    env = {k: v for k, v in os.environ.items() if k != "PYTHON_GIL"}
    result = subprocess.run([sys.executable, "-c", CHECK_SCRIPT], capture_output=True, text=True, check=False, env=env)
    assert result.returncode == 0, result.stderr
