from clickhouse_connect.driver.exceptions import NotSupportedError


def _pd_time_test(arr_or_dtype):
    kind = getattr(getattr(arr_or_dtype, "dtype", arr_or_dtype), "kind", None)
    return kind in ("M", "m")


pd_time_test = None

try:
    import numpy as np
except ImportError:
    np = None

try:
    import pandas as pd

    _pandas_version = tuple(int(x) for x in pd.__version__.split(".")[:2])
    if _pandas_version < (2, 0):
        pd = None

    else:
        pd_time_test = _pd_time_test

except ImportError:
    pd = None

try:
    import pyarrow as arrow
except ImportError:
    arrow = None

try:
    import polars as pl
except ImportError:
    pl = None

def check_numpy():
    if np:
        return np
    raise NotSupportedError('Numpy package is not installed')


def check_pandas():
    if pd:
        return pd
    try:
        import pandas as _pd  # pylint: disable=import-outside-toplevel
        raise NotSupportedError(
            f"pandas >= 2.0 is required, found {_pd.__version__}. "
            "Please upgrade: pip install 'pandas>=2'"
        )
    except ImportError as exc:
        raise NotSupportedError("Pandas package is not installed") from exc


def check_arrow():
    if arrow:
        return arrow
    raise NotSupportedError('PyArrow package is not installed')


def check_polars():
    if pl:
        return pl
    raise NotSupportedError("Polars package is not installed")
