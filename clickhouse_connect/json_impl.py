import json as py_json
import logging
from collections import OrderedDict
from typing import Any

from clickhouse_connect.driver import options


def _numpy_json_default(obj: Any) -> Any:
    np = options.np
    if np is not None:
        if isinstance(obj, np.integer) and obj.dtype.kind in "iu":
            return int(obj)
        if isinstance(obj, np.floating):
            return float(obj)
        if isinstance(obj, np.bool_):
            return bool(obj)
        if isinstance(obj, np.ndarray) and obj.dtype.kind in "biufUO":
            return obj.tolist()
    pd = options.pd
    if pd is not None and obj is pd.NA:
        return None
    raise TypeError(f"Object of type {obj.__class__.__name__} is not JSON serializable")


try:
    import orjson

    def _orjson_to_json(obj: Any) -> bytes:
        return orjson.dumps(obj, default=_numpy_json_default)
except ImportError:
    orjson = None
    _orjson_to_json = None

try:
    import ujson

    def _ujson_to_json(obj: Any) -> bytes:
        try:
            return ujson.dumps(obj).encode()
        except TypeError:
            return ujson.dumps(obj, default=_numpy_json_default).encode()
except ImportError:
    ujson = None
    _ujson_to_json = None


_json_encoder = py_json.JSONEncoder(separators=(",", ":"), default=_numpy_json_default)


def _pyjson_to_json(obj: Any) -> bytes:
    return _json_encoder.encode(obj).encode()


logger = logging.getLogger(__name__)
_to_json = OrderedDict()
_to_json["orjson"] = _orjson_to_json if orjson else None
_to_json["ujson"] = _ujson_to_json if ujson else None
_to_json["python"] = _pyjson_to_json

any_to_json = _pyjson_to_json


def set_json_library(impl: str = None):
    global any_to_json
    if impl:
        func = _to_json.get(impl)
        if func:
            any_to_json = func
            return
        raise NotImplementedError(f"JSON library {impl} is not supported")
    for library, func in _to_json.items():
        if func:
            logger.debug("Using %s library for writing JSON byte strings", library)
            any_to_json = func
            break


set_json_library()
