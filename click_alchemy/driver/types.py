from typing import Callable

from click_alchemy.driver import ClickHouseType, type_map
from click_alchemy.driver.parser import leb128_string


def register(name: str, to_python: Callable, **kwargs):
    type_map[name] = ClickHouseType(name, to_python, **kwargs)


register('String', leb128_string)
register('UInt8', lambda source, loc: (int.from_bytes(source[loc:loc + 1], 'little'), loc + 1))
register('UInt16', lambda source, loc: (int.from_bytes(source[loc:loc + 2], 'little'), loc + 2))
