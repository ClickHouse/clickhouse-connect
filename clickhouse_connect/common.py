import sys
from dataclasses import dataclass
from typing import Any, Sequence, Optional, Dict

import pkg_resources

from clickhouse_connect.driver.exceptions import ProgrammingError

def version():
    try:
        return pkg_resources.get_distribution('clickhouse-connect').version
    except pkg_resources.ResolutionError:
        return 'development'


@dataclass
class CommonSetting:
    name: str
    options: Sequence[Any]
    default: Any
    value: Optional[Any] = None


_common_settings: Dict[str, CommonSetting] = {}


def build_client_name(client_name: str):
    product_name = get_setting('product_name')
    product_name = product_name.strip() + ' ' if product_name else ''
    client_name = client_name.strip() + ' ' if client_name else ''
    py_version = sys.version.split(' ', maxsplit=1)[0]
    return f'{client_name}{product_name}clickhouse-connect/{version()} (lv:py/{py_version}; os:{sys.platform})'


def get_setting(name: str):
    setting = _common_settings.get(name)
    if setting is None:
        raise ProgrammingError(f'Unrecognized common setting {name}')
    return setting.value if setting.value is not None else setting.default


def set_setting(name: str, value: Any):
    setting = _common_settings.get(name)
    if setting is None:
        raise ProgrammingError(f'Unrecognized common setting {name}')
    if setting.options and value not in setting.options:
        raise ProgrammingError(f'Unrecognized option {value} for setting {name})')
    if value == setting.default:
        setting.value = None
    else:
        setting.value = value


def _init_common(name: str, options: Sequence[Any], default: Any):
    _common_settings[name] = CommonSetting(name, options, default)


_init_common('autogenerate_session_id', (True, False), True)
_init_common('dict_parameter_format', ('json', 'map'), 'json')
_init_common('invalid_setting_action', ('send', 'drop', 'error'), 'error')
_init_common('product_name', (), '')
