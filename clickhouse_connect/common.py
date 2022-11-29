from dataclasses import dataclass
from typing import Any, Sequence, Optional, Dict

import pkg_resources

from clickhouse_connect.driver.exceptions import ProgrammingError


def version():
    return pkg_resources.get_distribution('clickhouse-connect').version


@dataclass
class CommonSetting:
    name: str
    options: Sequence[Any]
    default: Any
    value: Optional[Any] = None


_common_settings: Dict[str, CommonSetting] = {}


def get_setting(name: str):
    setting = _common_settings.get(name)
    if setting is None:
        raise ProgrammingError(f'Unrecognized common setting {name}')
    return setting.value if setting.value is not None else setting.default


def set_setting(name: str, value: Any):
    setting = _common_settings.get(name)
    if setting is None:
        raise ProgrammingError(f'Unrecognized common setting {name}')
    if value not in setting.options:
        raise ProgrammingError(f'Unrecognized option {value} for setting {name})')
    if value == setting.default:
        setting.value = None
    else:
        setting.value = value


def _init_common(name: str, options: Sequence[Any], default: Any):
    _common_settings[name] = CommonSetting(name, options, default)


_init_common('autogenerate_session_id', (True, False), True)
_init_common('dict_parameter_format', ('json', 'map'), 'json')
_init_common('invalid_setting_action', ('send', 'drop'), 'drop')
