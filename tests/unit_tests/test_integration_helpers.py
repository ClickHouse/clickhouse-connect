from types import SimpleNamespace

import pytest

from clickhouse_connect.driver.models import SettingDef
from tests.integration_tests.conftest import nullable_tuple_settings


@pytest.mark.parametrize("value", ["0", "1"])
@pytest.mark.parametrize("readonly", [0, 1])
def test_nullable_tuple_settings_available(value, readonly):
    name = "allow_experimental_nullable_tuple_type"
    client = SimpleNamespace(server_settings={name: SettingDef(name, value, readonly)})

    assert nullable_tuple_settings(client) == {name: 1}


def test_nullable_tuple_settings_unavailable():
    client = SimpleNamespace(server_settings={})

    with pytest.raises(pytest.skip.Exception, match=r"Server does not support Nullable\(Tuple"):
        nullable_tuple_settings(client)
