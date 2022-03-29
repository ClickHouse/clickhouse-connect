from typing import Dict, Type

from datatypes.base import ClickHouseType


class ChSqlaType:
    ch_type: ClickHouseType

    def __init_subclass__(cls, **kwargs):
        type_map[cls.__name__] = cls

    def compile(self):
        return self.ch_type.name


type_map: Dict[str, Type[ChSqlaType]]