import re

from superset.utils.core import GenericDataType
from clickhouse_connect.datatypes.registry import type_map


type_mapping = (
    (r'^(float|decimal|int|uint)', GenericDataType.NUMERIC),
    (r'^date', GenericDataType.TEMPORAL),
    (r'^bool', GenericDataType.BOOLEAN)
)


def map_generic_types():
    compiled = [(re.compile(pattern, re.IGNORECASE), gen_type) for pattern, gen_type in type_mapping]
    for name, ch_type_cls in type_map.items():
        for pattern, gen_type in compiled:
            match = pattern.match(name)
            if match:
                ch_type_cls.generic_type = gen_type
                break
        else:
            ch_type_cls.generic_type = GenericDataType.STRING





