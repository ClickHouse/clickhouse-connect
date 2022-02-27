import re

from superset.utils.core import GenericDataType
from clickhouse_connect.datatypes.registry import type_map


type_mapping = (
    (r'^(FLOAT|DECIMAL|INT|UINT)', GenericDataType.NUMERIC),
    (r'^DATE', GenericDataType.TEMPORAL),
    (r'^BOOL', GenericDataType.BOOLEAN)
)


def map_generic_types():
    import clickhouse_connect.sqlalchemy # Hack to ensure sqlachemy type information is always imported
    compiled = [(re.compile(pattern), gen_type) for pattern, gen_type in type_mapping]
    for name, ch_type_cls in type_map.items():
        for pattern, gen_type in compiled:
            match = pattern.match(name)
            if match:
                ch_type_cls.generic_type = gen_type
                break
        else:
            ch_type_cls.generic_type = GenericDataType.STRING





