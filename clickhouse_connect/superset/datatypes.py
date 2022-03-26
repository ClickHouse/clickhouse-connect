import re

from superset.utils.core import GenericDataType
from clickhouse_connect.datatypes.base import type_map
from clickhouse_connect.datatypes import fixed_string_format, uint64_format, ip_format, uuid_format

type_mapping = (
    (r'^(FLOAT|DECIMAL|INT|UINT)', GenericDataType.NUMERIC),
    (r'^DATE', GenericDataType.TEMPORAL),
    (r'^BOOL', GenericDataType.BOOLEAN)
)


def configure_types():
    fixed_string_format(fmt='str', encoding='utf8')
    uint64_format('signed')
    ip_format('string')
    uuid_format('string')
    compiled = [(re.compile(pattern), gen_type) for pattern, gen_type in type_mapping]
    for name, ch_type_cls in type_map.items():
        for pattern, gen_type in compiled:
            match = pattern.match(name)
            if match:
                ch_type_cls.generic_type = gen_type
                break
        else:
            ch_type_cls.generic_type = GenericDataType.STRING


