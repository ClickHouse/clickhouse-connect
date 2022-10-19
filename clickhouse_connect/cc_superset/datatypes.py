import re

from superset.utils.core import GenericDataType
from clickhouse_connect.cc_sqlalchemy.datatypes.base import sqla_type_map
from clickhouse_connect.datatypes.format import set_default_formats

type_mapping = (
    (r'^(FLOAT|DECIMAL|INT|UINT)', GenericDataType.NUMERIC),
    (r'^DATE', GenericDataType.TEMPORAL),
    (r'^BOOL', GenericDataType.BOOLEAN)
)


def configure_types():
    """
    Monkey patch the Superset generic_type onto the clickhouse type, also set defaults for certain type formatting to be
    better compatible with superset
    """
    set_default_formats('FixedString', 'string',
                        'IPv*', 'string',
                        'UInt64', 'signed',
                        'UUID', 'string',
                        '*Int256', 'string',
                        '*Int128', 'string')
    compiled = [(re.compile(pattern, re.IGNORECASE), gen_type) for pattern, gen_type in type_mapping]
    for name, sqla_type in sqla_type_map.items():
        for pattern, gen_type in compiled:
            match = pattern.match(name)
            if match:
                sqla_type.generic_type = gen_type
                break
        else:
            sqla_type.generic_type = GenericDataType.STRING
