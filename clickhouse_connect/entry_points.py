#!/usr/bin/env python3

import sys
import pkg_resources

EXPECTED_EPS = {'sqlalchemy.dialects:clickhousedb',
                'sqlalchemy.dialects:clickhousedb.connect',
                'superset.db_engine_specs:clickhousedb'}


def validate_entrypoints():
    expected_eps = EXPECTED_EPS.copy()
    try:
        dist = pkg_resources.get_distribution('clickhouse-connect')
    except pkg_resources.DistributionNotFound:
        print ('\nClickHouse Connect package not found in this Python installation')
        return -1
    entry_map = dist.get_entry_map()
    print()
    for ep_group, entry_points in entry_map.items():
        print (ep_group)
        for entry_point in entry_points.values():
            print (f'    {entry_point.name}={entry_point.module_name}.{", ".join(entry_point.attrs)}')
            name = f'{ep_group}:{entry_point.name}'
            try:
                expected_eps.remove(name)
            except KeyError:
                print (f'\nUnexpected entry point {name} found')
                return -1
    if expected_eps:
        print()
        for name in expected_eps:
            print (f'Did not find expected ep {name}')
        return -1
    print ('\nEntrypoints correctly installed')
    return 0


if __name__ == '__main__':
    sys.exit(validate_entrypoints())
