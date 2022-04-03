from sqlalchemy.sql.ddl import DDL
from sqlalchemy.exc import ArgumentError


class CreateDatabase(DDL):
    def __init__(self, name:str, engine: str = None, zoo_path: str = None, shard_name: str = '{shard}',
                 replica_name: str = '{replica}'):
        if engine and engine not in ('Atomic', 'Lazy', 'Replicated'):
            raise ArgumentError(f'Unrecognized engine type {engine}')
        stmt = f'CREATE DATABASE {name}'
        if engine:
            stmt += f' Engine {engine}'
            if engine == 'Replicated':
                if not zoo_path:
                    raise ArgumentError('zoo_path is required for Replicated Database Engine')
                stmt += f" ('{zoo_path}', '{shard_name}', '{replica_name}'"
        super().__init__(stmt)


class DropDatabase(DDL):
    def __init__(self, name: str):
        super().__init__(f'DROP DATABASE {name}')
