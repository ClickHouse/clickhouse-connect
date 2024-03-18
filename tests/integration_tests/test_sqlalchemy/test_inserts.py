from pytest import fixture

import sqlalchemy as db
from sqlalchemy import MetaData
from sqlalchemy.engine import Engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session

from clickhouse_connect.cc_sqlalchemy.datatypes.sqltypes import LowCardinality, String, UInt64
from clickhouse_connect.cc_sqlalchemy.ddl.tableengine import engine_map
from clickhouse_connect.driver import Client


@fixture(scope='module', autouse=True, name='test_model')
def test_model_fixture(test_client: Client, test_engine: Engine, test_db: str, test_table_engine: str):
    if not test_client.min_version('22.6.1'):
        yield None
        return
    engine_cls = engine_map[test_table_engine]

    Base = declarative_base(metadata=MetaData(schema=test_db))  # pylint: disable=invalid-name

    class Model(Base):
        __tablename__ = 'insert_model'
        __table_args__ = (engine_cls(order_by=['test_name', 'value_1']),)
        test_name = db.Column(LowCardinality(String), primary_key=True)
        value_1 = db.Column(String)
        metric_2 = db.Column(UInt64)
        description = db.Column(String)

    test_engine.execute('DROP TABLE IF EXISTS insert_model')
    Base.metadata.create_all(test_engine)
    yield Model


def test_single_insert(test_engine: Engine, test_model):
    conn = test_engine.connect()
    conn.execute(db.insert(test_model).values(test_name='single_insert',
                                              value_1='v1',
                                              metric_2=25738,
                                              description='Single Desc'))
    conn.execute(db.insert(test_model), {'test_name': 'another_single_insert'})


def test_multiple_insert(test_engine: Engine, test_model):
    session = Session(test_engine)
    model_1 = test_model(test_name='multi_1',
                         value_1='v1',
                         metric_2=100,
                         description='First of Many')
    model_2 = test_model(test_name='multi_2',
                         value_1='v2',
                         metric_2=100,
                         description='Second of Many')
    model_3 = test_model(value_1='v7',
                         metric_2=77,
                         description='Third of Many',
                         test_name='odd_one')
    session.add(model_1)
    session.add(model_2)
    session.add(model_3)
    session.commit()


def test_bulk_insert(test_engine: Engine, test_model):
    session = Session(test_engine)
    model_1 = test_model(test_name='bulk_1',
                         value_1='v1',
                         metric_2=100,
                         description='First of Bulk')
    model_2 = test_model(test_name='bulk_2',
                         value_1='v2',
                         metric_2=100,
                         description='Second of Bulk')
    model_3 = test_model(value_1='vb78',
                         metric_2=528,
                         description='Third of Bulk',
                         test_name='bulk')
    session.bulk_save_objects([model_1, model_2, model_3])
    session.commit()
