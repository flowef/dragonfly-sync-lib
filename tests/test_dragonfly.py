from dragonfly import dragonfly

single_result = [{'id': 1, 'value': 'ok', 'deleted': False}]
double_result = [{
    'id': 1,
    'value': 'ok',
    'deleted': True
}, {
    'id': 2,
    'value': 'ok',
    'deleted': False
}]
unexpected_result = [{'id': 1, 'value': 'not ok'}]


class DummyDataSource(dragonfly.DataSourceAdapter):
    def fetch(self, entity, metadata):
        if entity == "TestEntity":
            yield single_result
        elif entity == "OtherEntity":
            yield double_result
        else:
            yield unexpected_result

    def close(self):
        pass


class DummyDB():
    db = {}

    def upsert(self, id, value):
        DummyDB.db[id] = value

    def remove(self, id):
        if id in DummyDB.db:
            del DummyDB.db[id]

    def __getitem__(self, key):
        return DummyDB.db.get(key)


class DummyPersistence(dragonfly.PersistenceAdapter):
    def __init__(self):
        self.client = DummyDB()

    def upsert(self, collection, record, metadata):
        self.client.upsert(record['id'], record['value'])

    def remove(self, collection, record, metadata=None):
        self.client.remove(record['id'])

    def close(self):
        pass


def test__reader_calls_underlying_adapter():
    data_reader = dragonfly.DefaultDataReader(DummyDataSource())

    result = next(data_reader.read('TestEntity', {}))

    assert result[0]['id'] == single_result[0]['id']
    assert result[0]['value'] == 'ok'


def test__underlying_adapter_filters_entities_accordingly():
    data_reader = dragonfly.DefaultDataReader(DummyDataSource())

    result = next(data_reader.read('OtherEntity', {}))

    assert len(result) == len(double_result)

    for i in range(len(result)):
        assert result[i]['id'] == (i + 1) and result[i]['value'] == 'ok'


def test__writer_calls_upsert_on_soft_delete():
    db = DummyDB()

    metadata = {
        'soft_delete': True,
        'table': 'TestEntity',
        'delete_flag': 'deleted'
    }
    data_reader = dragonfly.DefaultDataReader(DummyDataSource())
    data_writer = dragonfly.DefaultDataWriter(DummyPersistence())
    records = next(data_reader.read('TestEntity', {}))
    data_writer.update('TestEntity', metadata, records)

    assert db[1] == 'ok'

    records = next(data_reader.read('OtherEntity', {}))
    data_writer.update('TestEntity', metadata, records)

    assert db[1] == 'ok'
    assert db[2] == 'ok'


def test__writer_calls_remove_on_hard_delete():
    db = DummyDB()

    metadata = {
        'soft_delete': False,
        'table': 'TestEntity',
        'delete_flag': 'deleted'
    }
    data_reader = dragonfly.DefaultDataReader(DummyDataSource())
    data_writer = dragonfly.DefaultDataWriter(DummyPersistence())
    records = next(data_reader.read('TestEntity', {}))
    data_writer.update('TestEntity', metadata, records)

    assert db[1] == 'ok'

    records = next(data_reader.read('OtherEntity', {}))
    data_writer.update('TestEntity', metadata, records)

    assert db[1] is None
    assert db[2] == 'ok'
