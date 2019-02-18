from dragonfly import dragonfly

single_result = [{'id': 1, 'value': 'ok'}]
double_result = [{'id': 1, 'value': 'ok'}, {'id': 2, 'value': 'ok'}]
unexpected_result = [{'id': 1, 'value': 'not ok'}]


class TestReadAdapter(dragonfly.DataSourceAdapter):
    def fetch(self, entity, metadata):
        if entity == "TestEntity":
            yield single_result
        elif entity == "OtherEntity":
            yield double_result
        else:
            yield unexpected_result

    def close(self):
        pass


class SimulatedDB():
    db = {}

    def upsert(self, id, value):
        SimulatedDB.db[id] = value

    def remove(self, id):
        del SimulatedDB.db[id]

    def __getitem__(self, key):
        return SimulatedDB.db[key]


class TestPersistenceAdapter(dragonfly.PersistenceAdapter):
    def __init__(self):
        self.client = SimulatedDB()

    def upsert(self, collection, record, metadata):
        self.client.upsert(record['id'], record['value'])

    def remove(self, collection, record, metadata=None):
        self.client.remove(record['id'])

    def close(self):
        pass


def test__reader_calls_underlying_adapter():
    data_reader = dragonfly.DefaultDataReader(TestReadAdapter())

    result = next(data_reader.read('TestEntity', {}))

    assert result[0]['id'] == single_result[0]['id']
    assert result[0]['value'] == 'ok'


def test__underlying_adapter_filters_entities_accordingly():
    data_reader = dragonfly.DefaultDataReader(TestReadAdapter())

    result = next(data_reader.read('OtherEntity', {}))

    assert len(result) == len(double_result)

    for i in range(len(result)):
        assert result[i]['id'] == (i + 1) and result[i]['value'] == 'ok'


def test__writer_calls_upsert_on_soft_delete():
    db = SimulatedDB()

    metadata = {'soft_delete': True, 'table': 'TestEntity'}
    data_reader = dragonfly.DefaultDataReader(TestReadAdapter())

    data_writer = dragonfly.DefaultDataWriter(TestPersistenceAdapter())
    data_writer.update('TestEntity', metadata,
                       data_reader.read('TestEntity', {}))

    assert db[1] == 'ok'


def test__writer_calls_remove_on_hard_delete():
    raise
