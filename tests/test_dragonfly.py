import json

from dragonfly import dragonfly

unexpected_result = [{'id': 1, 'value': 'not ok'}]


class DummyDataSource(dragonfly.DataSourceAdapter):
    def fetch(self, entity, metadata):
        with open('tests/testbase.json') as first:
            yield json.load(first)
        with open('tests/update.json') as update:
            yield json.load(update)

    def close(self, *args):
        pass


class DummyDB():
    db = {}

    def upsert(id, value):
        DummyDB.db[id] = value

    def remove(id):
        if id in DummyDB.db:
            del DummyDB.db[id]


class DummyPersistence(dragonfly.PersistenceAdapter):
    def upsert(self, collection, record, metadata):
        DummyDB.upsert(record['id'], record['value'])

    def remove(self, collection, record, metadata=None):
        DummyDB.remove(record['id'])

    def close(self, *args):
        pass


def test__reader_calls_underlying_adapter():
    data_reader = dragonfly.DefaultDataReader(DummyDataSource())

    reads = data_reader.read('_', {})
    result = next(reads)

    assert result[0]['id'] == 1
    assert result[0]['value'] == 'ok'


def test__writer_calls_upsert_on_soft_delete():
    metadata = {
        'soft_delete': True,
        'table': 'TestEntity',
        'delete_flag': 'deleted'
    }
    data_reader = dragonfly.DefaultDataReader(DummyDataSource())
    data_writer = dragonfly.DefaultDataWriter(DummyPersistence())
    reads = data_reader.read('_', {})
    records = next(reads)
    data_writer.update('_', metadata, records)

    assert DummyDB.db.get(1) == 'ok'

    records = next(reads)
    data_writer.update('_', metadata, records)

    assert DummyDB.db.get(1) == 'ok'
    assert DummyDB.db.get(2) == 'ok'


def test__writer_calls_remove_on_hard_delete():
    metadata = {
        'soft_delete': False,
        'table': 'TestEntity',
        'delete_flag': 'deleted'
    }
    data_reader = dragonfly.DefaultDataReader(DummyDataSource())
    data_writer = dragonfly.DefaultDataWriter(DummyPersistence())
    reads = data_reader.read('_', {})
    records = next(reads)
    data_writer.update('_', metadata, records)

    assert DummyDB.db.get(1) == 'ok'

    records = next(reads)
    data_writer.update('_', metadata, records)

    assert DummyDB.db.get(1) is None
    assert DummyDB.db.get(2) == 'ok'


def test__sync_client_produces_correct_record_count():
    source = dragonfly.DefaultDataReader(DummyDataSource())
    destination = dragonfly.DefaultDataWriter(DummyPersistence())
    with dragonfly.Sync('tests/test.yaml') as sync:
        records = sync(source, destination)

    assert len(DummyDB.db) == 2
    assert records == 3
