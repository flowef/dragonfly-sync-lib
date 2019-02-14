# MIT License
#
# Copyright (c) 2019 Flow Executive Finders
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
#
import abc
import logging
from datetime import datetime

import yaml

from dragonfly import util


class DataIO(abc.ABC):
    @abc.abstractmethod
    def close(self, *args, **kwargs):
        pass


class DataSourceAdapter(DataIO):
    @abc.abstractmethod
    def fetch(self, entity: str, metadata: dict):
        pass


class PersistenceAdapter(DataIO):
    @abc.abstractmethod
    def upsert(self, collection: str, record: dict, metadata: dict):
        pass

    @abc.abstractmethod
    def remove(self, collection: str, record: dict, metadata: dict = None):
        pass


class DataReader(DataIO):
    def __init__(self, ds_adapter: DataSourceAdapter):
        self.data_source = ds_adapter

    def __call__(self, entity_name: str, metadata: dict):
        return self.read(entity_name, metadata)

    @abc.abstractmethod
    def read(self, entity_name: str, metadata: dict):
        pass

    def close(self, *args, **kwargs):
        self.data_source.close(*args, **kwargs)


class DataWriter(DataIO):
    def __init__(self, db_adapter: PersistenceAdapter):
        self.client = db_adapter

    def __call__(self, entity_name: str, metadata: dict, datasource: dict):
        self.update(entity_name, metadata, datasource)

    @abc.abstractmethod
    def update(self, entity_name: str, metadata: dict, datasource: dict):
        pass

    def close(self, *args, **kwargs):
        self.client.close(*args, **kwargs)


class DefaultDataReader(DataReader):
    def read(self, entity_name: str, metadata: dict):
        return self.data_source.fetch(entity_name, metadata)


class DefaultDataWriter(DataWriter):
    def update(self, entity_name: str, metadata: dict, datasource: dict):
        logging.debug(f"update {entity_name}: process started...")
        for row in datasource:
            if (not metadata['soft_delete'] and row.get('isDeleted')):
                self.client.remove(metadata['table'], row)
            else:
                self.client.upsert(metadata['table'], row, metadata)
        logging.debug(f"update {entity_name}: process finished!")


class DataSyncClient():
    def __init__(self, reader: DataWriter, writer: DataWriter):
        self.read = reader
        self.write = writer

    def sync(self, entity_name: str, metadata: dict) -> int:
        logging.debug(f"Started syncing of {entity_name}")
        count = 0
        for batch in self.read(entity_name, metadata):
            self.write(entity_name, metadata, batch)
            count += len(batch)
        return count

    def __enter__(self):
        return self.sync

    def __exit__(self, *args):
        self.read.close()
        self.write.close()


class Sync:
    def __init__(self, config_filename: str, reader: DataReader,
                 writer: DataWriter):
        self.config_filename = config_filename
        self.reader = reader
        self.writer = writer

        with open(config_filename) as stream:
            self.config = yaml.load(stream)

    def __update_config(self):
        with open(self.config_filename, 'w') as stream:
            yaml.dump(self.config, stream, **self.config['meta'])

    def run(self, *args, **kwargs):
        start_time = datetime.now()
        records_synced = 0

        with DataSyncClient(self.reader, self.writer) as sync:
            for entity, metadata in self.config['entities'].items():
                records_synced += sync(entity, metadata)
                metadata['last_sync'] = util.to_lucene(start_time)

        self.__update_config()

        return records_synced

    def __call__(self, *args, **kwargs):
        return self.run(*args, **kwargs)
