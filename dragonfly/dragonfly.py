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
import importlib
import logging
from datetime import datetime

import yaml

from dragonfly import util


class DataReader(abc.ABC):
    @abc.abstractmethod
    def fetch(self, entity: str, metadata: dict):
        pass


class PersistenceAdapter(abc.ABC):
    @abc.abstractmethod
    def upsert(self, collection: str, record: dict, metadata: dict):
        pass

    @abc.abstractmethod
    def remove(self, collection: str, record: dict, metadata: dict = None):
        pass

    @abc.abstractmethod
    def close(self):
        pass


class DataWriter():
    def __init__(self, db_adapter: PersistenceAdapter):
        self.client = db_adapter

    def update(self, entity_name, metadata, datasource):
        logging.debug(f"update {entity_name}: process started...")
        for row in datasource:
            logging.debug(row)
            if (not metadata['soft_delete'] and row.get('isDeleted')):
                self.client.remove(metadata['table'], row)
            else:
                self.client.upsert(metadata['table'], row, metadata)
        logging.debug(f"update {entity_name}: process finished!")

    def close(self):
        self.client.close()


class DataSyncClient():
    def __init__(self, source: DataReader, destination: DataWriter):
        self.__source = source
        self.__destination = destination

    def sync(self, entity_name: str, metadata: dict) -> int:
        logging.debug(f"Started syncing of {entity_name}")
        count = 0
        for batch in self.__source.fetch(entity_name, metadata):
            self.__destination.update(entity_name, metadata, batch)
            count += len(batch)
        return count

    def __enter__(self):
        return self.sync

    def __exit__(self, *args):
        self.__destination.close()


DATABASE_CONFIG = 'database'
ENTITIES_CONFIG = 'entities'
META_CONFIG = 'meta'
LAST_SYNC = 'last_sync'


class Sync:
    def __init__(self, config_filename, reader_args={}, adapter_args={}):
        self.start_time = datetime.now()
        self.config_filename = config_filename
        with open(config_filename) as stream:
            self.config = yaml.load(stream)

        intro = self.config['sync']
        module = importlib.import_module(intro['module_name'])
        reader_class = getattr(module, intro['data_reader'])
        adapter_class = getattr(module, intro['persistence_adapter'])

        self.reader = reader_class(**reader_args)
        self.writer = DataWriter(adapter_class(**adapter_args))

    def run(self):
        entities = self.config[ENTITIES_CONFIG]

        total = 0

        with DataSyncClient(self.reader, self.writer) as sync:
            for entity, metadata in entities.items():
                total += sync(entity, metadata)
                metadata[LAST_SYNC] = util.to_lucene(self.start_time)

        with open(self.config_filename, 'w') as stream:
            yaml.dump(self.config, stream, **self.config[META_CONFIG])

        return total
