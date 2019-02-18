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
        """ Closes a stream of data."""
        pass


class DataSourceAdapter(DataIO):
    @abc.abstractmethod
    def fetch(self, entity: str, metadata: dict):
        """ Executes routines to read entity-related data from a source
        using metadata to guide the search. """
        pass


class PersistenceAdapter(DataIO):
    @abc.abstractmethod
    def upsert(self, collection: str, record: dict, metadata: dict):
        """ Executes routines to write entity-related data to a
        persistence medium. This method inserts data if it doesn't
        already exist, and updates data if it does. """
        pass

    @abc.abstractmethod
    def remove(self, collection: str, record: dict, metadata: dict = None):
        """ Executes routines to delete entity-related data from the
        persistence medium. """
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
        """ Delegates stream closure to underlying adapter. """
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
        """ Delegates stream closure to underlying adapter. """
        self.client.close(*args, **kwargs)


class DefaultDataReader(DataReader):
    """ A simple data reader that delegates reading to its
    underlying adapter. """

    def read(self, entity_name: str, metadata: dict):
        """ Calls data fetching routine from the underlying adapter. """
        return self.data_source.fetch(entity_name, metadata)

    def __enter__(self, *args, **kwargs):
        return self

    def __exit__(self, *args):
        return self.close(*args)


class DefaultDataWriter(DataWriter):
    """ A simple data writer that upserts or deletes data. """

    def update(self, entity_name: str, metadata: dict, datasource: dict):
        """ Calls the underlying persistence adapter to upsert or delete
        data. This method uses a configurable deletion flag in `metadata`
        to decide which path to take. """
        logging.debug(f"update {entity_name}: process started...")
        for row in datasource:
            if not metadata['soft_delete'] and row.get(
                    metadata.get('delete_flag')):
                self.client.remove(metadata['table'], row)
            else:
                self.client.upsert(metadata['table'], row, metadata)
        logging.debug(f"update {entity_name}: process finished!")

    def __enter__(self, *args, **kwargs):
        return self

    def __exit__(self, *args):
        return self.close(*args)


class Sync:
    def __init__(self, config_filename: str):
        self.config_filename = config_filename

    def __update_config(self):
        with open(self.config_filename, 'w') as stream:
            yaml.dump(self.config, stream, **self.config['meta'])

    def __read_config(self):
        with open(self.config_filename) as stream:
            self.config = yaml.load(stream)

    def run(self, reader, writer):
        start_time = datetime.now()
        records_synced = 0

        with reader as source, writer as destination:
            for entity, metadata in self.config['entities'].items():
                logging.debug(f"Started syncing of {entity}")
                for batch in source.read(entity, metadata):
                    destination.update(entity, metadata, batch)
                    records_synced += len(batch)
                metadata['last_sync'] = util.to_lucene(start_time)

        return records_synced

    def __enter__(self):
        self.__read_config()
        return self.run

    def __exit__(self, *args):
        self.__update_config()
