import abc
import logging


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
