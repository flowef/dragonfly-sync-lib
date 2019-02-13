import logging
import sys
from datetime import datetime

import yaml

from dragonfly import dragonfly, util
from flow import bullhorn, mysql

LOG_ENTRY_FORMAT = '%(asctime)-15s %(message)s'
logging.basicConfig(
    filename='log.txt', level=logging.DEBUG, format=LOG_ENTRY_FORMAT)

TODATE = util.to_lucene(datetime.now())

DATABASE_CONFIG = 'database'
ENTITIES_CONFIG = 'entities'
META_CONFIG = 'meta'
LAST_SYNC = 'last_sync'

if __name__ == "__main__":
    config_filename = sys.argv[1]

    with open(config_filename) as stream:
        config = yaml.load(stream)

    source = bullhorn.RESTClient(TODATE)
    destination = dragonfly.DataWriter(mysql.Adapter(config[DATABASE_CONFIG]))

    entities = config[ENTITIES_CONFIG]

    total = 0
    with dragonfly.DataSyncClient(source, destination) as sync:
        for entity, metadata in entities.items():
            total += sync(entity, metadata)
            metadata[LAST_SYNC] = TODATE

    with open(config_filename, 'w') as stream:
        yaml.dump(config, stream, **config[META_CONFIG])

    summary = f"Synced {total} records!"
    logging.info(summary)
    print(summary)
