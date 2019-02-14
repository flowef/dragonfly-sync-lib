import logging

EXAMPLE_FILENAME = 'example.yaml'
DEFAULT_FILENAME = 'sync.yaml'


def gen_sample_sync_config():
    logging.info("Generating new config from example.yaml")
    try:
        with open(EXAMPLE_FILENAME) as example:
            with open(DEFAULT_FILENAME, 'w') as copy:
                copy.write(example.read())
    except FileNotFoundError:
        logging.error(
            "Example file was deleted! Cannot copy sample configuration.")
        raise

    logging.info(f"Done! Written to '{DEFAULT_FILENAME}'")
