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
import logging
import sys

import yaml

from dragonfly import dragonfly, examples

if __name__ == "__main__":
    config_filename = sys.argv[1]

    try:
        with open('logging.yaml') as stream:
            logging_config = yaml.load(stream)
        if logging_config['enabled']:
            logging.basicConfig(
                filename=logging_config['file'],
                level=logging_config['level'],
                format=logging_config['format'])
    except FileNotFoundError:
        print("Logging configuration not found. No activities will be logged.")

    sync = dragonfly.Sync(config_filename, examples.RESTClient(),
                          examples.FileAdapter())
    total = sync()
    summary = f"Synced {total} records!"
    logging.info(summary)
    print(summary)
