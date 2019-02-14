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
