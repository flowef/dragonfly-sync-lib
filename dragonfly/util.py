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
from datetime import datetime


def to_query(params: dict) -> str:
    """ Returns the given dictionary as a string with the format
    `key1=value1&...&keyN=valueN`.
    For use with `HTTP GET` query strings."""
    return str.join('&', [f"{k}={v}" for k, v in params.items()])


def to_lucene(date: datetime) -> str:
    """ Returns the given date as a string with the format %Y%m%dT%H%M%S
    for use with lucene query syntax."""
    return date.strftime("%Y%m%dT%H%M%S")
