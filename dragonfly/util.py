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
