from enum import Enum


class ExchangeType(str, Enum):
    direct = 'direct'
    fanout = 'fanout'
    headers = 'headers'
    topic = 'topic'
