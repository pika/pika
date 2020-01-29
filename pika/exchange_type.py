from enum import Enum


class ExchangeType(Enum) :
    direct = "Exchange type used for AMQP direct exchanges."
    fanout = "Exchange type used for AMQP fanout exchanges."
    headers = "Exchange type used for AMQP headers exchanges."
    topic = "Exchange type used for AMQP topic exchanges."
