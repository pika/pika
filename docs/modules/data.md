# Field Tables

Header values and method arguments such as `queue_declare(arguments=...)` are AMQP field tables. pika chooses each value's field type from its Python type: `str` is sent as `S`, `bytes` as `x`, `bool` as `t`, `float` as `d`, `int` as `I` or `l` as described below, `decimal.Decimal` as `D`, `datetime` as `T`, `dict` as `F`, `list` as `A` and `None` as `V`. Import the 64-bit marker type as `pika.data.LongLong`.

::: pika.data.LongLong
