"""
AMQP Specification
==================
This module implements the constants and classes that comprise AMQP protocol
level constructs. It should rarely be directly referenced outside of Pika's
own internal use.

.. note:: Auto-generated code by codegen.py, do not edit directly. Pull
requests to this file without accompanying ``utils/codegen.py`` changes will be
rejected.

"""

from __future__ import annotations

import struct
from typing import Any

from pika import amqp_object
from pika import data
from pika._utils import override

# Pre-compiled struct formats, one per fixed-size AMQP domain.
_PACK_OCTET = struct.Struct('B')
_PACK_SHORT = struct.Struct('>H')
_PACK_LONG = struct.Struct('>I')
_PACK_LONGLONG = struct.Struct('>Q')

# Single-byte `bytes` objects indexed by value, for the bit-field buffers
# below, which only ever hold `1 << 0` through `1 << 7`.
_OCTET_BYTES = tuple(bytes((i,)) for i in range(256))

PROTOCOL_VERSION = (0, 9, 1)
PORT = 5672

ACCESS_REFUSED = 403
CHANNEL_ERROR = 504
COMMAND_INVALID = 503
CONNECTION_FORCED = 320
CONTENT_TOO_LARGE = 311
FRAME_BODY = 3
FRAME_END = 206
FRAME_END_SIZE = 1
FRAME_ERROR = 501
FRAME_HEADER = 2
FRAME_HEADER_SIZE = 7
FRAME_HEARTBEAT = 8
FRAME_MAX_SIZE = 131072
FRAME_METHOD = 1
FRAME_MIN_SIZE = 8192
INTERNAL_ERROR = 541
INVALID_PATH = 402
NOT_ALLOWED = 530
NOT_FOUND = 404
NOT_IMPLEMENTED = 540
NO_CONSUMERS = 313
NO_ROUTE = 312
PERSISTENT_DELIVERY_MODE = 2
PRECONDITION_FAILED = 406
REPLY_SUCCESS = 200
RESOURCE_ERROR = 506
RESOURCE_LOCKED = 405
SYNTAX_ERROR = 502
TRANSIENT_DELIVERY_MODE = 1
UNEXPECTED_FRAME = 505


class Basic(amqp_object.Class):

    INDEX = 0x003C  # 60
    NAME = 'Basic'

    class Qos(amqp_object.Method):

        INDEX = 0x003C000A  # 60, 10; 3932170
        NAME = 'Basic.Qos'
        synchronous: bool = True

        def __init__(self,
                     prefetch_size: int = 0,
                     prefetch_count: int = 0,
                     global_qos: bool = False):
            self.prefetch_size = prefetch_size
            self.prefetch_count = prefetch_count
            self.global_qos = global_qos

        @override
        def decode(self, encoded: bytes, offset: int = 0) -> Basic.Qos:
            self.prefetch_size = _PACK_LONG.unpack_from(encoded, offset)[0]
            offset += 4
            self.prefetch_count = _PACK_SHORT.unpack_from(encoded, offset)[0]
            offset += 2
            bit_buffer = _PACK_OCTET.unpack_from(encoded, offset)[0]
            offset += 1
            self.global_qos = (bit_buffer & (1 << 0)) != 0
            return self

        @override
        def encode(self) -> list[bytes]:
            pieces: list[bytes] = []
            pieces.append(_PACK_LONG.pack(self.prefetch_size))
            pieces.append(_PACK_SHORT.pack(self.prefetch_count))
            bit_buffer = 0
            if self.global_qos:
                bit_buffer |= 1 << 0
            pieces.append(_OCTET_BYTES[bit_buffer])
            return pieces

    class QosOk(amqp_object.Method):

        INDEX = 0x003C000B  # 60, 11; 3932171
        NAME = 'Basic.QosOk'
        synchronous: bool = False

        def __init__(self):
            pass

        @override
        def decode(self, encoded: bytes, offset: int = 0) -> Basic.QosOk:
            return self

        @override
        def encode(self) -> list[bytes]:
            pieces: list[bytes] = []
            return pieces

    class Consume(amqp_object.Method):

        INDEX = 0x003C0014  # 60, 20; 3932180
        NAME = 'Basic.Consume'
        synchronous: bool = True

        def __init__(self,
                     ticket: int = 0,
                     queue: str | bytes = '',
                     consumer_tag: str | bytes = '',
                     no_local: bool = False,
                     no_ack: bool = False,
                     exclusive: bool = False,
                     nowait: bool = False,
                     arguments: dict[Any, Any] | None = None):
            self.ticket = ticket
            self.queue = queue
            self.consumer_tag = consumer_tag
            self.no_local = no_local
            self.no_ack = no_ack
            self.exclusive = exclusive
            self.nowait = nowait
            self.arguments = arguments

        @override
        def decode(self, encoded: bytes, offset: int = 0) -> Basic.Consume:
            self.ticket = _PACK_SHORT.unpack_from(encoded, offset)[0]
            offset += 2
            self.queue, offset = data.decode_short_string(encoded, offset)
            self.consumer_tag, offset = data.decode_short_string(
                encoded, offset)
            bit_buffer = _PACK_OCTET.unpack_from(encoded, offset)[0]
            offset += 1
            self.no_local = (bit_buffer & (1 << 0)) != 0
            self.no_ack = (bit_buffer & (1 << 1)) != 0
            self.exclusive = (bit_buffer & (1 << 2)) != 0
            self.nowait = (bit_buffer & (1 << 3)) != 0
            (self.arguments, offset) = data.decode_table(encoded, offset)
            return self

        @override
        def encode(self) -> list[bytes]:
            pieces: list[bytes] = []
            pieces.append(_PACK_SHORT.pack(self.ticket))
            assert isinstance(self.queue, (str, bytes)),\
                   'A non-string value was supplied for self.queue'
            data.encode_short_string(pieces, self.queue)
            assert isinstance(self.consumer_tag, (str, bytes)),\
                   'A non-string value was supplied for self.consumer_tag'
            data.encode_short_string(pieces, self.consumer_tag)
            bit_buffer = 0
            if self.no_local:
                bit_buffer |= 1 << 0
            if self.no_ack:
                bit_buffer |= 1 << 1
            if self.exclusive:
                bit_buffer |= 1 << 2
            if self.nowait:
                bit_buffer |= 1 << 3
            pieces.append(_OCTET_BYTES[bit_buffer])
            data.encode_table(pieces, self.arguments)
            return pieces

    class ConsumeOk(amqp_object.Method):

        INDEX = 0x003C0015  # 60, 21; 3932181
        NAME = 'Basic.ConsumeOk'
        synchronous: bool = False

        def __init__(self, consumer_tag: str | bytes | None = None):
            self.consumer_tag = consumer_tag

        @override
        def decode(self, encoded: bytes, offset: int = 0) -> Basic.ConsumeOk:
            self.consumer_tag, offset = data.decode_short_string(
                encoded, offset)
            return self

        @override
        def encode(self) -> list[bytes]:
            pieces: list[bytes] = []
            assert isinstance(self.consumer_tag, (str, bytes)),\
                   'A non-string value was supplied for self.consumer_tag'
            data.encode_short_string(pieces, self.consumer_tag)
            return pieces

    class Cancel(amqp_object.Method):

        INDEX = 0x003C001E  # 60, 30; 3932190
        NAME = 'Basic.Cancel'
        synchronous: bool = True

        def __init__(self,
                     consumer_tag: str | bytes | None = None,
                     nowait: bool = False):
            self.consumer_tag = consumer_tag
            self.nowait = nowait

        @override
        def decode(self, encoded: bytes, offset: int = 0) -> Basic.Cancel:
            self.consumer_tag, offset = data.decode_short_string(
                encoded, offset)
            bit_buffer = _PACK_OCTET.unpack_from(encoded, offset)[0]
            offset += 1
            self.nowait = (bit_buffer & (1 << 0)) != 0
            return self

        @override
        def encode(self) -> list[bytes]:
            pieces: list[bytes] = []
            assert isinstance(self.consumer_tag, (str, bytes)),\
                   'A non-string value was supplied for self.consumer_tag'
            data.encode_short_string(pieces, self.consumer_tag)
            bit_buffer = 0
            if self.nowait:
                bit_buffer |= 1 << 0
            pieces.append(_OCTET_BYTES[bit_buffer])
            return pieces

    class CancelOk(amqp_object.Method):

        INDEX = 0x003C001F  # 60, 31; 3932191
        NAME = 'Basic.CancelOk'
        synchronous: bool = False

        def __init__(self, consumer_tag: str | bytes | None = None):
            self.consumer_tag = consumer_tag

        @override
        def decode(self, encoded: bytes, offset: int = 0) -> Basic.CancelOk:
            self.consumer_tag, offset = data.decode_short_string(
                encoded, offset)
            return self

        @override
        def encode(self) -> list[bytes]:
            pieces: list[bytes] = []
            assert isinstance(self.consumer_tag, (str, bytes)),\
                   'A non-string value was supplied for self.consumer_tag'
            data.encode_short_string(pieces, self.consumer_tag)
            return pieces

    class Publish(amqp_object.Method):

        INDEX = 0x003C0028  # 60, 40; 3932200
        NAME = 'Basic.Publish'
        synchronous: bool = False

        def __init__(self,
                     ticket: int = 0,
                     exchange: str | bytes = '',
                     routing_key: str | bytes = '',
                     mandatory: bool = False,
                     immediate: bool = False):
            self.ticket = ticket
            self.exchange = exchange
            self.routing_key = routing_key
            self.mandatory = mandatory
            self.immediate = immediate

        @override
        def decode(self, encoded: bytes, offset: int = 0) -> Basic.Publish:
            self.ticket = _PACK_SHORT.unpack_from(encoded, offset)[0]
            offset += 2
            self.exchange, offset = data.decode_short_string(encoded, offset)
            self.routing_key, offset = data.decode_short_string(encoded, offset)
            bit_buffer = _PACK_OCTET.unpack_from(encoded, offset)[0]
            offset += 1
            self.mandatory = (bit_buffer & (1 << 0)) != 0
            self.immediate = (bit_buffer & (1 << 1)) != 0
            return self

        @override
        def encode(self) -> list[bytes]:
            pieces: list[bytes] = []
            pieces.append(_PACK_SHORT.pack(self.ticket))
            assert isinstance(self.exchange, (str, bytes)),\
                   'A non-string value was supplied for self.exchange'
            data.encode_short_string(pieces, self.exchange)
            assert isinstance(self.routing_key, (str, bytes)),\
                   'A non-string value was supplied for self.routing_key'
            data.encode_short_string(pieces, self.routing_key)
            bit_buffer = 0
            if self.mandatory:
                bit_buffer |= 1 << 0
            if self.immediate:
                bit_buffer |= 1 << 1
            pieces.append(_OCTET_BYTES[bit_buffer])
            return pieces

    class Return(amqp_object.Method):

        INDEX = 0x003C0032  # 60, 50; 3932210
        NAME = 'Basic.Return'
        synchronous: bool = False

        def __init__(self,
                     reply_code: int | None = None,
                     reply_text: str | bytes = '',
                     exchange: str | bytes | None = None,
                     routing_key: str | bytes | None = None):
            self.reply_code = reply_code
            self.reply_text = reply_text
            self.exchange = exchange
            self.routing_key = routing_key

        @override
        def decode(self, encoded: bytes, offset: int = 0) -> Basic.Return:
            self.reply_code = _PACK_SHORT.unpack_from(encoded, offset)[0]
            offset += 2
            self.reply_text, offset = data.decode_short_string(encoded, offset)
            self.exchange, offset = data.decode_short_string(encoded, offset)
            self.routing_key, offset = data.decode_short_string(encoded, offset)
            return self

        @override
        def encode(self) -> list[bytes]:
            pieces: list[bytes] = []
            pieces.append(_PACK_SHORT.pack(self.reply_code))
            assert isinstance(self.reply_text, (str, bytes)),\
                   'A non-string value was supplied for self.reply_text'
            data.encode_short_string(pieces, self.reply_text)
            assert isinstance(self.exchange, (str, bytes)),\
                   'A non-string value was supplied for self.exchange'
            data.encode_short_string(pieces, self.exchange)
            assert isinstance(self.routing_key, (str, bytes)),\
                   'A non-string value was supplied for self.routing_key'
            data.encode_short_string(pieces, self.routing_key)
            return pieces

    class Deliver(amqp_object.Method):

        INDEX = 0x003C003C  # 60, 60; 3932220
        NAME = 'Basic.Deliver'
        synchronous: bool = False

        def __init__(self,
                     consumer_tag: str | bytes | None = None,
                     delivery_tag: int | None = None,
                     redelivered: bool = False,
                     exchange: str | bytes | None = None,
                     routing_key: str | bytes | None = None):
            self.consumer_tag = consumer_tag
            self.delivery_tag = delivery_tag
            self.redelivered = redelivered
            self.exchange = exchange
            self.routing_key = routing_key

        @override
        def decode(self, encoded: bytes, offset: int = 0) -> Basic.Deliver:
            self.consumer_tag, offset = data.decode_short_string(
                encoded, offset)
            self.delivery_tag = _PACK_LONGLONG.unpack_from(encoded, offset)[0]
            offset += 8
            bit_buffer = _PACK_OCTET.unpack_from(encoded, offset)[0]
            offset += 1
            self.redelivered = (bit_buffer & (1 << 0)) != 0
            self.exchange, offset = data.decode_short_string(encoded, offset)
            self.routing_key, offset = data.decode_short_string(encoded, offset)
            return self

        @override
        def encode(self) -> list[bytes]:
            pieces: list[bytes] = []
            assert isinstance(self.consumer_tag, (str, bytes)),\
                   'A non-string value was supplied for self.consumer_tag'
            data.encode_short_string(pieces, self.consumer_tag)
            pieces.append(_PACK_LONGLONG.pack(self.delivery_tag))
            bit_buffer = 0
            if self.redelivered:
                bit_buffer |= 1 << 0
            pieces.append(_OCTET_BYTES[bit_buffer])
            assert isinstance(self.exchange, (str, bytes)),\
                   'A non-string value was supplied for self.exchange'
            data.encode_short_string(pieces, self.exchange)
            assert isinstance(self.routing_key, (str, bytes)),\
                   'A non-string value was supplied for self.routing_key'
            data.encode_short_string(pieces, self.routing_key)
            return pieces

    class Get(amqp_object.Method):

        INDEX = 0x003C0046  # 60, 70; 3932230
        NAME = 'Basic.Get'
        synchronous: bool = True

        def __init__(self,
                     ticket: int = 0,
                     queue: str | bytes = '',
                     no_ack: bool = False):
            self.ticket = ticket
            self.queue = queue
            self.no_ack = no_ack

        @override
        def decode(self, encoded: bytes, offset: int = 0) -> Basic.Get:
            self.ticket = _PACK_SHORT.unpack_from(encoded, offset)[0]
            offset += 2
            self.queue, offset = data.decode_short_string(encoded, offset)
            bit_buffer = _PACK_OCTET.unpack_from(encoded, offset)[0]
            offset += 1
            self.no_ack = (bit_buffer & (1 << 0)) != 0
            return self

        @override
        def encode(self) -> list[bytes]:
            pieces: list[bytes] = []
            pieces.append(_PACK_SHORT.pack(self.ticket))
            assert isinstance(self.queue, (str, bytes)),\
                   'A non-string value was supplied for self.queue'
            data.encode_short_string(pieces, self.queue)
            bit_buffer = 0
            if self.no_ack:
                bit_buffer |= 1 << 0
            pieces.append(_OCTET_BYTES[bit_buffer])
            return pieces

    class GetOk(amqp_object.Method):

        INDEX = 0x003C0047  # 60, 71; 3932231
        NAME = 'Basic.GetOk'
        synchronous: bool = False

        def __init__(self,
                     delivery_tag: int | None = None,
                     redelivered: bool = False,
                     exchange: str | bytes | None = None,
                     routing_key: str | bytes | None = None,
                     message_count: int | None = None):
            self.delivery_tag = delivery_tag
            self.redelivered = redelivered
            self.exchange = exchange
            self.routing_key = routing_key
            self.message_count = message_count

        @override
        def decode(self, encoded: bytes, offset: int = 0) -> Basic.GetOk:
            self.delivery_tag = _PACK_LONGLONG.unpack_from(encoded, offset)[0]
            offset += 8
            bit_buffer = _PACK_OCTET.unpack_from(encoded, offset)[0]
            offset += 1
            self.redelivered = (bit_buffer & (1 << 0)) != 0
            self.exchange, offset = data.decode_short_string(encoded, offset)
            self.routing_key, offset = data.decode_short_string(encoded, offset)
            self.message_count = _PACK_LONG.unpack_from(encoded, offset)[0]
            offset += 4
            return self

        @override
        def encode(self) -> list[bytes]:
            pieces: list[bytes] = []
            pieces.append(_PACK_LONGLONG.pack(self.delivery_tag))
            bit_buffer = 0
            if self.redelivered:
                bit_buffer |= 1 << 0
            pieces.append(_OCTET_BYTES[bit_buffer])
            assert isinstance(self.exchange, (str, bytes)),\
                   'A non-string value was supplied for self.exchange'
            data.encode_short_string(pieces, self.exchange)
            assert isinstance(self.routing_key, (str, bytes)),\
                   'A non-string value was supplied for self.routing_key'
            data.encode_short_string(pieces, self.routing_key)
            pieces.append(_PACK_LONG.pack(self.message_count))
            return pieces

    class GetEmpty(amqp_object.Method):

        INDEX = 0x003C0048  # 60, 72; 3932232
        NAME = 'Basic.GetEmpty'
        synchronous: bool = False

        def __init__(self, cluster_id: str | bytes = ''):
            self.cluster_id = cluster_id

        @override
        def decode(self, encoded: bytes, offset: int = 0) -> Basic.GetEmpty:
            self.cluster_id, offset = data.decode_short_string(encoded, offset)
            return self

        @override
        def encode(self) -> list[bytes]:
            pieces: list[bytes] = []
            assert isinstance(self.cluster_id, (str, bytes)),\
                   'A non-string value was supplied for self.cluster_id'
            data.encode_short_string(pieces, self.cluster_id)
            return pieces

    class Ack(amqp_object.Method):

        INDEX = 0x003C0050  # 60, 80; 3932240
        NAME = 'Basic.Ack'
        synchronous: bool = False

        def __init__(self, delivery_tag: int = 0, multiple: bool = False):
            self.delivery_tag = delivery_tag
            self.multiple = multiple

        @override
        def decode(self, encoded: bytes, offset: int = 0) -> Basic.Ack:
            self.delivery_tag = _PACK_LONGLONG.unpack_from(encoded, offset)[0]
            offset += 8
            bit_buffer = _PACK_OCTET.unpack_from(encoded, offset)[0]
            offset += 1
            self.multiple = (bit_buffer & (1 << 0)) != 0
            return self

        @override
        def encode(self) -> list[bytes]:
            pieces: list[bytes] = []
            pieces.append(_PACK_LONGLONG.pack(self.delivery_tag))
            bit_buffer = 0
            if self.multiple:
                bit_buffer |= 1 << 0
            pieces.append(_OCTET_BYTES[bit_buffer])
            return pieces

    class Reject(amqp_object.Method):

        INDEX = 0x003C005A  # 60, 90; 3932250
        NAME = 'Basic.Reject'
        synchronous: bool = False

        def __init__(self,
                     delivery_tag: int | None = None,
                     requeue: bool = True):
            self.delivery_tag = delivery_tag
            self.requeue = requeue

        @override
        def decode(self, encoded: bytes, offset: int = 0) -> Basic.Reject:
            self.delivery_tag = _PACK_LONGLONG.unpack_from(encoded, offset)[0]
            offset += 8
            bit_buffer = _PACK_OCTET.unpack_from(encoded, offset)[0]
            offset += 1
            self.requeue = (bit_buffer & (1 << 0)) != 0
            return self

        @override
        def encode(self) -> list[bytes]:
            pieces: list[bytes] = []
            pieces.append(_PACK_LONGLONG.pack(self.delivery_tag))
            bit_buffer = 0
            if self.requeue:
                bit_buffer |= 1 << 0
            pieces.append(_OCTET_BYTES[bit_buffer])
            return pieces

    class RecoverAsync(amqp_object.Method):

        INDEX = 0x003C0064  # 60, 100; 3932260
        NAME = 'Basic.RecoverAsync'
        synchronous: bool = False

        def __init__(self, requeue: bool = False):
            self.requeue = requeue

        @override
        def decode(self, encoded: bytes, offset: int = 0) -> Basic.RecoverAsync:
            bit_buffer = _PACK_OCTET.unpack_from(encoded, offset)[0]
            offset += 1
            self.requeue = (bit_buffer & (1 << 0)) != 0
            return self

        @override
        def encode(self) -> list[bytes]:
            pieces: list[bytes] = []
            bit_buffer = 0
            if self.requeue:
                bit_buffer |= 1 << 0
            pieces.append(_OCTET_BYTES[bit_buffer])
            return pieces

    class Recover(amqp_object.Method):

        INDEX = 0x003C006E  # 60, 110; 3932270
        NAME = 'Basic.Recover'
        synchronous: bool = True

        def __init__(self, requeue: bool = False):
            self.requeue = requeue

        @override
        def decode(self, encoded: bytes, offset: int = 0) -> Basic.Recover:
            bit_buffer = _PACK_OCTET.unpack_from(encoded, offset)[0]
            offset += 1
            self.requeue = (bit_buffer & (1 << 0)) != 0
            return self

        @override
        def encode(self) -> list[bytes]:
            pieces: list[bytes] = []
            bit_buffer = 0
            if self.requeue:
                bit_buffer |= 1 << 0
            pieces.append(_OCTET_BYTES[bit_buffer])
            return pieces

    class RecoverOk(amqp_object.Method):

        INDEX = 0x003C006F  # 60, 111; 3932271
        NAME = 'Basic.RecoverOk'
        synchronous: bool = False

        def __init__(self):
            pass

        @override
        def decode(self, encoded: bytes, offset: int = 0) -> Basic.RecoverOk:
            return self

        @override
        def encode(self) -> list[bytes]:
            pieces: list[bytes] = []
            return pieces

    class Nack(amqp_object.Method):

        INDEX = 0x003C0078  # 60, 120; 3932280
        NAME = 'Basic.Nack'
        synchronous: bool = False

        def __init__(self,
                     delivery_tag: int = 0,
                     multiple: bool = False,
                     requeue: bool = True):
            self.delivery_tag = delivery_tag
            self.multiple = multiple
            self.requeue = requeue

        @override
        def decode(self, encoded: bytes, offset: int = 0) -> Basic.Nack:
            self.delivery_tag = _PACK_LONGLONG.unpack_from(encoded, offset)[0]
            offset += 8
            bit_buffer = _PACK_OCTET.unpack_from(encoded, offset)[0]
            offset += 1
            self.multiple = (bit_buffer & (1 << 0)) != 0
            self.requeue = (bit_buffer & (1 << 1)) != 0
            return self

        @override
        def encode(self) -> list[bytes]:
            pieces: list[bytes] = []
            pieces.append(_PACK_LONGLONG.pack(self.delivery_tag))
            bit_buffer = 0
            if self.multiple:
                bit_buffer |= 1 << 0
            if self.requeue:
                bit_buffer |= 1 << 1
            pieces.append(_OCTET_BYTES[bit_buffer])
            return pieces


class Connection(amqp_object.Class):

    INDEX = 0x000A  # 10
    NAME = 'Connection'

    class Start(amqp_object.Method):

        INDEX = 0x000A000A  # 10, 10; 655370
        NAME = 'Connection.Start'
        synchronous: bool = True

        def __init__(self,
                     version_major: int = 0,
                     version_minor: int = 9,
                     server_properties: dict[Any, Any] | None = None,
                     mechanisms: str | bytes = 'PLAIN',
                     locales: str | bytes = 'en_US'):
            self.version_major = version_major
            self.version_minor = version_minor
            self.server_properties = server_properties
            self.mechanisms = mechanisms
            self.locales = locales

        @override
        def decode(self, encoded: bytes, offset: int = 0) -> Connection.Start:
            self.version_major = _PACK_OCTET.unpack_from(encoded, offset)[0]
            offset += 1
            self.version_minor = _PACK_OCTET.unpack_from(encoded, offset)[0]
            offset += 1
            (self.server_properties,
             offset) = data.decode_table(encoded, offset)
            length = _PACK_LONG.unpack_from(encoded, offset)[0]
            offset += 4
            self.mechanisms = encoded[offset:offset + length]
            offset += length
            length = _PACK_LONG.unpack_from(encoded, offset)[0]
            offset += 4
            self.locales = encoded[offset:offset + length]
            offset += length
            return self

        @override
        def encode(self) -> list[bytes]:
            pieces: list[bytes] = []
            pieces.append(_PACK_OCTET.pack(self.version_major))
            pieces.append(_PACK_OCTET.pack(self.version_minor))
            data.encode_table(pieces, self.server_properties)
            assert isinstance(self.mechanisms, (str, bytes)),\
                   'A non-string value was supplied for self.mechanisms'
            value = self.mechanisms.encode('utf-8') if isinstance(
                self.mechanisms, str) else self.mechanisms
            pieces.append(_PACK_LONG.pack(len(value)))
            pieces.append(value)
            assert isinstance(self.locales, (str, bytes)),\
                   'A non-string value was supplied for self.locales'
            value = self.locales.encode('utf-8') if isinstance(
                self.locales, str) else self.locales
            pieces.append(_PACK_LONG.pack(len(value)))
            pieces.append(value)
            return pieces

    class StartOk(amqp_object.Method):

        INDEX = 0x000A000B  # 10, 11; 655371
        NAME = 'Connection.StartOk'
        synchronous: bool = False

        def __init__(self,
                     client_properties: dict[Any, Any] | None = None,
                     mechanism: str | bytes = 'PLAIN',
                     response: str | bytes | None = None,
                     locale: str | bytes = 'en_US'):
            self.client_properties = client_properties
            self.mechanism = mechanism
            self.response = response
            self.locale = locale

        @override
        def decode(self, encoded: bytes, offset: int = 0) -> Connection.StartOk:
            (self.client_properties,
             offset) = data.decode_table(encoded, offset)
            self.mechanism, offset = data.decode_short_string(encoded, offset)
            length = _PACK_LONG.unpack_from(encoded, offset)[0]
            offset += 4
            self.response = encoded[offset:offset + length]
            offset += length
            self.locale, offset = data.decode_short_string(encoded, offset)
            return self

        @override
        def encode(self) -> list[bytes]:
            pieces: list[bytes] = []
            data.encode_table(pieces, self.client_properties)
            assert isinstance(self.mechanism, (str, bytes)),\
                   'A non-string value was supplied for self.mechanism'
            data.encode_short_string(pieces, self.mechanism)
            assert isinstance(self.response, (str, bytes)),\
                   'A non-string value was supplied for self.response'
            value = self.response.encode('utf-8') if isinstance(
                self.response, str) else self.response
            pieces.append(_PACK_LONG.pack(len(value)))
            pieces.append(value)
            assert isinstance(self.locale, (str, bytes)),\
                   'A non-string value was supplied for self.locale'
            data.encode_short_string(pieces, self.locale)
            return pieces

    class Secure(amqp_object.Method):

        INDEX = 0x000A0014  # 10, 20; 655380
        NAME = 'Connection.Secure'
        synchronous: bool = True

        def __init__(self, challenge: str | bytes | None = None):
            self.challenge = challenge

        @override
        def decode(self, encoded: bytes, offset: int = 0) -> Connection.Secure:
            length = _PACK_LONG.unpack_from(encoded, offset)[0]
            offset += 4
            self.challenge = encoded[offset:offset + length]
            offset += length
            return self

        @override
        def encode(self) -> list[bytes]:
            pieces: list[bytes] = []
            assert isinstance(self.challenge, (str, bytes)),\
                   'A non-string value was supplied for self.challenge'
            value = self.challenge.encode('utf-8') if isinstance(
                self.challenge, str) else self.challenge
            pieces.append(_PACK_LONG.pack(len(value)))
            pieces.append(value)
            return pieces

    class SecureOk(amqp_object.Method):

        INDEX = 0x000A0015  # 10, 21; 655381
        NAME = 'Connection.SecureOk'
        synchronous: bool = False

        def __init__(self, response: str | bytes | None = None):
            self.response = response

        @override
        def decode(self,
                   encoded: bytes,
                   offset: int = 0) -> Connection.SecureOk:
            length = _PACK_LONG.unpack_from(encoded, offset)[0]
            offset += 4
            self.response = encoded[offset:offset + length]
            offset += length
            return self

        @override
        def encode(self) -> list[bytes]:
            pieces: list[bytes] = []
            assert isinstance(self.response, (str, bytes)),\
                   'A non-string value was supplied for self.response'
            value = self.response.encode('utf-8') if isinstance(
                self.response, str) else self.response
            pieces.append(_PACK_LONG.pack(len(value)))
            pieces.append(value)
            return pieces

    class Tune(amqp_object.Method):

        INDEX = 0x000A001E  # 10, 30; 655390
        NAME = 'Connection.Tune'
        synchronous: bool = True

        def __init__(self,
                     channel_max: int = 0,
                     frame_max: int = 0,
                     heartbeat: int = 0):
            self.channel_max = channel_max
            self.frame_max = frame_max
            self.heartbeat = heartbeat

        @override
        def decode(self, encoded: bytes, offset: int = 0) -> Connection.Tune:
            self.channel_max = _PACK_SHORT.unpack_from(encoded, offset)[0]
            offset += 2
            self.frame_max = _PACK_LONG.unpack_from(encoded, offset)[0]
            offset += 4
            self.heartbeat = _PACK_SHORT.unpack_from(encoded, offset)[0]
            offset += 2
            return self

        @override
        def encode(self) -> list[bytes]:
            pieces: list[bytes] = []
            pieces.append(_PACK_SHORT.pack(self.channel_max))
            pieces.append(_PACK_LONG.pack(self.frame_max))
            pieces.append(_PACK_SHORT.pack(self.heartbeat))
            return pieces

    class TuneOk(amqp_object.Method):

        INDEX = 0x000A001F  # 10, 31; 655391
        NAME = 'Connection.TuneOk'
        synchronous: bool = False

        def __init__(self,
                     channel_max: int = 0,
                     frame_max: int = 0,
                     heartbeat: int = 0):
            self.channel_max = channel_max
            self.frame_max = frame_max
            self.heartbeat = heartbeat

        @override
        def decode(self, encoded: bytes, offset: int = 0) -> Connection.TuneOk:
            self.channel_max = _PACK_SHORT.unpack_from(encoded, offset)[0]
            offset += 2
            self.frame_max = _PACK_LONG.unpack_from(encoded, offset)[0]
            offset += 4
            self.heartbeat = _PACK_SHORT.unpack_from(encoded, offset)[0]
            offset += 2
            return self

        @override
        def encode(self) -> list[bytes]:
            pieces: list[bytes] = []
            pieces.append(_PACK_SHORT.pack(self.channel_max))
            pieces.append(_PACK_LONG.pack(self.frame_max))
            pieces.append(_PACK_SHORT.pack(self.heartbeat))
            return pieces

    class Open(amqp_object.Method):

        INDEX = 0x000A0028  # 10, 40; 655400
        NAME = 'Connection.Open'
        synchronous: bool = True

        def __init__(self,
                     virtual_host: str | bytes = '/',
                     capabilities: str | bytes = '',
                     insist: bool = False):
            self.virtual_host = virtual_host
            self.capabilities = capabilities
            self.insist = insist

        @override
        def decode(self, encoded: bytes, offset: int = 0) -> Connection.Open:
            self.virtual_host, offset = data.decode_short_string(
                encoded, offset)
            self.capabilities, offset = data.decode_short_string(
                encoded, offset)
            bit_buffer = _PACK_OCTET.unpack_from(encoded, offset)[0]
            offset += 1
            self.insist = (bit_buffer & (1 << 0)) != 0
            return self

        @override
        def encode(self) -> list[bytes]:
            pieces: list[bytes] = []
            assert isinstance(self.virtual_host, (str, bytes)),\
                   'A non-string value was supplied for self.virtual_host'
            data.encode_short_string(pieces, self.virtual_host)
            assert isinstance(self.capabilities, (str, bytes)),\
                   'A non-string value was supplied for self.capabilities'
            data.encode_short_string(pieces, self.capabilities)
            bit_buffer = 0
            if self.insist:
                bit_buffer |= 1 << 0
            pieces.append(_OCTET_BYTES[bit_buffer])
            return pieces

    class OpenOk(amqp_object.Method):

        INDEX = 0x000A0029  # 10, 41; 655401
        NAME = 'Connection.OpenOk'
        synchronous: bool = False

        def __init__(self, known_hosts: str | bytes = ''):
            self.known_hosts = known_hosts

        @override
        def decode(self, encoded: bytes, offset: int = 0) -> Connection.OpenOk:
            self.known_hosts, offset = data.decode_short_string(encoded, offset)
            return self

        @override
        def encode(self) -> list[bytes]:
            pieces: list[bytes] = []
            assert isinstance(self.known_hosts, (str, bytes)),\
                   'A non-string value was supplied for self.known_hosts'
            data.encode_short_string(pieces, self.known_hosts)
            return pieces

    class Close(amqp_object.Method):

        INDEX = 0x000A0032  # 10, 50; 655410
        NAME = 'Connection.Close'
        synchronous: bool = True

        def __init__(self,
                     reply_code: int | None = None,
                     reply_text: str | bytes = '',
                     class_id: int | None = None,
                     method_id: int | None = None):
            self.reply_code = reply_code
            self.reply_text = reply_text
            self.class_id = class_id
            self.method_id = method_id

        @override
        def decode(self, encoded: bytes, offset: int = 0) -> Connection.Close:
            self.reply_code = _PACK_SHORT.unpack_from(encoded, offset)[0]
            offset += 2
            self.reply_text, offset = data.decode_short_string(encoded, offset)
            self.class_id = _PACK_SHORT.unpack_from(encoded, offset)[0]
            offset += 2
            self.method_id = _PACK_SHORT.unpack_from(encoded, offset)[0]
            offset += 2
            return self

        @override
        def encode(self) -> list[bytes]:
            pieces: list[bytes] = []
            pieces.append(_PACK_SHORT.pack(self.reply_code))
            assert isinstance(self.reply_text, (str, bytes)),\
                   'A non-string value was supplied for self.reply_text'
            data.encode_short_string(pieces, self.reply_text)
            pieces.append(_PACK_SHORT.pack(self.class_id))
            pieces.append(_PACK_SHORT.pack(self.method_id))
            return pieces

    class CloseOk(amqp_object.Method):

        INDEX = 0x000A0033  # 10, 51; 655411
        NAME = 'Connection.CloseOk'
        synchronous: bool = False

        def __init__(self):
            pass

        @override
        def decode(self, encoded: bytes, offset: int = 0) -> Connection.CloseOk:
            return self

        @override
        def encode(self) -> list[bytes]:
            pieces: list[bytes] = []
            return pieces

    class Blocked(amqp_object.Method):

        INDEX = 0x000A003C  # 10, 60; 655420
        NAME = 'Connection.Blocked'
        synchronous: bool = False

        def __init__(self, reason: str | bytes = ''):
            self.reason = reason

        @override
        def decode(self, encoded: bytes, offset: int = 0) -> Connection.Blocked:
            self.reason, offset = data.decode_short_string(encoded, offset)
            return self

        @override
        def encode(self) -> list[bytes]:
            pieces: list[bytes] = []
            assert isinstance(self.reason, (str, bytes)),\
                   'A non-string value was supplied for self.reason'
            data.encode_short_string(pieces, self.reason)
            return pieces

    class Unblocked(amqp_object.Method):

        INDEX = 0x000A003D  # 10, 61; 655421
        NAME = 'Connection.Unblocked'
        synchronous: bool = False

        def __init__(self):
            pass

        @override
        def decode(self,
                   encoded: bytes,
                   offset: int = 0) -> Connection.Unblocked:
            return self

        @override
        def encode(self) -> list[bytes]:
            pieces: list[bytes] = []
            return pieces

    class UpdateSecret(amqp_object.Method):

        INDEX = 0x000A0046  # 10, 70; 655430
        NAME = 'Connection.UpdateSecret'
        synchronous: bool = True

        def __init__(self,
                     new_secret: str | bytes | None = None,
                     reason: str | bytes | None = None):
            self.new_secret = new_secret
            self.reason = reason

        @override
        def decode(self,
                   encoded: bytes,
                   offset: int = 0) -> Connection.UpdateSecret:
            length = _PACK_LONG.unpack_from(encoded, offset)[0]
            offset += 4
            self.new_secret = encoded[offset:offset + length]
            offset += length
            self.reason, offset = data.decode_short_string(encoded, offset)
            return self

        @override
        def encode(self) -> list[bytes]:
            pieces: list[bytes] = []
            assert isinstance(self.new_secret, (str, bytes)),\
                   'A non-string value was supplied for self.new_secret'
            value = self.new_secret.encode('utf-8') if isinstance(
                self.new_secret, str) else self.new_secret
            pieces.append(_PACK_LONG.pack(len(value)))
            pieces.append(value)
            assert isinstance(self.reason, (str, bytes)),\
                   'A non-string value was supplied for self.reason'
            data.encode_short_string(pieces, self.reason)
            return pieces

    class UpdateSecretOk(amqp_object.Method):

        INDEX = 0x000A0047  # 10, 71; 655431
        NAME = 'Connection.UpdateSecretOk'
        synchronous: bool = False

        def __init__(self):
            pass

        @override
        def decode(self,
                   encoded: bytes,
                   offset: int = 0) -> Connection.UpdateSecretOk:
            return self

        @override
        def encode(self) -> list[bytes]:
            pieces: list[bytes] = []
            return pieces


class Channel(amqp_object.Class):

    INDEX = 0x0014  # 20
    NAME = 'Channel'

    class Open(amqp_object.Method):

        INDEX = 0x0014000A  # 20, 10; 1310730
        NAME = 'Channel.Open'
        synchronous: bool = True

        def __init__(self, out_of_band: str | bytes = ''):
            self.out_of_band = out_of_band

        @override
        def decode(self, encoded: bytes, offset: int = 0) -> Channel.Open:
            self.out_of_band, offset = data.decode_short_string(encoded, offset)
            return self

        @override
        def encode(self) -> list[bytes]:
            pieces: list[bytes] = []
            assert isinstance(self.out_of_band, (str, bytes)),\
                   'A non-string value was supplied for self.out_of_band'
            data.encode_short_string(pieces, self.out_of_band)
            return pieces

    class OpenOk(amqp_object.Method):

        INDEX = 0x0014000B  # 20, 11; 1310731
        NAME = 'Channel.OpenOk'
        synchronous: bool = False

        def __init__(self, channel_id: str | bytes = ''):
            self.channel_id = channel_id

        @override
        def decode(self, encoded: bytes, offset: int = 0) -> Channel.OpenOk:
            length = _PACK_LONG.unpack_from(encoded, offset)[0]
            offset += 4
            self.channel_id = encoded[offset:offset + length]
            offset += length
            return self

        @override
        def encode(self) -> list[bytes]:
            pieces: list[bytes] = []
            assert isinstance(self.channel_id, (str, bytes)),\
                   'A non-string value was supplied for self.channel_id'
            value = self.channel_id.encode('utf-8') if isinstance(
                self.channel_id, str) else self.channel_id
            pieces.append(_PACK_LONG.pack(len(value)))
            pieces.append(value)
            return pieces

    class Flow(amqp_object.Method):

        INDEX = 0x00140014  # 20, 20; 1310740
        NAME = 'Channel.Flow'
        synchronous: bool = True

        def __init__(self, active: bool | None = None):
            self.active = active

        @override
        def decode(self, encoded: bytes, offset: int = 0) -> Channel.Flow:
            bit_buffer = _PACK_OCTET.unpack_from(encoded, offset)[0]
            offset += 1
            self.active = (bit_buffer & (1 << 0)) != 0
            return self

        @override
        def encode(self) -> list[bytes]:
            pieces: list[bytes] = []
            bit_buffer = 0
            if self.active:
                bit_buffer |= 1 << 0
            pieces.append(_OCTET_BYTES[bit_buffer])
            return pieces

    class FlowOk(amqp_object.Method):

        INDEX = 0x00140015  # 20, 21; 1310741
        NAME = 'Channel.FlowOk'
        synchronous: bool = False

        def __init__(self, active: bool | None = None):
            self.active = active

        @override
        def decode(self, encoded: bytes, offset: int = 0) -> Channel.FlowOk:
            bit_buffer = _PACK_OCTET.unpack_from(encoded, offset)[0]
            offset += 1
            self.active = (bit_buffer & (1 << 0)) != 0
            return self

        @override
        def encode(self) -> list[bytes]:
            pieces: list[bytes] = []
            bit_buffer = 0
            if self.active:
                bit_buffer |= 1 << 0
            pieces.append(_OCTET_BYTES[bit_buffer])
            return pieces

    class Close(amqp_object.Method):

        INDEX = 0x00140028  # 20, 40; 1310760
        NAME = 'Channel.Close'
        synchronous: bool = True

        def __init__(self,
                     reply_code: int | None = None,
                     reply_text: str | bytes = '',
                     class_id: int | None = None,
                     method_id: int | None = None):
            self.reply_code = reply_code
            self.reply_text = reply_text
            self.class_id = class_id
            self.method_id = method_id

        @override
        def decode(self, encoded: bytes, offset: int = 0) -> Channel.Close:
            self.reply_code = _PACK_SHORT.unpack_from(encoded, offset)[0]
            offset += 2
            self.reply_text, offset = data.decode_short_string(encoded, offset)
            self.class_id = _PACK_SHORT.unpack_from(encoded, offset)[0]
            offset += 2
            self.method_id = _PACK_SHORT.unpack_from(encoded, offset)[0]
            offset += 2
            return self

        @override
        def encode(self) -> list[bytes]:
            pieces: list[bytes] = []
            pieces.append(_PACK_SHORT.pack(self.reply_code))
            assert isinstance(self.reply_text, (str, bytes)),\
                   'A non-string value was supplied for self.reply_text'
            data.encode_short_string(pieces, self.reply_text)
            pieces.append(_PACK_SHORT.pack(self.class_id))
            pieces.append(_PACK_SHORT.pack(self.method_id))
            return pieces

    class CloseOk(amqp_object.Method):

        INDEX = 0x00140029  # 20, 41; 1310761
        NAME = 'Channel.CloseOk'
        synchronous: bool = False

        def __init__(self):
            pass

        @override
        def decode(self, encoded: bytes, offset: int = 0) -> Channel.CloseOk:
            return self

        @override
        def encode(self) -> list[bytes]:
            pieces: list[bytes] = []
            return pieces


class Access(amqp_object.Class):

    INDEX = 0x001E  # 30
    NAME = 'Access'

    class Request(amqp_object.Method):

        INDEX = 0x001E000A  # 30, 10; 1966090
        NAME = 'Access.Request'
        synchronous: bool = True

        def __init__(self,
                     realm: str | bytes = '/data',
                     exclusive: bool = False,
                     passive: bool = True,
                     active: bool = True,
                     write: bool = True,
                     read: bool = True):
            self.realm = realm
            self.exclusive = exclusive
            self.passive = passive
            self.active = active
            self.write = write
            self.read = read

        @override
        def decode(self, encoded: bytes, offset: int = 0) -> Access.Request:
            self.realm, offset = data.decode_short_string(encoded, offset)
            bit_buffer = _PACK_OCTET.unpack_from(encoded, offset)[0]
            offset += 1
            self.exclusive = (bit_buffer & (1 << 0)) != 0
            self.passive = (bit_buffer & (1 << 1)) != 0
            self.active = (bit_buffer & (1 << 2)) != 0
            self.write = (bit_buffer & (1 << 3)) != 0
            self.read = (bit_buffer & (1 << 4)) != 0
            return self

        @override
        def encode(self) -> list[bytes]:
            pieces: list[bytes] = []
            assert isinstance(self.realm, (str, bytes)),\
                   'A non-string value was supplied for self.realm'
            data.encode_short_string(pieces, self.realm)
            bit_buffer = 0
            if self.exclusive:
                bit_buffer |= 1 << 0
            if self.passive:
                bit_buffer |= 1 << 1
            if self.active:
                bit_buffer |= 1 << 2
            if self.write:
                bit_buffer |= 1 << 3
            if self.read:
                bit_buffer |= 1 << 4
            pieces.append(_OCTET_BYTES[bit_buffer])
            return pieces

    class RequestOk(amqp_object.Method):

        INDEX = 0x001E000B  # 30, 11; 1966091
        NAME = 'Access.RequestOk'
        synchronous: bool = False

        def __init__(self, ticket: int = 1):
            self.ticket = ticket

        @override
        def decode(self, encoded: bytes, offset: int = 0) -> Access.RequestOk:
            self.ticket = _PACK_SHORT.unpack_from(encoded, offset)[0]
            offset += 2
            return self

        @override
        def encode(self) -> list[bytes]:
            pieces: list[bytes] = []
            pieces.append(_PACK_SHORT.pack(self.ticket))
            return pieces


class Exchange(amqp_object.Class):

    INDEX = 0x0028  # 40
    NAME = 'Exchange'

    class Declare(amqp_object.Method):

        INDEX = 0x0028000A  # 40, 10; 2621450
        NAME = 'Exchange.Declare'
        synchronous: bool = True

        def __init__(self,
                     ticket: int = 0,
                     exchange: str | bytes | None = None,
                     type: str | bytes = 'direct',
                     passive: bool = False,
                     durable: bool = False,
                     auto_delete: bool = False,
                     internal: bool = False,
                     nowait: bool = False,
                     arguments: dict[Any, Any] | None = None):
            self.ticket = ticket
            self.exchange = exchange
            self.type = type
            self.passive = passive
            self.durable = durable
            self.auto_delete = auto_delete
            self.internal = internal
            self.nowait = nowait
            self.arguments = arguments

        @override
        def decode(self, encoded: bytes, offset: int = 0) -> Exchange.Declare:
            self.ticket = _PACK_SHORT.unpack_from(encoded, offset)[0]
            offset += 2
            self.exchange, offset = data.decode_short_string(encoded, offset)
            self.type, offset = data.decode_short_string(encoded, offset)
            bit_buffer = _PACK_OCTET.unpack_from(encoded, offset)[0]
            offset += 1
            self.passive = (bit_buffer & (1 << 0)) != 0
            self.durable = (bit_buffer & (1 << 1)) != 0
            self.auto_delete = (bit_buffer & (1 << 2)) != 0
            self.internal = (bit_buffer & (1 << 3)) != 0
            self.nowait = (bit_buffer & (1 << 4)) != 0
            (self.arguments, offset) = data.decode_table(encoded, offset)
            return self

        @override
        def encode(self) -> list[bytes]:
            pieces: list[bytes] = []
            pieces.append(_PACK_SHORT.pack(self.ticket))
            assert isinstance(self.exchange, (str, bytes)),\
                   'A non-string value was supplied for self.exchange'
            data.encode_short_string(pieces, self.exchange)
            assert isinstance(self.type, (str, bytes)),\
                   'A non-string value was supplied for self.type'
            data.encode_short_string(pieces, self.type)
            bit_buffer = 0
            if self.passive:
                bit_buffer |= 1 << 0
            if self.durable:
                bit_buffer |= 1 << 1
            if self.auto_delete:
                bit_buffer |= 1 << 2
            if self.internal:
                bit_buffer |= 1 << 3
            if self.nowait:
                bit_buffer |= 1 << 4
            pieces.append(_OCTET_BYTES[bit_buffer])
            data.encode_table(pieces, self.arguments)
            return pieces

    class DeclareOk(amqp_object.Method):

        INDEX = 0x0028000B  # 40, 11; 2621451
        NAME = 'Exchange.DeclareOk'
        synchronous: bool = False

        def __init__(self):
            pass

        @override
        def decode(self, encoded: bytes, offset: int = 0) -> Exchange.DeclareOk:
            return self

        @override
        def encode(self) -> list[bytes]:
            pieces: list[bytes] = []
            return pieces

    class Delete(amqp_object.Method):

        INDEX = 0x00280014  # 40, 20; 2621460
        NAME = 'Exchange.Delete'
        synchronous: bool = True

        def __init__(self,
                     ticket: int = 0,
                     exchange: str | bytes | None = None,
                     if_unused: bool = False,
                     nowait: bool = False):
            self.ticket = ticket
            self.exchange = exchange
            self.if_unused = if_unused
            self.nowait = nowait

        @override
        def decode(self, encoded: bytes, offset: int = 0) -> Exchange.Delete:
            self.ticket = _PACK_SHORT.unpack_from(encoded, offset)[0]
            offset += 2
            self.exchange, offset = data.decode_short_string(encoded, offset)
            bit_buffer = _PACK_OCTET.unpack_from(encoded, offset)[0]
            offset += 1
            self.if_unused = (bit_buffer & (1 << 0)) != 0
            self.nowait = (bit_buffer & (1 << 1)) != 0
            return self

        @override
        def encode(self) -> list[bytes]:
            pieces: list[bytes] = []
            pieces.append(_PACK_SHORT.pack(self.ticket))
            assert isinstance(self.exchange, (str, bytes)),\
                   'A non-string value was supplied for self.exchange'
            data.encode_short_string(pieces, self.exchange)
            bit_buffer = 0
            if self.if_unused:
                bit_buffer |= 1 << 0
            if self.nowait:
                bit_buffer |= 1 << 1
            pieces.append(_OCTET_BYTES[bit_buffer])
            return pieces

    class DeleteOk(amqp_object.Method):

        INDEX = 0x00280015  # 40, 21; 2621461
        NAME = 'Exchange.DeleteOk'
        synchronous: bool = False

        def __init__(self):
            pass

        @override
        def decode(self, encoded: bytes, offset: int = 0) -> Exchange.DeleteOk:
            return self

        @override
        def encode(self) -> list[bytes]:
            pieces: list[bytes] = []
            return pieces

    class Bind(amqp_object.Method):

        INDEX = 0x0028001E  # 40, 30; 2621470
        NAME = 'Exchange.Bind'
        synchronous: bool = True

        def __init__(self,
                     ticket: int = 0,
                     destination: str | bytes | None = None,
                     source: str | bytes | None = None,
                     routing_key: str | bytes = '',
                     nowait: bool = False,
                     arguments: dict[Any, Any] | None = None):
            self.ticket = ticket
            self.destination = destination
            self.source = source
            self.routing_key = routing_key
            self.nowait = nowait
            self.arguments = arguments

        @override
        def decode(self, encoded: bytes, offset: int = 0) -> Exchange.Bind:
            self.ticket = _PACK_SHORT.unpack_from(encoded, offset)[0]
            offset += 2
            self.destination, offset = data.decode_short_string(encoded, offset)
            self.source, offset = data.decode_short_string(encoded, offset)
            self.routing_key, offset = data.decode_short_string(encoded, offset)
            bit_buffer = _PACK_OCTET.unpack_from(encoded, offset)[0]
            offset += 1
            self.nowait = (bit_buffer & (1 << 0)) != 0
            (self.arguments, offset) = data.decode_table(encoded, offset)
            return self

        @override
        def encode(self) -> list[bytes]:
            pieces: list[bytes] = []
            pieces.append(_PACK_SHORT.pack(self.ticket))
            assert isinstance(self.destination, (str, bytes)),\
                   'A non-string value was supplied for self.destination'
            data.encode_short_string(pieces, self.destination)
            assert isinstance(self.source, (str, bytes)),\
                   'A non-string value was supplied for self.source'
            data.encode_short_string(pieces, self.source)
            assert isinstance(self.routing_key, (str, bytes)),\
                   'A non-string value was supplied for self.routing_key'
            data.encode_short_string(pieces, self.routing_key)
            bit_buffer = 0
            if self.nowait:
                bit_buffer |= 1 << 0
            pieces.append(_OCTET_BYTES[bit_buffer])
            data.encode_table(pieces, self.arguments)
            return pieces

    class BindOk(amqp_object.Method):

        INDEX = 0x0028001F  # 40, 31; 2621471
        NAME = 'Exchange.BindOk'
        synchronous: bool = False

        def __init__(self):
            pass

        @override
        def decode(self, encoded: bytes, offset: int = 0) -> Exchange.BindOk:
            return self

        @override
        def encode(self) -> list[bytes]:
            pieces: list[bytes] = []
            return pieces

    class Unbind(amqp_object.Method):

        INDEX = 0x00280028  # 40, 40; 2621480
        NAME = 'Exchange.Unbind'
        synchronous: bool = True

        def __init__(self,
                     ticket: int = 0,
                     destination: str | bytes | None = None,
                     source: str | bytes | None = None,
                     routing_key: str | bytes = '',
                     nowait: bool = False,
                     arguments: dict[Any, Any] | None = None):
            self.ticket = ticket
            self.destination = destination
            self.source = source
            self.routing_key = routing_key
            self.nowait = nowait
            self.arguments = arguments

        @override
        def decode(self, encoded: bytes, offset: int = 0) -> Exchange.Unbind:
            self.ticket = _PACK_SHORT.unpack_from(encoded, offset)[0]
            offset += 2
            self.destination, offset = data.decode_short_string(encoded, offset)
            self.source, offset = data.decode_short_string(encoded, offset)
            self.routing_key, offset = data.decode_short_string(encoded, offset)
            bit_buffer = _PACK_OCTET.unpack_from(encoded, offset)[0]
            offset += 1
            self.nowait = (bit_buffer & (1 << 0)) != 0
            (self.arguments, offset) = data.decode_table(encoded, offset)
            return self

        @override
        def encode(self) -> list[bytes]:
            pieces: list[bytes] = []
            pieces.append(_PACK_SHORT.pack(self.ticket))
            assert isinstance(self.destination, (str, bytes)),\
                   'A non-string value was supplied for self.destination'
            data.encode_short_string(pieces, self.destination)
            assert isinstance(self.source, (str, bytes)),\
                   'A non-string value was supplied for self.source'
            data.encode_short_string(pieces, self.source)
            assert isinstance(self.routing_key, (str, bytes)),\
                   'A non-string value was supplied for self.routing_key'
            data.encode_short_string(pieces, self.routing_key)
            bit_buffer = 0
            if self.nowait:
                bit_buffer |= 1 << 0
            pieces.append(_OCTET_BYTES[bit_buffer])
            data.encode_table(pieces, self.arguments)
            return pieces

    class UnbindOk(amqp_object.Method):

        INDEX = 0x00280033  # 40, 51; 2621491
        NAME = 'Exchange.UnbindOk'
        synchronous: bool = False

        def __init__(self):
            pass

        @override
        def decode(self, encoded: bytes, offset: int = 0) -> Exchange.UnbindOk:
            return self

        @override
        def encode(self) -> list[bytes]:
            pieces: list[bytes] = []
            return pieces


class Queue(amqp_object.Class):

    INDEX = 0x0032  # 50
    NAME = 'Queue'

    class Declare(amqp_object.Method):

        INDEX = 0x0032000A  # 50, 10; 3276810
        NAME = 'Queue.Declare'
        synchronous: bool = True

        def __init__(self,
                     ticket: int = 0,
                     queue: str | bytes = '',
                     passive: bool = False,
                     durable: bool = False,
                     exclusive: bool = False,
                     auto_delete: bool = False,
                     nowait: bool = False,
                     arguments: dict[Any, Any] | None = None):
            self.ticket = ticket
            self.queue = queue
            self.passive = passive
            self.durable = durable
            self.exclusive = exclusive
            self.auto_delete = auto_delete
            self.nowait = nowait
            self.arguments = arguments

        @override
        def decode(self, encoded: bytes, offset: int = 0) -> Queue.Declare:
            self.ticket = _PACK_SHORT.unpack_from(encoded, offset)[0]
            offset += 2
            self.queue, offset = data.decode_short_string(encoded, offset)
            bit_buffer = _PACK_OCTET.unpack_from(encoded, offset)[0]
            offset += 1
            self.passive = (bit_buffer & (1 << 0)) != 0
            self.durable = (bit_buffer & (1 << 1)) != 0
            self.exclusive = (bit_buffer & (1 << 2)) != 0
            self.auto_delete = (bit_buffer & (1 << 3)) != 0
            self.nowait = (bit_buffer & (1 << 4)) != 0
            (self.arguments, offset) = data.decode_table(encoded, offset)
            return self

        @override
        def encode(self) -> list[bytes]:
            pieces: list[bytes] = []
            pieces.append(_PACK_SHORT.pack(self.ticket))
            assert isinstance(self.queue, (str, bytes)),\
                   'A non-string value was supplied for self.queue'
            data.encode_short_string(pieces, self.queue)
            bit_buffer = 0
            if self.passive:
                bit_buffer |= 1 << 0
            if self.durable:
                bit_buffer |= 1 << 1
            if self.exclusive:
                bit_buffer |= 1 << 2
            if self.auto_delete:
                bit_buffer |= 1 << 3
            if self.nowait:
                bit_buffer |= 1 << 4
            pieces.append(_OCTET_BYTES[bit_buffer])
            data.encode_table(pieces, self.arguments)
            return pieces

    class DeclareOk(amqp_object.Method):

        INDEX = 0x0032000B  # 50, 11; 3276811
        NAME = 'Queue.DeclareOk'
        synchronous: bool = False

        def __init__(self,
                     queue: str | bytes | None = None,
                     message_count: int | None = None,
                     consumer_count: int | None = None):
            self.queue = queue
            self.message_count = message_count
            self.consumer_count = consumer_count

        @override
        def decode(self, encoded: bytes, offset: int = 0) -> Queue.DeclareOk:
            self.queue, offset = data.decode_short_string(encoded, offset)
            self.message_count = _PACK_LONG.unpack_from(encoded, offset)[0]
            offset += 4
            self.consumer_count = _PACK_LONG.unpack_from(encoded, offset)[0]
            offset += 4
            return self

        @override
        def encode(self) -> list[bytes]:
            pieces: list[bytes] = []
            assert isinstance(self.queue, (str, bytes)),\
                   'A non-string value was supplied for self.queue'
            data.encode_short_string(pieces, self.queue)
            pieces.append(_PACK_LONG.pack(self.message_count))
            pieces.append(_PACK_LONG.pack(self.consumer_count))
            return pieces

    class Bind(amqp_object.Method):

        INDEX = 0x00320014  # 50, 20; 3276820
        NAME = 'Queue.Bind'
        synchronous: bool = True

        def __init__(self,
                     ticket: int = 0,
                     queue: str | bytes = '',
                     exchange: str | bytes | None = None,
                     routing_key: str | bytes = '',
                     nowait: bool = False,
                     arguments: dict[Any, Any] | None = None):
            self.ticket = ticket
            self.queue = queue
            self.exchange = exchange
            self.routing_key = routing_key
            self.nowait = nowait
            self.arguments = arguments

        @override
        def decode(self, encoded: bytes, offset: int = 0) -> Queue.Bind:
            self.ticket = _PACK_SHORT.unpack_from(encoded, offset)[0]
            offset += 2
            self.queue, offset = data.decode_short_string(encoded, offset)
            self.exchange, offset = data.decode_short_string(encoded, offset)
            self.routing_key, offset = data.decode_short_string(encoded, offset)
            bit_buffer = _PACK_OCTET.unpack_from(encoded, offset)[0]
            offset += 1
            self.nowait = (bit_buffer & (1 << 0)) != 0
            (self.arguments, offset) = data.decode_table(encoded, offset)
            return self

        @override
        def encode(self) -> list[bytes]:
            pieces: list[bytes] = []
            pieces.append(_PACK_SHORT.pack(self.ticket))
            assert isinstance(self.queue, (str, bytes)),\
                   'A non-string value was supplied for self.queue'
            data.encode_short_string(pieces, self.queue)
            assert isinstance(self.exchange, (str, bytes)),\
                   'A non-string value was supplied for self.exchange'
            data.encode_short_string(pieces, self.exchange)
            assert isinstance(self.routing_key, (str, bytes)),\
                   'A non-string value was supplied for self.routing_key'
            data.encode_short_string(pieces, self.routing_key)
            bit_buffer = 0
            if self.nowait:
                bit_buffer |= 1 << 0
            pieces.append(_OCTET_BYTES[bit_buffer])
            data.encode_table(pieces, self.arguments)
            return pieces

    class BindOk(amqp_object.Method):

        INDEX = 0x00320015  # 50, 21; 3276821
        NAME = 'Queue.BindOk'
        synchronous: bool = False

        def __init__(self):
            pass

        @override
        def decode(self, encoded: bytes, offset: int = 0) -> Queue.BindOk:
            return self

        @override
        def encode(self) -> list[bytes]:
            pieces: list[bytes] = []
            return pieces

    class Purge(amqp_object.Method):

        INDEX = 0x0032001E  # 50, 30; 3276830
        NAME = 'Queue.Purge'
        synchronous: bool = True

        def __init__(self,
                     ticket: int = 0,
                     queue: str | bytes = '',
                     nowait: bool = False):
            self.ticket = ticket
            self.queue = queue
            self.nowait = nowait

        @override
        def decode(self, encoded: bytes, offset: int = 0) -> Queue.Purge:
            self.ticket = _PACK_SHORT.unpack_from(encoded, offset)[0]
            offset += 2
            self.queue, offset = data.decode_short_string(encoded, offset)
            bit_buffer = _PACK_OCTET.unpack_from(encoded, offset)[0]
            offset += 1
            self.nowait = (bit_buffer & (1 << 0)) != 0
            return self

        @override
        def encode(self) -> list[bytes]:
            pieces: list[bytes] = []
            pieces.append(_PACK_SHORT.pack(self.ticket))
            assert isinstance(self.queue, (str, bytes)),\
                   'A non-string value was supplied for self.queue'
            data.encode_short_string(pieces, self.queue)
            bit_buffer = 0
            if self.nowait:
                bit_buffer |= 1 << 0
            pieces.append(_OCTET_BYTES[bit_buffer])
            return pieces

    class PurgeOk(amqp_object.Method):

        INDEX = 0x0032001F  # 50, 31; 3276831
        NAME = 'Queue.PurgeOk'
        synchronous: bool = False

        def __init__(self, message_count: int | None = None):
            self.message_count = message_count

        @override
        def decode(self, encoded: bytes, offset: int = 0) -> Queue.PurgeOk:
            self.message_count = _PACK_LONG.unpack_from(encoded, offset)[0]
            offset += 4
            return self

        @override
        def encode(self) -> list[bytes]:
            pieces: list[bytes] = []
            pieces.append(_PACK_LONG.pack(self.message_count))
            return pieces

    class Delete(amqp_object.Method):

        INDEX = 0x00320028  # 50, 40; 3276840
        NAME = 'Queue.Delete'
        synchronous: bool = True

        def __init__(self,
                     ticket: int = 0,
                     queue: str | bytes = '',
                     if_unused: bool = False,
                     if_empty: bool = False,
                     nowait: bool = False):
            self.ticket = ticket
            self.queue = queue
            self.if_unused = if_unused
            self.if_empty = if_empty
            self.nowait = nowait

        @override
        def decode(self, encoded: bytes, offset: int = 0) -> Queue.Delete:
            self.ticket = _PACK_SHORT.unpack_from(encoded, offset)[0]
            offset += 2
            self.queue, offset = data.decode_short_string(encoded, offset)
            bit_buffer = _PACK_OCTET.unpack_from(encoded, offset)[0]
            offset += 1
            self.if_unused = (bit_buffer & (1 << 0)) != 0
            self.if_empty = (bit_buffer & (1 << 1)) != 0
            self.nowait = (bit_buffer & (1 << 2)) != 0
            return self

        @override
        def encode(self) -> list[bytes]:
            pieces: list[bytes] = []
            pieces.append(_PACK_SHORT.pack(self.ticket))
            assert isinstance(self.queue, (str, bytes)),\
                   'A non-string value was supplied for self.queue'
            data.encode_short_string(pieces, self.queue)
            bit_buffer = 0
            if self.if_unused:
                bit_buffer |= 1 << 0
            if self.if_empty:
                bit_buffer |= 1 << 1
            if self.nowait:
                bit_buffer |= 1 << 2
            pieces.append(_OCTET_BYTES[bit_buffer])
            return pieces

    class DeleteOk(amqp_object.Method):

        INDEX = 0x00320029  # 50, 41; 3276841
        NAME = 'Queue.DeleteOk'
        synchronous: bool = False

        def __init__(self, message_count: int | None = None):
            self.message_count = message_count

        @override
        def decode(self, encoded: bytes, offset: int = 0) -> Queue.DeleteOk:
            self.message_count = _PACK_LONG.unpack_from(encoded, offset)[0]
            offset += 4
            return self

        @override
        def encode(self) -> list[bytes]:
            pieces: list[bytes] = []
            pieces.append(_PACK_LONG.pack(self.message_count))
            return pieces

    class Unbind(amqp_object.Method):

        INDEX = 0x00320032  # 50, 50; 3276850
        NAME = 'Queue.Unbind'
        synchronous: bool = True

        def __init__(self,
                     ticket: int = 0,
                     queue: str | bytes = '',
                     exchange: str | bytes | None = None,
                     routing_key: str | bytes = '',
                     arguments: dict[Any, Any] | None = None):
            self.ticket = ticket
            self.queue = queue
            self.exchange = exchange
            self.routing_key = routing_key
            self.arguments = arguments

        @override
        def decode(self, encoded: bytes, offset: int = 0) -> Queue.Unbind:
            self.ticket = _PACK_SHORT.unpack_from(encoded, offset)[0]
            offset += 2
            self.queue, offset = data.decode_short_string(encoded, offset)
            self.exchange, offset = data.decode_short_string(encoded, offset)
            self.routing_key, offset = data.decode_short_string(encoded, offset)
            (self.arguments, offset) = data.decode_table(encoded, offset)
            return self

        @override
        def encode(self) -> list[bytes]:
            pieces: list[bytes] = []
            pieces.append(_PACK_SHORT.pack(self.ticket))
            assert isinstance(self.queue, (str, bytes)),\
                   'A non-string value was supplied for self.queue'
            data.encode_short_string(pieces, self.queue)
            assert isinstance(self.exchange, (str, bytes)),\
                   'A non-string value was supplied for self.exchange'
            data.encode_short_string(pieces, self.exchange)
            assert isinstance(self.routing_key, (str, bytes)),\
                   'A non-string value was supplied for self.routing_key'
            data.encode_short_string(pieces, self.routing_key)
            data.encode_table(pieces, self.arguments)
            return pieces

    class UnbindOk(amqp_object.Method):

        INDEX = 0x00320033  # 50, 51; 3276851
        NAME = 'Queue.UnbindOk'
        synchronous: bool = False

        def __init__(self):
            pass

        @override
        def decode(self, encoded: bytes, offset: int = 0) -> Queue.UnbindOk:
            return self

        @override
        def encode(self) -> list[bytes]:
            pieces: list[bytes] = []
            return pieces


class Tx(amqp_object.Class):

    INDEX = 0x005A  # 90
    NAME = 'Tx'

    class Select(amqp_object.Method):

        INDEX = 0x005A000A  # 90, 10; 5898250
        NAME = 'Tx.Select'
        synchronous: bool = True

        def __init__(self):
            pass

        @override
        def decode(self, encoded: bytes, offset: int = 0) -> Tx.Select:
            return self

        @override
        def encode(self) -> list[bytes]:
            pieces: list[bytes] = []
            return pieces

    class SelectOk(amqp_object.Method):

        INDEX = 0x005A000B  # 90, 11; 5898251
        NAME = 'Tx.SelectOk'
        synchronous: bool = False

        def __init__(self):
            pass

        @override
        def decode(self, encoded: bytes, offset: int = 0) -> Tx.SelectOk:
            return self

        @override
        def encode(self) -> list[bytes]:
            pieces: list[bytes] = []
            return pieces

    class Commit(amqp_object.Method):

        INDEX = 0x005A0014  # 90, 20; 5898260
        NAME = 'Tx.Commit'
        synchronous: bool = True

        def __init__(self):
            pass

        @override
        def decode(self, encoded: bytes, offset: int = 0) -> Tx.Commit:
            return self

        @override
        def encode(self) -> list[bytes]:
            pieces: list[bytes] = []
            return pieces

    class CommitOk(amqp_object.Method):

        INDEX = 0x005A0015  # 90, 21; 5898261
        NAME = 'Tx.CommitOk'
        synchronous: bool = False

        def __init__(self):
            pass

        @override
        def decode(self, encoded: bytes, offset: int = 0) -> Tx.CommitOk:
            return self

        @override
        def encode(self) -> list[bytes]:
            pieces: list[bytes] = []
            return pieces

    class Rollback(amqp_object.Method):

        INDEX = 0x005A001E  # 90, 30; 5898270
        NAME = 'Tx.Rollback'
        synchronous: bool = True

        def __init__(self):
            pass

        @override
        def decode(self, encoded: bytes, offset: int = 0) -> Tx.Rollback:
            return self

        @override
        def encode(self) -> list[bytes]:
            pieces: list[bytes] = []
            return pieces

    class RollbackOk(amqp_object.Method):

        INDEX = 0x005A001F  # 90, 31; 5898271
        NAME = 'Tx.RollbackOk'
        synchronous: bool = False

        def __init__(self):
            pass

        @override
        def decode(self, encoded: bytes, offset: int = 0) -> Tx.RollbackOk:
            return self

        @override
        def encode(self) -> list[bytes]:
            pieces: list[bytes] = []
            return pieces


class Confirm(amqp_object.Class):

    INDEX = 0x0055  # 85
    NAME = 'Confirm'

    class Select(amqp_object.Method):

        INDEX = 0x0055000A  # 85, 10; 5570570
        NAME = 'Confirm.Select'
        synchronous: bool = True

        def __init__(self, nowait: bool = False):
            self.nowait = nowait

        @override
        def decode(self, encoded: bytes, offset: int = 0) -> Confirm.Select:
            bit_buffer = _PACK_OCTET.unpack_from(encoded, offset)[0]
            offset += 1
            self.nowait = (bit_buffer & (1 << 0)) != 0
            return self

        @override
        def encode(self) -> list[bytes]:
            pieces: list[bytes] = []
            bit_buffer = 0
            if self.nowait:
                bit_buffer |= 1 << 0
            pieces.append(_OCTET_BYTES[bit_buffer])
            return pieces

    class SelectOk(amqp_object.Method):

        INDEX = 0x0055000B  # 85, 11; 5570571
        NAME = 'Confirm.SelectOk'
        synchronous: bool = False

        def __init__(self):
            pass

        @override
        def decode(self, encoded: bytes, offset: int = 0) -> Confirm.SelectOk:
            return self

        @override
        def encode(self) -> list[bytes]:
            pieces: list[bytes] = []
            return pieces


class BasicProperties(amqp_object.Properties):

    CLASS = Basic
    INDEX = 0x003C  # 60
    NAME = 'BasicProperties'

    FLAG_CONTENT_TYPE = (1 << 15)
    FLAG_CONTENT_ENCODING = (1 << 14)
    FLAG_HEADERS = (1 << 13)
    FLAG_DELIVERY_MODE = (1 << 12)
    FLAG_PRIORITY = (1 << 11)
    FLAG_CORRELATION_ID = (1 << 10)
    FLAG_REPLY_TO = (1 << 9)
    FLAG_EXPIRATION = (1 << 8)
    FLAG_MESSAGE_ID = (1 << 7)
    FLAG_TIMESTAMP = (1 << 6)
    FLAG_TYPE = (1 << 5)
    FLAG_USER_ID = (1 << 4)
    FLAG_APP_ID = (1 << 3)
    FLAG_CLUSTER_ID = (1 << 2)

    def __init__(self,
                 content_type: str | bytes | None = None,
                 content_encoding: str | bytes | None = None,
                 headers: dict[Any, Any] | None = None,
                 delivery_mode: int | None = None,
                 priority: int | None = None,
                 correlation_id: str | bytes | None = None,
                 reply_to: str | bytes | None = None,
                 expiration: str | bytes | None = None,
                 message_id: str | bytes | None = None,
                 timestamp: int | None = None,
                 type: str | bytes | None = None,
                 user_id: str | bytes | None = None,
                 app_id: str | bytes | None = None,
                 cluster_id: str | bytes | None = None):
        self.content_type = content_type
        self.content_encoding = content_encoding
        self.headers = headers
        self.delivery_mode = delivery_mode
        self.priority = priority
        self.correlation_id = correlation_id
        self.reply_to = reply_to
        self.expiration = expiration
        self.message_id = message_id
        self.timestamp = timestamp
        self.type = type
        self.user_id = user_id
        self.app_id = app_id
        self.cluster_id = cluster_id

    def decode(self, encoded: bytes, offset: int = 0) -> BasicProperties:
        flags = 0
        flagword_index = 0
        while True:
            partial_flags = _PACK_SHORT.unpack_from(encoded, offset)[0]
            offset += 2
            flags = flags | (partial_flags << (flagword_index * 16))
            if not (partial_flags & 1):
                break
            flagword_index += 1
        if flags & BasicProperties.FLAG_CONTENT_TYPE:
            self.content_type, offset = data.decode_short_string(
                encoded, offset)
        else:
            self.content_type = None
        if flags & BasicProperties.FLAG_CONTENT_ENCODING:
            self.content_encoding, offset = data.decode_short_string(
                encoded, offset)
        else:
            self.content_encoding = None
        if flags & BasicProperties.FLAG_HEADERS:
            (self.headers, offset) = data.decode_table(encoded, offset)
        else:
            self.headers = None
        if flags & BasicProperties.FLAG_DELIVERY_MODE:
            self.delivery_mode = _PACK_OCTET.unpack_from(encoded, offset)[0]
            offset += 1
        else:
            self.delivery_mode = None
        if flags & BasicProperties.FLAG_PRIORITY:
            self.priority = _PACK_OCTET.unpack_from(encoded, offset)[0]
            offset += 1
        else:
            self.priority = None
        if flags & BasicProperties.FLAG_CORRELATION_ID:
            self.correlation_id, offset = data.decode_short_string(
                encoded, offset)
        else:
            self.correlation_id = None
        if flags & BasicProperties.FLAG_REPLY_TO:
            self.reply_to, offset = data.decode_short_string(encoded, offset)
        else:
            self.reply_to = None
        if flags & BasicProperties.FLAG_EXPIRATION:
            self.expiration, offset = data.decode_short_string(encoded, offset)
        else:
            self.expiration = None
        if flags & BasicProperties.FLAG_MESSAGE_ID:
            self.message_id, offset = data.decode_short_string(encoded, offset)
        else:
            self.message_id = None
        if flags & BasicProperties.FLAG_TIMESTAMP:
            self.timestamp = _PACK_LONGLONG.unpack_from(encoded, offset)[0]
            offset += 8
        else:
            self.timestamp = None
        if flags & BasicProperties.FLAG_TYPE:
            self.type, offset = data.decode_short_string(encoded, offset)
        else:
            self.type = None
        if flags & BasicProperties.FLAG_USER_ID:
            self.user_id, offset = data.decode_short_string(encoded, offset)
        else:
            self.user_id = None
        if flags & BasicProperties.FLAG_APP_ID:
            self.app_id, offset = data.decode_short_string(encoded, offset)
        else:
            self.app_id = None
        if flags & BasicProperties.FLAG_CLUSTER_ID:
            self.cluster_id, offset = data.decode_short_string(encoded, offset)
        else:
            self.cluster_id = None
        return self

    def encode(self) -> list[bytes]:
        pieces: list[bytes] = []
        flags = 0
        if self.content_type is not None:
            flags = flags | BasicProperties.FLAG_CONTENT_TYPE
            assert isinstance(self.content_type, (str, bytes)),\
                   'A non-string value was supplied for self.content_type'
            data.encode_short_string(pieces, self.content_type)
        if self.content_encoding is not None:
            flags = flags | BasicProperties.FLAG_CONTENT_ENCODING
            assert isinstance(self.content_encoding, (str, bytes)),\
                   'A non-string value was supplied for self.content_encoding'
            data.encode_short_string(pieces, self.content_encoding)
        if self.headers is not None:
            flags = flags | BasicProperties.FLAG_HEADERS
            data.encode_table(pieces, self.headers)
        if self.delivery_mode is not None:
            flags = flags | BasicProperties.FLAG_DELIVERY_MODE
            pieces.append(_PACK_OCTET.pack(self.delivery_mode))
        if self.priority is not None:
            flags = flags | BasicProperties.FLAG_PRIORITY
            pieces.append(_PACK_OCTET.pack(self.priority))
        if self.correlation_id is not None:
            flags = flags | BasicProperties.FLAG_CORRELATION_ID
            assert isinstance(self.correlation_id, (str, bytes)),\
                   'A non-string value was supplied for self.correlation_id'
            data.encode_short_string(pieces, self.correlation_id)
        if self.reply_to is not None:
            flags = flags | BasicProperties.FLAG_REPLY_TO
            assert isinstance(self.reply_to, (str, bytes)),\
                   'A non-string value was supplied for self.reply_to'
            data.encode_short_string(pieces, self.reply_to)
        if self.expiration is not None:
            flags = flags | BasicProperties.FLAG_EXPIRATION
            assert isinstance(self.expiration, (str, bytes)),\
                   'A non-string value was supplied for self.expiration'
            data.encode_short_string(pieces, self.expiration)
        if self.message_id is not None:
            flags = flags | BasicProperties.FLAG_MESSAGE_ID
            assert isinstance(self.message_id, (str, bytes)),\
                   'A non-string value was supplied for self.message_id'
            data.encode_short_string(pieces, self.message_id)
        if self.timestamp is not None:
            flags = flags | BasicProperties.FLAG_TIMESTAMP
            pieces.append(_PACK_LONGLONG.pack(self.timestamp))
        if self.type is not None:
            flags = flags | BasicProperties.FLAG_TYPE
            assert isinstance(self.type, (str, bytes)),\
                   'A non-string value was supplied for self.type'
            data.encode_short_string(pieces, self.type)
        if self.user_id is not None:
            flags = flags | BasicProperties.FLAG_USER_ID
            assert isinstance(self.user_id, (str, bytes)),\
                   'A non-string value was supplied for self.user_id'
            data.encode_short_string(pieces, self.user_id)
        if self.app_id is not None:
            flags = flags | BasicProperties.FLAG_APP_ID
            assert isinstance(self.app_id, (str, bytes)),\
                   'A non-string value was supplied for self.app_id'
            data.encode_short_string(pieces, self.app_id)
        if self.cluster_id is not None:
            flags = flags | BasicProperties.FLAG_CLUSTER_ID
            assert isinstance(self.cluster_id, (str, bytes)),\
                   'A non-string value was supplied for self.cluster_id'
            data.encode_short_string(pieces, self.cluster_id)
        flag_pieces: list[bytes] = []
        while True:
            remainder = flags >> 16
            partial_flags = flags & 0xFFFE
            if remainder != 0:
                partial_flags |= 1
            flag_pieces.append(_PACK_SHORT.pack(partial_flags))
            flags = remainder
            if not flags:
                break
        return flag_pieces + pieces


methods: dict[int, type[amqp_object.Method]] = {
    0x003C000A: Basic.Qos,
    0x003C000B: Basic.QosOk,
    0x003C0014: Basic.Consume,
    0x003C0015: Basic.ConsumeOk,
    0x003C001E: Basic.Cancel,
    0x003C001F: Basic.CancelOk,
    0x003C0028: Basic.Publish,
    0x003C0032: Basic.Return,
    0x003C003C: Basic.Deliver,
    0x003C0046: Basic.Get,
    0x003C0047: Basic.GetOk,
    0x003C0048: Basic.GetEmpty,
    0x003C0050: Basic.Ack,
    0x003C005A: Basic.Reject,
    0x003C0064: Basic.RecoverAsync,
    0x003C006E: Basic.Recover,
    0x003C006F: Basic.RecoverOk,
    0x003C0078: Basic.Nack,
    0x000A000A: Connection.Start,
    0x000A000B: Connection.StartOk,
    0x000A0014: Connection.Secure,
    0x000A0015: Connection.SecureOk,
    0x000A001E: Connection.Tune,
    0x000A001F: Connection.TuneOk,
    0x000A0028: Connection.Open,
    0x000A0029: Connection.OpenOk,
    0x000A0032: Connection.Close,
    0x000A0033: Connection.CloseOk,
    0x000A003C: Connection.Blocked,
    0x000A003D: Connection.Unblocked,
    0x000A0046: Connection.UpdateSecret,
    0x000A0047: Connection.UpdateSecretOk,
    0x0014000A: Channel.Open,
    0x0014000B: Channel.OpenOk,
    0x00140014: Channel.Flow,
    0x00140015: Channel.FlowOk,
    0x00140028: Channel.Close,
    0x00140029: Channel.CloseOk,
    0x001E000A: Access.Request,
    0x001E000B: Access.RequestOk,
    0x0028000A: Exchange.Declare,
    0x0028000B: Exchange.DeclareOk,
    0x00280014: Exchange.Delete,
    0x00280015: Exchange.DeleteOk,
    0x0028001E: Exchange.Bind,
    0x0028001F: Exchange.BindOk,
    0x00280028: Exchange.Unbind,
    0x00280033: Exchange.UnbindOk,
    0x0032000A: Queue.Declare,
    0x0032000B: Queue.DeclareOk,
    0x00320014: Queue.Bind,
    0x00320015: Queue.BindOk,
    0x0032001E: Queue.Purge,
    0x0032001F: Queue.PurgeOk,
    0x00320028: Queue.Delete,
    0x00320029: Queue.DeleteOk,
    0x00320032: Queue.Unbind,
    0x00320033: Queue.UnbindOk,
    0x005A000A: Tx.Select,
    0x005A000B: Tx.SelectOk,
    0x005A0014: Tx.Commit,
    0x005A0015: Tx.CommitOk,
    0x005A001E: Tx.Rollback,
    0x005A001F: Tx.RollbackOk,
    0x0055000A: Confirm.Select,
    0x0055000B: Confirm.SelectOk
}

props: dict[int, type[BasicProperties]] = {0x003C: BasicProperties}


def has_content(methodNumber: int) -> bool:
    return methodNumber in (
        Basic.Publish.INDEX,
        Basic.Return.INDEX,
        Basic.Deliver.INDEX,
        Basic.GetOk.INDEX,
    )
