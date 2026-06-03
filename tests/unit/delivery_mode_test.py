"""
Tests for pika.delivery_mode
"""
import struct
import unittest

from pika.delivery_mode import DeliveryMode
from pika.spec import BasicProperties


class DeliveryModeTests(unittest.TestCase):

    def test_delivery_mode_transient_value(self):
        self.assertEqual(DeliveryMode.Transient.value, 1)

    def test_delivery_mode_persistent_value(self):
        self.assertEqual(DeliveryMode.Persistent.value, 2)

    def test_delivery_mode_is_int(self):
        """DeliveryMode members must be usable as integers.

        Required for compatibility with the wire encoder, which calls
        ``struct.pack('B', delivery_mode)``.  ``struct.pack`` requires
        ``__index__()`` on its argument; a plain ``Enum`` does not satisfy
        this and a publish would raise ``struct.error: required argument
        is not an integer``.
        """
        self.assertEqual(int(DeliveryMode.Transient), 1)
        self.assertEqual(int(DeliveryMode.Persistent), 2)
        # __index__ is what struct.pack calls.
        self.assertEqual(DeliveryMode.Transient.__index__(), 1)
        self.assertEqual(DeliveryMode.Persistent.__index__(), 2)
        # Direct equality with int.
        self.assertEqual(DeliveryMode.Transient, 1)
        self.assertEqual(DeliveryMode.Persistent, 2)

    def test_struct_pack_accepts_delivery_mode(self):
        """``struct.pack('B', delivery_mode)`` must succeed - this is the
        exact call BasicProperties.encode() makes for the delivery_mode
        field.  A plain ``Enum`` would raise here.
        """
        self.assertEqual(struct.pack('B', DeliveryMode.Transient), b'\x01')
        self.assertEqual(struct.pack('B', DeliveryMode.Persistent), b'\x02')

    def test_basic_properties_encode_with_enum(self):
        """A ``BasicProperties`` constructed with a ``DeliveryMode`` enum
        member must encode without error.  This exercises the real
        publish path that broke when ``DeliveryMode`` was a plain enum.
        """
        props = BasicProperties(delivery_mode=DeliveryMode.Persistent)
        encoded = b''.join(props.encode())
        self.assertIsInstance(encoded, bytes)
        self.assertGreater(len(encoded), 0)

    def test_basic_properties_encode_with_integer(self):
        """A ``BasicProperties`` constructed with an integer delivery_mode
        must continue to encode without error - backward compatibility for
        callers that have not migrated to the enum.
        """
        props = BasicProperties(delivery_mode=2)
        encoded = b''.join(props.encode())
        self.assertIsInstance(encoded, bytes)
        self.assertGreater(len(encoded), 0)

    def test_basic_properties_round_trip_with_enum(self):
        """Encoding a ``BasicProperties`` with a ``DeliveryMode`` enum
        member and decoding it must recover the integer value.
        """
        original = BasicProperties(delivery_mode=DeliveryMode.Persistent)
        encoded = b''.join(original.encode())
        decoded = BasicProperties()
        decoded.decode(encoded)
        # Decode reads via struct.unpack_from which returns a raw int.
        self.assertEqual(decoded.delivery_mode, 2)
        # The raw int must compare equal to the enum member.
        self.assertEqual(decoded.delivery_mode, DeliveryMode.Persistent)

    def test_basic_properties_round_trip_with_integer(self):
        """Encoding with an integer literal and decoding must recover the
        same integer.
        """
        original = BasicProperties(delivery_mode=1)
        encoded = b''.join(original.encode())
        decoded = BasicProperties()
        decoded.decode(encoded)
        self.assertEqual(decoded.delivery_mode, 1)

    def test_basic_properties_encode_rejects_non_integer(self):
        """A ``BasicProperties`` constructed with a non-integer
        delivery_mode (e.g. a string) must raise at encode time so callers
        passing the wrong type get a useful diagnostic.
        """
        props = BasicProperties(delivery_mode='persistent')  # type: ignore
        with self.assertRaises((struct.error, TypeError)):
            b''.join(props.encode())
