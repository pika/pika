# ***** BEGIN LICENSE BLOCK *****
#
# For copyright and licensing please refer to COPYING.
#
# ***** END LICENSE BLOCK *****

"""
Tests for the classes in pika.frames

"""

#############
# Imports
#############
# builtins
import unittest
import struct

# third-party
from mock import Mock

# homegrown
from pika import frames
import pika.spec as spec


__author__ = 'Brian K. Jones'
__email__ = 'bkjones@gmail.com'
__since__ = '2/25/11'

class TestFrame(unittest.TestCase):
    def setUp(self):
        self.frame = frames.Frame('ftype', 3)

    def test_init(self):
        self.assertEqual(self.frame.frame_type, 'ftype')
        self.assertEqual(self.frame.channel_number, 3)
