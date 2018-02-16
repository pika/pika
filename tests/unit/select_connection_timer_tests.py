# -*- coding: utf-8 -*-
"""
Tests for SelectConnection _Timer and _Timeout classes

"""

import time
import unittest

import mock

from pika.adapters import select_connection


# Suppress protected-access
# pylint: disable=W0212

# Suppress missing-docstring
# pylint: disable=C0111

# Suppress invalid-name
# pylint: disable=C0103

class TimeoutClassTests(unittest.TestCase):
    """Test select_connection._Timeout class"""

    def test_properties(self):
        now = time.time()
        cb = lambda: None
        timeout = select_connection._Timeout(now + 5.3, cb)
        self.assertIs(timeout.callback, cb)
        self.assertEqual(timeout.deadline, now + 5.3)

    def test_non_negative_deadline(self):
        select_connection._Timeout(0, lambda: None)
        select_connection._Timeout(5, lambda: None)

        with self.assertRaises(ValueError) as cm:
            select_connection._Timeout(-1, lambda: None)

        self.assertIn('deadline must be non-negative epoch number',
                      cm.exception.args[0])

    def test_non_callable_callback_raises(self):
        with self.assertRaises(TypeError) as cm:
            select_connection._Timeout(5, None)

        self.assertIn('callback must be a callable, but got',
                      cm.exception.args[0])

        with self.assertRaises(TypeError) as cm:
            select_connection._Timeout(5, dict())

        self.assertIn('callback must be a callable, but got',
                      cm.exception.args[0])

    def test_eq_operator(self):
        # Comparison should be by deadline only
        t1 = select_connection._Timeout(5, lambda: None)
        t2 = select_connection._Timeout(5, lambda: 5)
        self.assertEqual(t1, t2)

        t2 = select_connection._Timeout(10, lambda: 5)
        self.assertNotEqual(t1, t2)

    def test_lt_operator(self):
        # Comparison should be by deadline only
        t1 = select_connection._Timeout(4, lambda: None)
        t2 = select_connection._Timeout(5, lambda: 5)
        self.assertLess(t1, t2)

        t2 = select_connection._Timeout(4, lambda: 5)
        self.assertFalse(t1 < t2)

        t2 = select_connection._Timeout(3, lambda: 5)
        self.assertFalse(t1 < t2)

    def test_le_operator(self):
        # Comparison should be by deadline only
        t1 = select_connection._Timeout(4, lambda: None)
        t2 = select_connection._Timeout(4, lambda: 5)
        self.assertLessEqual(t1, t2)

        t2 = select_connection._Timeout(5, lambda: 5)
        self.assertLessEqual(t1, t2)

        t2 = select_connection._Timeout(3, lambda: 5)
        self.assertFalse(t1 <= t2)


class TimerClassTests(unittest.TestCase):
    """Test select_connection._Timer class"""

    def test_no_timeouts_remaining_interval_is_none(self):
        timer = select_connection._Timer()
        self.assertIsNone(timer.get_remaining_interval())

    def test_call_later_non_negative_delay_check(self):
        now = time.time()

        # 0 delay is okay
        with mock.patch('time.time', return_value=now):
            timer = select_connection._Timer()
            timer.call_later(0, lambda: None)
            self.assertEqual(timer._timeout_heap[0].deadline, now)
            self.assertEqual(timer.get_remaining_interval(), 0)

        # Positive delay is okay
        with mock.patch('time.time', return_value=now):
            timer = select_connection._Timer()
            timer.call_later(0.5, lambda: None)
            self.assertEqual(timer._timeout_heap[0].deadline, now + 0.5)
            self.assertEqual(timer.get_remaining_interval(), 0.5)

        # Negative delay raises ValueError
        timer = select_connection._Timer()
        with self.assertRaises(ValueError) as cm:
            timer.call_later(-5, lambda: None)
        self.assertIn('call_later: delay must be non-negative, but got',
                      cm.exception.args[0])

    def test_call_later_single_timer_expires(self):
        now = time.time()

        with mock.patch('time.time', return_value=now):
            bucket = []
            timer = select_connection._Timer()
            timer.call_later(5, lambda: bucket.append(1))

            # Nothing is ready to expire
            timer.process_timeouts()
            self.assertEqual(bucket, [])
            self.assertEqual(timer.get_remaining_interval(), 5)

        # Advance time by 5 seconds and expect the timer to expire
        with mock.patch('time.time', return_value=now + 5):
            self.assertEqual(timer.get_remaining_interval(), 0)
            timer.process_timeouts()
            self.assertEqual(bucket, [1])
            self.assertEqual(len(timer._timeout_heap), 0)
            self.assertIsNone(timer.get_remaining_interval())

    def test_call_later_multiple_timers(self):
        now = time.time()

        bucket = []
        timer = select_connection._Timer()

        with mock.patch('time.time', return_value=now):
            timer.call_later(5, lambda: bucket.append(1))
            timer.call_later(5, lambda: bucket.append(2))
            timer.call_later(10, lambda: bucket.append(3))

            # Nothing is ready to fire yet
            self.assertEqual(timer.get_remaining_interval(), 5)
            timer.process_timeouts()
            self.assertEqual(bucket, [])
            self.assertEqual(timer.get_remaining_interval(), 5)

        # Advance time by 6 seconds and expect first two timers to expire
        with mock.patch('time.time', return_value=now + 6):
            self.assertEqual(timer.get_remaining_interval(), 0)
            timer.process_timeouts()
            self.assertEqual(bucket, [1, 2])
            self.assertEqual(len(timer._timeout_heap), 1)
            self.assertEqual(timer.get_remaining_interval(), 4)

        # Advance time by 10 seconds and expect the 3rd timeout to expire
        with mock.patch('time.time', return_value=now + 10):
            self.assertEqual(timer.get_remaining_interval(), 0)
            timer.process_timeouts()
            self.assertEqual(bucket, [1, 2, 3])
            self.assertEqual(len(timer._timeout_heap), 0)
            self.assertIsNone(timer.get_remaining_interval())

    def test_add_and_remove_timeout(self):
        now = time.time()

        bucket = []
        timer = select_connection._Timer()

        with mock.patch('time.time', return_value=now):
            timer.call_later(10, lambda: bucket.append(3)) # t3
            t2 = timer.call_later(6, lambda: bucket.append(2))
            t1 = timer.call_later(5, lambda: bucket.append(1))

            # Nothing is ready to fire yet
            self.assertEqual(timer.get_remaining_interval(), 5)
            timer.process_timeouts()
            self.assertEqual(bucket, [])
            self.assertEqual(timer.get_remaining_interval(), 5)

            # Cancel t1 and t2 that haven't expired yet
            timer.remove_timeout(t1)
            self.assertIsNone(t1.callback)
            self.assertEqual(timer._num_cancellations, 1)
            timer.remove_timeout(t2)
            self.assertIsNone(t2.callback)
            self.assertEqual(timer._num_cancellations, 2)
            self.assertEqual(timer.get_remaining_interval(), 5)
            timer.process_timeouts()
            self.assertEqual(bucket, [])
            self.assertEqual(timer._num_cancellations, 2)
            self.assertEqual(timer.get_remaining_interval(), 5)
            self.assertEqual(len(timer._timeout_heap), 3)

        # Advance time by 6 seconds to expire t1 and t2 and verify they don't
        # fire
        with mock.patch('time.time', return_value=now + 6):
            self.assertEqual(timer.get_remaining_interval(), 0)
            timer.process_timeouts()
            self.assertEqual(bucket, [])
            self.assertEqual(timer._num_cancellations, 0)
            self.assertEqual(len(timer._timeout_heap), 1)
            self.assertEqual(timer.get_remaining_interval(), 4)

        # Advance time by 10 seconds to expire t3 and verify it fires
        with mock.patch('time.time', return_value=now + 10):
            self.assertEqual(timer.get_remaining_interval(), 0)
            timer.process_timeouts()
            self.assertEqual(bucket, [3])
            self.assertEqual(len(timer._timeout_heap), 0)
            self.assertIsNone(timer.get_remaining_interval())

    def test_gc_of_unexpired_timeouts(self):
        now = time.time()
        bucket = []
        timer = select_connection._Timer()

        with mock.patch.multiple(select_connection._Timer,
                                 _GC_CANCELLATION_THRESHOLD=1):
            with mock.patch('time.time', return_value=now):
                t3 = timer.call_later(10, lambda: bucket.append(3))
                t2 = timer.call_later(6, lambda: bucket.append(2))
                t1 = timer.call_later(5, lambda: bucket.append(1))

                # Cancel t1 and check that it doesn't trigger GC because it's
                # not greater than half the timeouts
                timer.remove_timeout(t1)
                self.assertEqual(timer._num_cancellations, 1)
                timer.process_timeouts()
                self.assertEqual(timer._num_cancellations, 1)
                self.assertEqual(bucket, [])
                self.assertEqual(len(timer._timeout_heap), 3)
                self.assertEqual(timer.get_remaining_interval(), 5)

                # Cancel t3 and verify GC since it's now greater than half of
                # total timeouts
                timer.remove_timeout(t3)
                self.assertEqual(timer._num_cancellations, 2)
                timer.process_timeouts()
                self.assertEqual(bucket, [])
                self.assertEqual(len(timer._timeout_heap), 1)
                self.assertIs(t2, timer._timeout_heap[0])
                self.assertEqual(timer.get_remaining_interval(), 6)
                self.assertEqual(timer._num_cancellations, 0)

    def test_add_timeout_from_another_timeout(self):
        now = time.time()
        bucket = []
        timer = select_connection._Timer()

        with mock.patch('time.time', return_value=now):
            t1 = timer.call_later(
                5,
                lambda: bucket.append(
                    timer.call_later(0, lambda: bucket.append(2))))

        # Advance time by 10 seconds and verify that t1 fires and creates t2,
        # but timer manager defers firing of t2 to next `process_timeouts` in
        # order to avoid IO starvation
        with mock.patch('time.time', return_value=now + 10):
            timer.process_timeouts()
            t2 = bucket.pop()
            self.assertIsInstance(t2, select_connection._Timeout)
            self.assertIsNot(t2, t1)
            self.assertEqual(bucket, [])
            self.assertEqual(len(timer._timeout_heap), 1)
            self.assertIs(t2, timer._timeout_heap[0])
            self.assertEqual(timer.get_remaining_interval(), 0)

            # t2 should now fire
            timer.process_timeouts()
            self.assertEqual(bucket, [2])
            self.assertEqual(timer.get_remaining_interval(), None)

    def test_cancel_unexpired_timeout_from_another_timeout(self):
        now = time.time()
        bucket = []
        timer = select_connection._Timer()

        with mock.patch('time.time', return_value=now):
            t2 = timer.call_later(10, lambda: bucket.append(2))
            t1 = timer.call_later(5, lambda: timer.remove_timeout(t2))

            self.assertIs(t1, timer._timeout_heap[0])

        # Advance time by 6 seconds and check that t2 is cancelled, but not
        # removed from timeout heap
        with mock.patch('time.time', return_value=now + 6):
            timer.process_timeouts()
            self.assertIsNone(t2.callback)
            self.assertEqual(timer.get_remaining_interval(), 4)
            self.assertIs(t2, timer._timeout_heap[0])
            self.assertEqual(timer._num_cancellations, 1)

        # Advance time by 10 seconds and verify that t2 is removed without
        # firing
        with mock.patch('time.time', return_value=now + 10):
            timer.process_timeouts()
            self.assertEqual(bucket, [])
            self.assertIsNone(timer.get_remaining_interval())
            self.assertEqual(len(timer._timeout_heap), 0)
            self.assertEqual(timer._num_cancellations, 0)


    def test_cancel_expired_timeout_from_another_timeout(self):
        now = time.time()
        bucket = []
        timer = select_connection._Timer()

        with mock.patch('time.time', return_value=now):
            t2 = timer.call_later(10, lambda: bucket.append(2))
            t1 = timer.call_later(
                5,
                lambda: (self.assertEqual(timer._num_cancellations, 0),
                         timer.remove_timeout(t2)))

            self.assertIs(t1, timer._timeout_heap[0])

        # Advance time by 10 seconds and check that t2 is cancelled and
        # removed from timeout heap
        with mock.patch('time.time', return_value=now + 10):
            timer.process_timeouts()
            self.assertEqual(bucket, [])
            self.assertIsNone(t2.callback)
            self.assertIsNone(timer.get_remaining_interval())
            self.assertEqual(len(timer._timeout_heap), 0)
            self.assertEqual(timer._num_cancellations, 0)
