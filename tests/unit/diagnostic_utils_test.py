"""
Test of `pika.diagnostic_utils`
"""

import unittest

import logging

import pika.compat
from pika import diagnostic_utils


# Suppress invalid-name, since our test names are descriptive and quite long
# pylint: disable=C0103

# Suppress missing-docstring to allow test method names to be printed by our
# test runner
# pylint: disable=C0111


class DiagnosticUtilsTest(unittest.TestCase):

    def test_args_and_return_value_propagation(self):

        bucket = []

        log_exception = diagnostic_utils.create_log_exception_decorator(
            logging.getLogger(__name__))

        return_value = (1, 2, 3)

        @log_exception
        def my_func(*args, **kwargs):
            bucket.append((args, kwargs))
            return return_value

        # Test with args and kwargs
        expected_args = ('a', 2, 'B', Exception('oh-oh'))
        expected_kwargs = dict(hello='world', bye='hello', error=RuntimeError())

        result = my_func(*expected_args, **expected_kwargs)

        self.assertIs(result, return_value)
        self.assertEqual(bucket, [(expected_args, expected_kwargs)])

        # Make sure that the original instances were passed through, not copies
        for i in pika.compat.xrange(len(expected_args)):
            self.assertIs(bucket[0][0][i], expected_args[i])

        for key in pika.compat.dictkeys(expected_kwargs):
            self.assertIs(bucket[0][1][key], expected_kwargs[key])

        # Now, repeat without any args/kwargs
        expected_args = tuple()
        expected_kwargs = dict()
        del bucket[:]  # .clear() doesn't exist in python 2.7

        result = my_func()

        self.assertIs(result, return_value)
        self.assertEqual(bucket, [(expected_args, expected_kwargs)])

    def test_exception_propagation(self):
        logger = logging.getLogger(__name__)
        log_exception = diagnostic_utils.create_log_exception_decorator(logger)

        # Suppress log output and capture LogRecord
        log_record_bucket = []
        logger.handle = log_record_bucket.append

        exception = Exception('Oops!')

        @log_exception
        def my_func_that_raises():
            raise exception

        with self.assertRaises(Exception) as ctx:
            my_func_that_raises()

        # Make sure the expected exception was raised
        self.assertIs(ctx.exception, exception)

        # Check log message
        self.assertEqual(len(log_record_bucket), 1)
        log_record = log_record_bucket[0]  # type: logging.LogRecord
        print(log_record.getMessage())
        expected_ending = 'Exception: Oops!\n'
        self.assertEqual(log_record.getMessage()[-len(expected_ending):],
                         expected_ending)
