# -*- coding: utf8 -*-
"""
Tests for pika.callback

"""
import logging
import mock
try:
    import unittest2 as unittest
except ImportError:
    import unittest

from pika import amqp_object
from pika import callback
from pika import frame
from pika import spec


class CallbackTests(unittest.TestCase):

    KEY = 'Test Key'
    ARGUMENTS = callback.CallbackManager.ARGUMENTS
    CALLS = callback.CallbackManager.CALLS
    CALLBACK = callback.CallbackManager.CALLBACK
    ONE_SHOT = callback.CallbackManager.ONE_SHOT
    ONLY_CALLER = callback.CallbackManager.ONLY_CALLER
    PREFIX_CLASS = spec.Basic.Consume
    PREFIX = 'Basic.Consume'
    ARGUMENTS_VALUE = {'foo': 'bar'}

    @property
    def _callback_dict(self):
        return {self.CALLBACK: self.callback_mock,
                self.ONE_SHOT: True,
                self.ONLY_CALLER: self.mock_caller,
                self.ARGUMENTS: self.ARGUMENTS_VALUE,
                self.CALLS: 1}

    def setUp(self):
        self.obj = callback.CallbackManager()
        self.callback_mock = mock.Mock()
        self.mock_caller = mock.Mock()

    def tearDown(self):
        del self.obj
        del self.callback_mock
        del self.mock_caller

    def test_initialization(self):
        obj = callback.CallbackManager()
        self.assertDictEqual(obj._stack, {})


    def test_name_or_value_method_object(self):
        value = spec.Basic.Consume()
        self.assertEqual(callback._name_or_value(value), self.PREFIX)

    def test_name_or_value_basic_consume_object(self):
        self.assertEqual(callback._name_or_value(spec.Basic.Consume()),
                         self.PREFIX)

    def test_name_or_value_amqpobject_class(self):
        self.assertEqual(callback._name_or_value(self.PREFIX_CLASS),
                         self.PREFIX)

    def test_name_or_value_protocol_header(self):
        self.assertEqual(callback._name_or_value(frame.ProtocolHeader()),
                         'ProtocolHeader')

    def test_name_or_value_method_frame(self):
        value = frame.Method(1, self.PREFIX_CLASS())
        self.assertEqual(callback._name_or_value(value), self.PREFIX)

    def test_name_or_value_str(self):
        value = 'Test String Value'
        expectation = value
        self.assertEqual(callback._name_or_value(value), expectation)

    def test_name_or_value_unicode(self):
        value = u'Это тест значения'
        expectation = ('\xd0\xad\xd1\x82\xd0\xbe \xd1\x82\xd0\xb5\xd1\x81\xd1'
                       '\x82 \xd0\xb7\xd0\xbd\xd0\xb0\xd1\x87\xd0\xb5\xd0\xbd'
                       '\xd0\xb8\xd1\x8f')
        self.assertEqual(callback._name_or_value(value), expectation)

    def test_empty_callbacks_on_init(self):
        self.assertFalse(self.obj._stack)

    def test_sanitize_decorator_with_args_only(self):
        self.obj.add(self.PREFIX_CLASS, self.KEY, None)
        self.assertIn(self.PREFIX, self.obj._stack.keys())

    def test_sanitize_decorator_with_kwargs(self):
        self.obj.add(prefix=self.PREFIX_CLASS, key=self.KEY, callback=None)
        self.assertIn(self.PREFIX, self.obj._stack.keys())

    def test_sanitize_decorator_with_mixed_args_and_kwargs(self):
        self.obj.add(self.PREFIX_CLASS, key=self.KEY, callback=None)
        self.assertIn(self.PREFIX, self.obj._stack.keys())

    def test_add_first_time_prefix_added(self):
        self.obj.add(self.PREFIX, self.KEY, None)
        self.assertIn(self.PREFIX, self.obj._stack)

    def test_add_first_time_key_added(self):
        self.obj.add(self.PREFIX, self.KEY, None)
        self.assertIn(self.KEY, self.obj._stack[self.PREFIX])

    def test_add_first_time_callback_added(self):
        self.obj.add(self.PREFIX, self.KEY, self.callback_mock)
        self.assertEqual(self.callback_mock,
                         self.obj._stack[self.PREFIX][self.KEY][0][self.CALLBACK])

    def test_add_oneshot_default_is_true(self):
        self.obj.add(self.PREFIX, self.KEY, None)
        self.assertTrue(self.obj._stack[self.PREFIX][self.KEY][0][self.ONE_SHOT])

    def test_add_oneshot_is_false(self):
        self.obj.add(self.PREFIX, self.KEY, None, False)
        self.assertFalse(self.obj._stack[self.PREFIX][self.KEY][0][self.ONE_SHOT])

    def test_add_only_caller_default_is_false(self):
        self.obj.add(self.PREFIX, self.KEY, None)
        self.assertFalse(self.obj._stack[self.PREFIX][self.KEY][0][self.ONLY_CALLER])

    def test_add_only_caller_true(self):
        self.obj.add(self.PREFIX, self.KEY, None, only_caller=True)
        self.assertTrue(self.obj._stack[self.PREFIX][self.KEY][0][self.ONLY_CALLER])

    def test_add_returns_prefix_value_and_key(self):
        self.assertEqual(self.obj.add(self.PREFIX, self.KEY, None),
                         (self.PREFIX, self.KEY))

    def test_add_duplicate_callback(self):
        mock_callback = mock.Mock()

        def add_callback():
            self.obj.add(self.PREFIX, self.KEY, mock_callback, False)


        with mock.patch('pika.callback.LOGGER', spec=logging.Logger) as logger:
            logger.warning = mock.Mock()
            add_callback()
            add_callback()
            DUPLICATE_WARNING = callback.CallbackManager.DUPLICATE_WARNING
            logger.warning.assert_called_once_with(DUPLICATE_WARNING,
                                                   self.PREFIX, self.KEY)

    def test_add_duplicate_callback_returns_prefix_value_and_key(self):
        self.obj.add(self.PREFIX, self.KEY, None)
        self.assertEqual(self.obj.add(self.PREFIX, self.KEY, None),
                         (self.PREFIX, self.KEY))

    def test_clear(self):
        self.obj.add(self.PREFIX, self.KEY, None)
        self.obj.clear()
        self.assertDictEqual(self.obj._stack, dict())

    def test_cleanup_removes_prefix(self):
        OTHER_PREFIX = 'Foo'
        self.obj.add(self.PREFIX, self.KEY, None)
        self.obj.add(OTHER_PREFIX, 'Bar', None)
        self.obj.cleanup(self.PREFIX)
        self.assertNotIn(self.PREFIX, self.obj._stack)

    def test_cleanup_keeps_other_prefix(self):
        OTHER_PREFIX = 'Foo'
        self.obj.add(self.PREFIX, self.KEY, None)
        self.obj.add(OTHER_PREFIX, 'Bar', None)
        self.obj.cleanup(self.PREFIX)
        self.assertIn(OTHER_PREFIX, self.obj._stack)

    def test_cleanup_returns_true(self):
        self.obj.add(self.PREFIX, self.KEY, None)
        self.assertTrue(self.obj.cleanup(self.PREFIX))

    def test_missing_prefix(self):
        self.assertFalse(self.obj.cleanup(self.PREFIX))

    def test_pending_none(self):
        self.assertIsNone(self.obj.pending(self.PREFIX_CLASS, self.KEY))

    def test_pending_one(self):
        self.obj.add(self.PREFIX, self.KEY, None)
        self.assertEqual(self.obj.pending(self.PREFIX_CLASS, self.KEY), 1)

    def test_pending_two(self):
        self.obj.add(self.PREFIX, self.KEY, None)
        self.obj.add(self.PREFIX, self.KEY, lambda x: True)
        self.assertEqual(self.obj.pending(self.PREFIX_CLASS, self.KEY), 2)

    def test_process_callback_false(self):
        self.obj._stack = dict()
        self.assertFalse(self.obj.process('FAIL', 'False', 'Empty',
                                          self.mock_caller, []))

    def test_process_false(self):
        self.assertFalse(self.obj.process(self.PREFIX_CLASS, self.KEY, self))

    def test_process_true(self):
        self.obj.add(self.PREFIX, self.KEY, self.callback_mock)
        self.assertTrue(self.obj.process(self.PREFIX_CLASS, self.KEY, self))

    def test_process_mock_called(self):
        args = (1, None, 'Hi')
        self.obj.add(self.PREFIX, self.KEY, self.callback_mock)
        self.obj.process(self.PREFIX, self.KEY, self, args)
        self.callback_mock.assert_called_once_with(args)

    def test_process_one_shot_removed(self):
        args = (1, None, 'Hi')
        self.obj.add(self.PREFIX, self.KEY, self.callback_mock)
        self.obj.process(self.PREFIX, self.KEY, self, args)
        self.assertNotIn(self.PREFIX, self.obj._stack)

    def test_process_non_one_shot_prefix_not_removed(self):
        self.obj.add(self.PREFIX, self.KEY, self.callback_mock, one_shot=False)
        self.obj.process(self.PREFIX, self.KEY, self)
        self.assertIn(self.PREFIX, self.obj._stack)

    def test_process_non_one_shot_key_not_removed(self):
        self.obj.add(self.PREFIX, self.KEY, self.callback_mock, one_shot=False)
        self.obj.process(self.PREFIX, self.KEY, self)
        self.assertIn(self.KEY, self.obj._stack[self.PREFIX])

    def test_process_non_one_shot_callback_not_removed(self):
        self.obj.add(self.PREFIX, self.KEY, self.callback_mock, one_shot=False)
        self.obj.process(self.PREFIX, self.KEY, self)
        self.assertEqual(self.obj._stack[self.PREFIX][self.KEY][0][self.CALLBACK],
                         self.callback_mock)

    def test_process_only_caller_fails(self):
        self.obj.add(self.PREFIX_CLASS, self.KEY, self.callback_mock,
                only_caller=self.mock_caller)
        self.obj.process(self.PREFIX_CLASS, self.KEY, self)
        self.assertFalse(self.callback_mock.called)

    def test_process_only_caller_fails_no_removal(self):
        self.obj.add(self.PREFIX_CLASS, self.KEY, self.callback_mock,
                     only_caller=self.mock_caller)
        self.obj.process(self.PREFIX_CLASS, self.KEY, self)
        self.assertEqual(self.obj._stack[self.PREFIX][self.KEY][0][self.CALLBACK],
                         self.callback_mock)

    def test_remove_with_no_callbacks_pending(self):
        self.obj = callback.CallbackManager()
        self.assertFalse(self.obj.remove(self.PREFIX, self.KEY,
                                         self.callback_mock))

    def test_remove_with_callback_true(self):
        self.obj.add(self.PREFIX_CLASS, self.KEY, self.callback_mock)
        self.assertTrue(self.obj.remove(self.PREFIX, self.KEY,
                                        self.callback_mock))

    def test_remove_with_callback_false(self):
        self.obj.add(self.PREFIX_CLASS, self.KEY, None)
        self.assertTrue(self.obj.remove(self.PREFIX, self.KEY,
                                        self.callback_mock))

    def test_remove_with_callback_true_empty_stack(self):
        self.obj.add(self.PREFIX_CLASS, self.KEY, self.callback_mock)
        self.obj.remove(prefix=self.PREFIX, key=self.KEY,
                        callback_value=self.callback_mock)
        self.assertDictEqual(self.obj._stack, dict())

    def test_remove_with_callback_true_non_empty_stack(self):
        self.obj.add(self.PREFIX_CLASS, self.KEY, self.callback_mock)
        self.obj.add(self.PREFIX_CLASS, self.KEY, self.mock_caller)
        self.obj.remove(self.PREFIX, self.KEY, self.callback_mock)
        self.assertEqual(self.mock_caller,
                         self.obj._stack[self.PREFIX][self.KEY][0][self.CALLBACK])

    def test_remove_prefix_key_with_other_key_prefix_remains(self):
        OTHER_KEY = 'Other Key'
        self.obj.add(self.PREFIX_CLASS, self.KEY, self.callback_mock)
        self.obj.add(self.PREFIX_CLASS, OTHER_KEY, self.mock_caller)
        self.obj.remove(self.PREFIX, self.KEY, self.callback_mock)
        self.assertIn(self.PREFIX, self.obj._stack)

    def test_remove_prefix_key_with_other_key_remains(self):
        OTHER_KEY = 'Other Key'
        self.obj.add(self.PREFIX_CLASS, self.KEY, self.callback_mock)
        self.obj.add(prefix=self.PREFIX_CLASS, key=OTHER_KEY,
                     callback=self.mock_caller)
        self.obj.remove(self.PREFIX, self.KEY)
        self.assertIn(OTHER_KEY, self.obj._stack[self.PREFIX])

    def test_remove_prefix_key_with_other_key_callback_remains(self):
        OTHER_KEY = 'Other Key'
        self.obj.add(self.PREFIX_CLASS, self.KEY, self.callback_mock)
        self.obj.add(self.PREFIX_CLASS, OTHER_KEY, self.mock_caller)
        self.obj.remove(self.PREFIX, self.KEY)
        self.assertEqual(self.mock_caller,
                         self.obj._stack[self.PREFIX][OTHER_KEY][0][self.CALLBACK])

    def test_remove_all(self):
        self.obj.add(self.PREFIX_CLASS, self.KEY, self.callback_mock)
        self.obj.remove_all(self.PREFIX, self.KEY)
        self.assertNotIn(self.PREFIX, self.obj._stack)

    def test_should_process_callback_true(self):
        self.obj.add(self.PREFIX_CLASS, self.KEY, self.callback_mock)
        value = self.obj._callback_dict(self.callback_mock, False, None, None)
        self.assertTrue(self.obj._should_process_callback(value,
                                                          self.mock_caller, []))

    def test_should_process_callback_false_argument_fail(self):
        self.obj.clear()
        self.obj.add(self.PREFIX_CLASS, self.KEY, self.callback_mock,
                     arguments={'foo': 'baz'})
        self.assertFalse(self.obj._should_process_callback(self._callback_dict,
                                                           self.mock_caller,
                                                           [{'foo': 'baz'}]))

    def test_should_process_callback_false_only_caller_failure(self):
        self.obj.add(self.PREFIX_CLASS, self.KEY, self.callback_mock)
        value = self.obj._callback_dict(self.callback_mock, False, self, None)
        self.assertTrue(self.obj._should_process_callback(value,
                                                          self.mock_caller, []))

    def test_should_process_callback_false_only_caller_failure(self):
        self.obj.add(self.PREFIX_CLASS, self.KEY, self.callback_mock)
        value = self.obj._callback_dict(self.callback_mock, False,
                                        self.mock_caller, None)
        self.assertTrue(self.obj._should_process_callback(value,
                                                          self.mock_caller, []))

    def test_dict(self):
        self.assertDictEqual(self.obj._callback_dict(self.callback_mock,
                                                     True, self.mock_caller,
                                                     self.ARGUMENTS_VALUE),
                             self._callback_dict)

    def test_arguments_match_no_arguments(self):
        self.assertFalse(self.obj._arguments_match(self._callback_dict, []))

    def test_arguments_match_dict_argument(self):
        self.assertTrue(self.obj._arguments_match(self._callback_dict,
                                                  [self.ARGUMENTS_VALUE]))

    def test_arguments_match_dict_argument_no_attribute(self):
        self.assertFalse(self.obj._arguments_match(self._callback_dict,
                                                   [{}]))

    def test_arguments_match_dict_argument_no_match(self):
        self.assertFalse(self.obj._arguments_match(self._callback_dict,
                                                   [{'foo': 'baz'}]))

    def test_arguments_match_obj_argument(self):
        class TestObj(object):
            foo = 'bar'
        test_instance = TestObj()
        self.assertTrue(self.obj._arguments_match(self._callback_dict,
                                                  [test_instance]))

    def test_arguments_match_obj_no_attribute(self):
        class TestObj(object):
            qux = 'bar'
        test_instance = TestObj()
        self.assertFalse(self.obj._arguments_match(self._callback_dict,
                                                  [test_instance]))

    def test_arguments_match_obj_argument_no_match(self):
        class TestObj(object):
            foo = 'baz'
        test_instance = TestObj()
        self.assertFalse(self.obj._arguments_match(self._callback_dict,
                                                   [test_instance]))

    def test_arguments_match_obj_argument_with_method(self):
        class TestFrame(object):
            method = None
        class MethodObj(object):
            foo = 'bar'
        test_instance = TestFrame()
        test_instance.method = MethodObj()
        self.assertTrue(self.obj._arguments_match(self._callback_dict,
                                                  [test_instance]))

    def test_arguments_match_obj_argument_with_method_no_match(self):
        class TestFrame(object):
            method = None
        class MethodObj(object):
            foo = 'baz'
        test_instance = TestFrame()
        test_instance.method = MethodObj()
        self.assertFalse(self.obj._arguments_match(self._callback_dict,
                                                   [test_instance]))
