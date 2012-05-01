# ***** BEGIN LICENSE BLOCK *****
#
# For copyright and licensing please refer to COPYING.
#
# ***** END LICENSE BLOCK *****


"""
Callback Unit Test
"""
from mock import Mock
import support
import unittest
import pika.callback as callback


class TestNameOrValue(unittest.TestCase):

    def setUp(self):
        self.cm = callback.CallbackManager()
        self.test_cls = Mock(spec=['NAME'])
        self.test_cls.NAME = 'attr of self'

    def test_sanitize_name_in_method(self):
        """
        Verify NAME is gotten from test_cls.method.NAME

        """
        # put a 'method' class in our test_cls
        # for the sanitize method to find.
        self.test_cls.method = Mock(spec=['NAME'])
        self.test_cls.method.NAME = 'attr of attr'
        result = callback._name_or_value(self.test_cls)
        self.assertEqual(result, self.test_cls.method.NAME)

    def test_sanitize_name_in_self(self):
        """
        Verify NAME is gotten from test_cls.NAME

        """
        result = callback._name_or_value(self.test_cls)
        self.assertEqual(result, self.test_cls.NAME)

    def test_sanitize_name_c(self):
        """
        Verify NAME is gotten from test_cls.__dict__['NAME']

        """
        delattr(self.test_cls, 'NAME')
        self.test_cls.__dict__['NAME'] = 'in __dict__'
        result = callback._name_or_value(self.test_cls)
        self.assertEqual(result, self.test_cls.__dict__['NAME'])


class TestAdd(unittest.TestCase):

    def setUp(self):
        self.callback = callback
        self.callback.log = Mock(spec=['warning', 'debug'])
        self.key = 1
        self.keyname = 'keyname'
        self.old_name_or_value = callback._name_or_value
        callback._name_or_value = Mock(return_value=self.keyname)
        self.cm = self.callback.CallbackManager()
        self.cm.sanitize = Mock(return_value=self.keyname)
        self.callable_thing = int
        self.prefix = 'prefoo'

    def tearDown(self):
        callback._name_or_value = self.old_name_or_value

    def test_add_new_prefix(self):
        # run the test
        self.cm.add(self.prefix, self.key, self.callable_thing)

        # All of this should be true once add is run.
        expected_callbacks = [{'handle': self.callable_thing,
                               'one_shot': True}]
        self.assertEqual(self.cm._callbacks[self.prefix][self.keyname],
                         expected_callbacks)
        self.assertTrue(isinstance(self.cm._callbacks, dict))
        self.assertTrue(self.prefix in self.cm._callbacks)
        self.assertTrue(self.keyname in self.cm._callbacks[self.prefix])
        temp = self.cm._callbacks[self.prefix][self.keyname]
        self.assertTrue(isinstance(temp, list))

    def test_add_duplicate(self):

        # Short circuit this until it checks for warnings.UserWarning instead
        # of logging.warning
        return

        existing_callback = {'handle': self.callable_thing, 'one_shot': True}
        self.cm._callbacks[self.prefix] = {self.keyname: [existing_callback]}
        temp = self.cm._callbacks[self.prefix][self.keyname]
        self.assertTrue(existing_callback in temp)

        # run the test
        self.cm.add(self.prefix, self.key, self.callable_thing)
        self.assertEqual(self.cm._callbacks[self.prefix][self.keyname],
                         [existing_callback])
        # All of this should be true once add is run.
        self.assertTrue(isinstance(self.cm._callbacks, dict))
        self.assertTrue(self.prefix in self.cm._callbacks)
        self.assertTrue(self.keyname in self.cm._callbacks[self.prefix])
        temp = self.cm._callbacks[self.prefix][self.keyname]
        self.assertTrue(isinstance(temp, list))
        self.assertTrue(self.callback.log.warning.called, "Log not called")

        # Inserting a duplicate callback emits a warning. This checks
        # that the log object (Mocked in setUp) was called.
        print self.callback.log.warning.call_args
        assert self.callback.log.warning.call_args == (
                            ('%s.add: Duplicate callback found for "%s:%s"',
                             'CallbackManager',
                             self.prefix,
                             self.keyname),
                            {})

    def test_only_caller(self):
        only_caller = '0nly_c@ll3r'

        # run the test
        self.cm.add(self.prefix, self.key, self.callable_thing,
                    only_caller=only_caller)

        expected_callbacks = [{'only': '0nly_c@ll3r', 'one_shot': True,
                               'handle': self.callable_thing}]
        self.assertEqual(self.cm._callbacks[self.prefix][self.keyname],
                         expected_callbacks)
        # All of this should be true once add is run.
        assert isinstance(self.cm._callbacks, dict)
        self.assertTrue(self.prefix in self.cm._callbacks)
        self.assertTrue(self.keyname in self.cm._callbacks[self.prefix])
        temp = self.cm._callbacks[self.prefix][self.keyname]
        self.assertTrue(isinstance(temp, list))


if __name__ == "__main__":
    unittest.main()
