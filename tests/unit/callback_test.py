# ***** BEGIN LICENSE BLOCK *****
#
# For copyright and licensing please refer to COPYING.
#
# ***** END LICENSE BLOCK *****


"""
Callback Unit Test
"""
from mock import Mock
import nose
import os
import sys
sys.path.append('..')
sys.path.append(os.path.join('..', '..'))

import pika.callback as callback

def teardown():
    cm = callback.CallbackManager.instance()
    if 'a' in cm._callbacks:
        del cm._callbacks['a']


def test_callback_instance():
    cm1 = callback.CallbackManager.instance()
    cm2 = callback.CallbackManager.instance()
    if (not cm1 or not cm2) or cm1 != cm2:
        assert False, "CallbackManager.instance() failure."
    pass


@nose.with_setup(None, teardown)
def test_add_callback():

    def callback_test():
        assert False, "callback() called on add only test."

    cm = callback.CallbackManager.instance()
    cm.add('a', 'b', callback_test)

    if 'a' not in cm._callbacks:
        assert False, "Prefix not found"

    if 'a' in cm._callbacks and 'b' not in cm._callbacks['a']:
        assert False, "Prefix found but key is missing"

    if 'b' in cm._callbacks['a']:
        for item in cm._callbacks['a']['b']:
            if item['handle'] == callback_test:
                break
        else:
            assert False, "Prefix and key found but could not find callback"


@nose.with_setup(None, teardown)
def test_process_callback():
    global callback_processed
    callback_processed = False

    def callback_test():
        global callback_processed
        callback_processed = True
    cm = callback.CallbackManager.instance()
    cm.add('a', 'b', callback_test)
    cm.process('a', 'b', 'None')
    if not callback_processed:
        assert False, "Callback not processed"
    pass


@nose.with_setup(None, teardown)
def test_remove_callback():
    def callback_test():
        assert False, "callback() called on remove only test."
    cm = callback.CallbackManager.instance()
    cm.add('a', 'b', callback_test)
    cm.remove('a', 'b')
    if 'a' in cm._callbacks and 'b' in cm._callbacks['a']:
        assert False, "Prefix and key were not removed"
    pass

@nose.with_setup(None, teardown)
def test_singleton_instance():
    cm = callback.CallbackManager.instance()
    cm2 = callback.CallbackManager.instance()
    assert id(cm) == id(cm2), "%s != %s" % (cm.id, cm2.id)

class TestSanitize(object):
    def setUp(self):
        reload(callback)
        self.cm = callback.CallbackManager.instance()
        self.test_cls = Mock(spec=['NAME'])
        self.test_cls.NAME = 'attr of self'

    def tearDown(self):
        del(self.test_cls)
        del(self.cm)

    def test_sanitize_name_in_method(self):
        """
        Verify NAME is gotten from test_cls.method.NAME

        """
        # put a 'method' class in our test_cls
        # for the sanitize method to find.
        self.test_cls.method = Mock(spec=['NAME'])
        self.test_cls.method.NAME = 'attr of attr'
        result = self.cm.sanitize(self.test_cls)
        nose.tools.eq_(result, self.test_cls.method.NAME)

    def test_sanitize_name_in_self(self):
        """
        Verify NAME is gotten from test_cls.NAME

        """
        result = self.cm.sanitize(self.test_cls)
        nose.tools.eq_(result, self.test_cls.NAME)

    def test_sanitize_name_c(self):
        """
        Verify NAME is gotten from test_cls.__dict__['NAME']

        """
        delattr(self.test_cls, 'NAME')
        self.test_cls.__dict__['NAME'] = 'in __dict__'
        result = self.cm.sanitize(self.test_cls)
        nose.tools.eq_(result, self.test_cls.__dict__['NAME'])

class TestAdd(object):
    def setUp(self):
        reload(callback)
        self.callback = callback
        self.callback.log = Mock(spec=['warning', 'debug'])
        self.cm = self.callback.CallbackManager.instance()
        self.key = 1
        self.keyname = 'keyname'
        self.cm.sanitize = Mock(return_value=self.keyname)
        self.callable_thing = int
        self.prefix = 'prefoo'

    def tearDown(self):
        del(self.cm)

    def test_add_new_prefix(self):
        # run the test
        self.cm.add(self.prefix, self.key, self.callable_thing)

        # All of this should be true once add is run.
        expected_callbacks = [{'handle': self.callable_thing, 'one_shot': True}]
        nose.tools.eq_(self.cm._callbacks[self.prefix][self.keyname], expected_callbacks)
        assert isinstance(self.cm._callbacks, dict)
        nose.tools.eq_(self.cm._callbacks.has_key(self.prefix), True)
        nose.tools.eq_(self.cm._callbacks[self.prefix].has_key(self.keyname), True)
        assert isinstance(self.cm._callbacks[self.prefix][self.keyname], list)

    def test_add_duplicate(self):
        existing_callback = {'handle': self.callable_thing, 'one_shot': True}
        self.cm._callbacks[self.prefix] = {self.keyname: [existing_callback]}
        assert existing_callback in self.cm._callbacks[self.prefix][self.keyname]

        # run the test
        self.cm.add(self.prefix, self.key, self.callable_thing)

        nose.tools.eq_(self.cm._callbacks[self.prefix][self.keyname], [existing_callback])
        # All of this should be true once add is run.
        assert isinstance(self.cm._callbacks, dict)
        nose.tools.eq_(self.cm._callbacks.has_key(self.prefix), True)
        nose.tools.eq_(self.cm._callbacks[self.prefix].has_key(self.keyname), True)
        assert isinstance(self.cm._callbacks[self.prefix][self.keyname], list)
        assert self.callback.log.warning.called, "Log not called"

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
        self.cm.add(self.prefix, self.key, self.callable_thing, only_caller=only_caller)

        expected_callbacks = [{'only': '0nly_c@ll3r', 'one_shot': True, 'handle': self.callable_thing}]
        nose.tools.eq_(self.cm._callbacks[self.prefix][self.keyname], expected_callbacks)
        # All of this should be true once add is run.
        assert isinstance(self.cm._callbacks, dict)
        nose.tools.eq_(self.cm._callbacks.has_key(self.prefix), True)
        nose.tools.eq_(self.cm._callbacks[self.prefix].has_key(self.keyname), True)
        assert isinstance(self.cm._callbacks[self.prefix][self.keyname], list)








if __name__ == "__main__":
    nose.runmodule()
