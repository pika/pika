#!/usr/bin/env python
"""
Callback Unit Test
"""
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


if __name__ == "__main__":
    nose.runmodule()
