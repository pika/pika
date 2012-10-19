"""
Non-module specific functions shared by modules in the pika package

"""
try:
    from collections import Callable
except ImportError:  #pragma: no cover
    Callable = None


def is_callable(handle):
    """Returns a bool value if the handle passed in is a callable
    method/function

    :param any handle: The object to check
    :rtype: bool

    """
    if Callable:
        return isinstance(handle, Callable)
    return hasattr(handle, '__call__')
