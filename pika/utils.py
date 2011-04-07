# ***** BEGIN LICENSE BLOCK *****
#
# For copyright and licensing please refer to COPYING.
#
# ***** END LICENSE BLOCK *****

__author__ = 'Gavin M. Roy'
__email__ = 'gmr@myyearbook.com'
__date__ = '2011-04-06'

try:
    from collections import Callable
    CHECK_INSTANCE = True
except ImportError:
    CHECK_INSTANCE = False


def is_callable(handle):
    """
    Returns a bool value if the handle passed in is a callable method/function
    """
    if CHECK_INSTANCE:
        return isinstance(handle, Callable)

    return hasattr(handle, '__call__')
