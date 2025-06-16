"""
Common validation functions
"""
from typing import Any, Callable, Optional


def require_string(value: Any, value_name: str) -> None:
    """Require that value is a string

    :raises: TypeError

    """
    if not isinstance(value, str):
        raise TypeError('{} must be a str or unicode str, but got {!r}'.format(
            value_name,
            value,
        ))


def require_callback(callback: Callable[..., Any], callback_name: str = 'callback'):
    """Require that callback is callable and is not None

    :raises: TypeError

    """
    if not callable(callback):
        raise TypeError('callback {} must be callable, but got {!r}'.format(
            callback_name,
            callback,
        ))


def rpc_completion_callback(callback: Optional[Callable[..., Any]]) -> bool:
    """Verify callback is callable if not None

    :returns: boolean indicating nowait
    :rtype: bool
    :raises: TypeError

    """
    if callback is None:
        # No callback means we will not expect a response
        # i.e. nowait=True
        return True

    if callable(callback):
        # nowait=False
        return False
    else:
        raise TypeError('completion callback must be callable if not None')


def zero_or_greater(name: str, value: int) -> None:
    """Verify that value is zero or greater. If not, 'name'
    will be used in error message

    :raises: ValueError

    """
    if int(value) < 0:
        errmsg = f'{name} must be >= 0, but got {value}'
        raise ValueError(errmsg)
