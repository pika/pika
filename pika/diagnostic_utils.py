"""
Diagnostic utilities
"""

from __future__ import annotations

import functools
import sys
import traceback
from typing import Any, Callable, TypeVar, cast, TYPE_CHECKING

if TYPE_CHECKING:
    import logging  # pragma: no cover

# TypeVar for the function being decorated
F = TypeVar('F', bound=Callable[..., Any])


def create_log_exception_decorator(logger: logging.Logger) -> Callable[[F], F]:
    """Create a decorator that logs and reraises any exceptions that escape
    the decorated function

    :param logging.Logger logger:
    :returns: the decorator
    :rtype: callable

    Usage example

    import logging

    from pika.diagnostics_utils import create_log_exception_decorator

    _log_exception = create_log_exception_decorator(logging.getLogger(__name__))

    @_log_exception
    def my_func_or_method():
        raise Exception('Oops!')

    """

    def log_exception(func: F) -> F:
        """The decorator returned by the parent function

        :param func: function to be wrapped
        :returns: the function wrapper
        :rtype: callable
        """

        @functools.wraps(func)
        def log_exception_func_wrap(*args: Any, **kwargs: Any) -> Any:
            """The wrapper function returned by the decorator. Invokes the
            function with the given args/kwargs and returns the function's
            return value. If the function exits with an exception, logs the
            exception traceback and re-raises the

            :param args: positional args passed to wrapped function
            :param kwargs: keyword args passed to wrapped function
            :returns: whatever the wrapped function returns
            :rtype: object
            """
            try:
                return func(*args, **kwargs)
            except:
                logger.exception(
                    'Wrapped func exited with exception. Caller\'s stack:\n%s',
                    ''.join(traceback.format_exception(*sys.exc_info())))
                raise

        return cast(F, log_exception_func_wrap)

    return log_exception
