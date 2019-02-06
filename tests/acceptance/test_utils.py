"""Acceptance test utils"""

import functools
import logging
import time
import traceback
import pika.compat


def retry_assertion(timeout_sec, retry_interval_sec=0.1):
    """Creates a decorator that retries the decorated function or
    method only upon `AssertionError` exception at the given retry interval
    not to exceed the overall given timeout.

    :param float timeout_sec: overall timeout in seconds
    :param float retry_interval_sec: amount of time to sleep
        between retries in seconds.

    :returns: decorator that implements the following behavior

    1. This decorator guarantees to call the decorated function or method at
    least once.
    2. It passes through all exceptions besides `AssertionError`, preserving the
    original exception and its traceback.
    3. If no exception, it returns the return value from the decorated function/method.
    4. It sleeps `time.sleep(retry_interval_sec)` between retries.
    5. It checks for expiry of the overall timeout before sleeping.
    6. If the overall timeout is exceeded, it re-raises the latest `AssertionError`,
    preserving its original traceback
    """

    def retry_assertion_decorator(func):
        """Decorator"""

        @functools.wraps(func)
        def retry_assertion_wrap(*args, **kwargs):
            """The wrapper"""

            num_attempts = 0
            start_time = pika.compat.time_now()

            while True:
                num_attempts += 1

                try:
                    result = func(*args, **kwargs)
                except AssertionError:

                    now = pika.compat.time_now()
                    # Compensate for time adjustment
                    if now < start_time:
                        start_time = now

                    if (now - start_time) > timeout_sec:
                        logging.exception(
                            'Exceeded retry timeout of %s sec in %s attempts '
                            'with func %r. Caller\'s stack:\n%s',
                            timeout_sec, num_attempts, func,
                            ''.join(traceback.format_stack()))
                        raise

                    logging.debug('Attempt %s failed; retrying %r in %s sec.',
                                  num_attempts, func, retry_interval_sec)

                    time.sleep(retry_interval_sec)
                else:
                    logging.debug('%r succeeded at attempt %s',
                                  func, num_attempts)
                    return result

        return retry_assertion_wrap

    return retry_assertion_decorator

