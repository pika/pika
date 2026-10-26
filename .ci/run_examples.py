"""
Run every example against a live broker, so one that stops working fails CI.

An example in `SERVES` has to keep running until interrupted and then stop cleanly on SIGINT, which
is what Ctrl-C sends; every other example has to exit 0 by itself. A traceback in the output fails
either kind.

The broker is the one `PIKA_CI_EXAMPLES=true .ci/linux/gha-setup.sh` starts: the TLS examples need
its 5671 listener, `tls_mutual_authentication_twisted.py` its SASL EXTERNAL, and the reset between
examples its management API.

Usage:
    python .ci/run_examples.py [name ...]
"""

from __future__ import annotations

import base64
import functools
import json
import os
import pathlib
import signal
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Callable

EXAMPLES_DIR = pathlib.Path(__file__).resolve().parent.parent / 'examples'

#: The examples that run until interrupted.
SERVES = frozenset({
    'async_as_callback.py',
    'asynchronous_consumer_example.py',
    'asynchronous_publisher_example.py',
    'asyncio_consumer_example.py',
    'basic_consumer_threaded.py',
    'blocking_consume.py',
    'blocking_consume_recover_multiple_hosts.py',
    'blocking_consume_recover_multiple_hosts_retry.py',
    'blocking_consume_tls.py',
    'consume.py',
    'consumer_queued.py',
    'consumer_simple.py',
    'long_running_publisher.py',
    'long_running_publisher_threaded.py',
    'tornado_consumer.py',
    'twisted_service.py',
})

FAILURE_MARKERS = ('Traceback (most recent call last)',
                   'Unhandled error in Deferred')

API = 'http://localhost:15672/api/'
AUTH = {'Authorization': 'Basic ' + base64.b64encode(b'guest:guest').decode()}


def run(argv: list[str],
        serves: bool,
        deadline: float = 60.0,
        settle: float = 5.0,
        grace: float = 15.0,
        connected: Callable[[], bool] | None = None,
        connect_wait: float = 15.0) -> tuple[str | None, str]:
    """
    Run one example and judge it.

    :param argv: The command that runs it.
    :param serves: Whether it runs until interrupted, rather than exiting 0 within *deadline*
        seconds.
    :param settle: How long a serving example has to keep running before it is sent SIGINT.
    :param grace: How long it then has to stop.
    :param connected: For a serving example, whether it has a connection open to the broker; a
        serving example that never connects fails rather than passing for staying alive.
    :param connect_wait: How long after *settle* to keep asking *connected*.
    :returns: Why it failed, or None when it passed, and its output.
    """
    reason = None
    # An ignored SIGINT is inherited, and `hatch run` starts its command with SIGINT ignored.
    preexec = _restore_sigint if os.name == 'posix' else None
    # A file rather than a pipe: a child blocked on a full pipe looks like a server waiting for work.
    with tempfile.TemporaryFile() as out:
        proc = subprocess.Popen(
            argv,
            env=dict(os.environ, PYTHONUNBUFFERED='1'),
            stdin=subprocess.DEVNULL,
            stdout=out,
            stderr=subprocess.STDOUT,
            preexec_fn=preexec)  # noqa: PLW1509 -- the harness has one thread
        try:
            if not serves:
                if not _exited(proc, deadline):
                    reason = f'still running after {deadline:g}s'
                elif proc.returncode:
                    reason = f'exited {proc.returncode}'
            elif _exited(proc, settle):
                reason = f'exited {proc.returncode} before it was interrupted'
            elif connected is not None and not _waited_for(
                    connected, proc, connect_wait):
                reason = 'never connected to the broker'
            else:
                proc.send_signal(signal.SIGINT)
                if not _exited(proc, grace):
                    reason = f'still running {grace:g}s after SIGINT'
                # twistd stops cleanly and then dies by the signal; an uncaught KeyboardInterrupt
                # dies the same way, but prints a traceback.
                elif proc.returncode not in (0, -signal.SIGINT):
                    reason = f'exited {proc.returncode} after SIGINT'
        finally:
            if proc.poll() is None:
                proc.kill()
                proc.wait()
        out.seek(0)
        output = out.read().decode('utf-8', errors='replace')
    for marker in FAILURE_MARKERS:
        if reason is None and marker in output:
            reason = f'printed {marker!r}'
    return reason, output


def _restore_sigint() -> None:
    signal.signal(signal.SIGINT, signal.SIG_DFL)


def _waited_for(connected: Callable[[], bool], proc: subprocess.Popen[bytes],
                timeout: float) -> bool:
    end = time.monotonic() + timeout
    while not connected():
        if time.monotonic() >= end or proc.poll() is not None:
            return False
        time.sleep(0.5)
    return True


def _exited(proc: subprocess.Popen[bytes], timeout: float) -> bool:
    try:
        proc.wait(timeout)
    except subprocess.TimeoutExpired:
        return False
    return True


def _command(path: pathlib.Path) -> list[str]:
    if path.name == 'twisted_service.py':
        # A twistd application; this interpreter's twistd, so that it imports the same pika.
        return [
            sys.executable, '-c',
            'from twisted.scripts.twistd import run; run()', '-n', '--pidfile=',
            '-y',
            str(path)
        ]
    return [sys.executable, str(path)]


def _reset(attempts: int = 10) -> None:
    # The last example's connection can still be closing, and until it is gone its exclusive queues
    # refuse a delete (400) or vanish mid-reset (404); a fresh listing no longer has them.
    for attempt in range(attempts):
        try:
            _delete_all()
            return
        except urllib.error.HTTPError:
            if attempt == attempts - 1:
                raise
            time.sleep(0.5)


def _delete_all() -> None:
    # Examples declare the same names with different arguments, `standard` among them.
    for kind in ('queues', 'exchanges'):
        for item in _api('GET', f'{kind}/%2F'):
            name = item['name']
            if kind == 'exchanges' and (not name or name.startswith('amq.')):
                continue
            _api('DELETE', f'{kind}/%2F/{urllib.parse.quote(name, safe="")}')


def _api(method: str, path: str) -> Any:
    request = urllib.request.Request(API + path, method=method, headers=AUTH)
    with urllib.request.urlopen(request, timeout=10) as response:
        return json.loads(response.read() or b'null')


def _connected_since(start: float) -> bool:
    # The management API can still list an earlier example's connection after it has closed.
    return any(
        item.get('connected_at', 0) / 1000 >= start
        for item in _api('GET', 'connections'))


def main(names: list[str]) -> int:
    """
    Run the examples and report each one.

    :param names: The example file names to run; empty runs all of them.
    :returns: Process exit status: 0 when every example passed, else 1.
    """
    paths = ([EXAMPLES_DIR / name for name in names] or
             sorted(EXAMPLES_DIR.glob('*.py')))
    status = 0
    for path in paths:
        _reset()
        start = time.monotonic()
        started_at = time.time() - 1
        reason, output = run(_command(path),
                             path.name in SERVES,
                             connected=functools.partial(
                                 _connected_since, started_at))
        if reason is None:
            print(f'PASS {path.name} ({time.monotonic() - start:.1f}s)',
                  flush=True)
            if os.environ.get('GITHUB_ACTIONS') == 'true':
                # Collapsed in the log, so a pass can still be audited.
                print(
                    f'::group::{path.name} output\n{output.rstrip()}\n::endgroup::',
                    flush=True)
        else:
            tail = '\n'.join(
                f'    {line}' for line in output.splitlines()[-100:])
            print(f'FAIL {path.name}: {reason}\n{tail}', flush=True)
            status = 1
    return status


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
