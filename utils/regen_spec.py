"""
Regenerate ``pika/spec.py`` from the upstream AMQP 0-9-1 code generator, or check that the committed
file still matches.

``utils/codegen.py`` needs two files from ``rabbitmq/rabbitmq-server``:
``deps/rabbitmq_codegen/amqp_codegen.py`` and
``deps/rabbitmq_codegen/amqp-rabbitmq-0.9.1.json``. Rather than cloning that
repository, this script downloads both into a temporary tree laid out the way
``codegen.py`` expects.

Because ``codegen.py`` hardcodes its output path as ``./pika/spec.py``, it runs
with its working directory set to that temporary tree, which keeps the generated
file out of the working copy until we decide what to do with it.

Usage:
    python utils/regen_spec.py            # regenerate pika/spec.py in place
    python utils/regen_spec.py --check    # diff only, exit 1 on mismatch
    python utils/regen_spec.py --ref REF  # generate from another upstream ref
"""

from __future__ import annotations

import argparse
import difflib
import shutil
import subprocess
import sys
import tempfile
import urllib.request
from pathlib import Path

# Deliberately a moving ref rather than a pinned SHA, for the same reason ruff
# is left unpinned: when upstream changes the AMQP spec, CI says so on the next
# push instead of leaving `pika/spec.py` quietly behind. Pass `--ref` to
# generate from some other revision, and see AGENTS.md for what to do when the
# check fails because upstream moved.
DEFAULT_REF = 'main'

RAW_BASE = 'https://raw.githubusercontent.com/rabbitmq/rabbitmq-server'

CODEGEN_FILES = ('amqp_codegen.py', 'amqp-rabbitmq-0.9.1.json')

REPO_ROOT = Path(__file__).resolve().parent.parent
CODEGEN = REPO_ROOT / 'utils' / 'codegen.py'
SPEC = REPO_ROOT / 'pika' / 'spec.py'

DOWNLOAD_TIMEOUT = 30


def fetch_codegen_inputs(dest: Path, ref: str) -> None:
    """
    Download the upstream code generator inputs into ``dest``.

    :param dest: Directory to populate, created if it does not exist.
    :param ref:``rabbitmq/rabbitmq-server`` branch, tag, or commit to fetch from.
    """
    dest.mkdir(parents=True, exist_ok=True)
    for name in CODEGEN_FILES:
        url = f'{RAW_BASE}/{ref}/deps/rabbitmq_codegen/{name}'
        print(f'fetching {url}', file=sys.stderr)
        with urllib.request.urlopen(url, timeout=DOWNLOAD_TIMEOUT) as response:
            (dest / name).write_bytes(response.read())


def generate(workdir: Path, ref: str) -> Path:
    """
    Run the code generator and yapf inside ``workdir`` and return the result.

    :param workdir: Temporary tree holding the upstream inputs.
    :param ref:``rabbitmq/rabbitmq-server`` branch, tag, or commit to fetch from.
    :returns: Path to the generated, formatted spec module.
    """
    fetch_codegen_inputs(workdir / 'deps' / 'rabbitmq_codegen', ref)
    generated = workdir / 'pika' / 'spec.py'
    generated.parent.mkdir(parents=True, exist_ok=True)

    subprocess.run([sys.executable, str(CODEGEN),
                    str(workdir)],
                   cwd=workdir,
                   check=True)
    # `fmt` excludes pika/spec.py, so yapf has to be run against it directly;
    # without this step the result differs from the committed file by line
    # wrapping alone.
    subprocess.run([
        sys.executable, '-m', 'yapf', '--in-place', '--style', 'google',
        str(generated)
    ],
                   check=True)
    return generated


def check(generated: Path, ref: str) -> int:
    """
    Diff the generated spec against the committed one.

    :param generated: Path to the freshly generated spec module.
    :param ref: Upstream ref the spec was generated from, named in the failure message.
    :returns: Process exit status, 0 when the two files match.
    """
    expected = generated.read_text(encoding='utf-8').splitlines(keepends=True)
    actual = SPEC.read_text(encoding='utf-8').splitlines(keepends=True)
    if expected == actual:
        print('pika/spec.py is up to date')
        return 0

    sys.stdout.writelines(
        difflib.unified_diff(actual,
                             expected,
                             fromfile='pika/spec.py (committed)',
                             tofile='pika/spec.py (regenerated)'))
    # Keep the diff above the instruction when the two streams are merged, as
    # they are in a CI log.
    sys.stdout.flush()
    print(
        f'\npika/spec.py does not match the output of utils/codegen.py against '
        f'rabbitmq-server {ref}.\nRun `hatch run spec-regen` and commit the '
        f'result. If the diff is unrelated to your change, upstream moved the '
        f'spec: see the auto-generated code section of AGENTS.md.',
        file=sys.stderr)
    return 1


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--check',
                        action='store_true',
                        help='diff instead of writing pika/spec.py')
    parser.add_argument('--ref',
                        default=DEFAULT_REF,
                        help='rabbitmq-server ref to generate from '
                        f'(default: {DEFAULT_REF})')
    args = parser.parse_args()

    with tempfile.TemporaryDirectory() as tmp:
        generated = generate(Path(tmp), args.ref)
        if args.check:
            return check(generated, args.ref)
        shutil.copyfile(generated, SPEC)
    print(f'wrote {SPEC.relative_to(REPO_ROOT)}')
    return 0


if __name__ == '__main__':
    sys.exit(main())
