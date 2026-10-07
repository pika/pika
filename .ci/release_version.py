"""
Version arithmetic for the release workflow.

Computes the version a release publishes, from the version currently in `pyproject.toml` plus the
dispatch inputs. Lives here rather than in `release.yaml` for the same reason the alias decision
lives in `docs_site.py`: it is parsing and a state transition, the two things shell expresses worst,
and the version it produces becomes a git tag, a PyPI release and a documentation directory, none of
which can be taken back.

Expressed as shell it was both untestable and wrong. `${current%%[a-zA-Z]*}` strips a pre-release
suffix correctly, but every bump then incremented, so once `1.5.0a1` was published no input produced
`1.5.0`: `patch` gave `1.5.1`, `minor` gave `1.6.0`, `major` gave `2.0.0`. Promoting a pre-release
meant hand-editing `pyproject.toml` between two workflow runs. `packaging` is used rather than hand-
rolled string work because it already knows what `base_version` means.
"""

from __future__ import annotations

import argparse
import re
import sys

from packaging.version import InvalidVersion, Version

#: Canonical PEP 440 pre-release segments, spelled exactly. No leading zeros:
#: `b01` matches a looser pattern but normalizes to `b1`, so the git tag and the
#: version PyPI indexes would differ, and the docs deploy would reject the name
#: after both were already unrecoverable.
PRERELEASE_TAG = re.compile(r'^(a|b|rc)(0|[1-9][0-9]*)$')

#: Bumps that move the base version, and the one that does not. `none` exists
#: because a pre-release cycle needs the base version held twice: to progress
#: `1.5.0a1` to `1.5.0b1`, and to promote it to `1.5.0`.
BUMPS = ('major', 'minor', 'patch', 'none')


def base_of(current: str) -> tuple[int, int, int]:
    """
    Return *current* as a `(major, minor, patch)` triple, ignoring any suffix.

    `Version.base_version` drops a pre-release, post-release or development segment, so `1.5.0a1`
    and `1.5.0` both arrive here as `(1, 5, 0)`. That is what makes promoting a pre-release a matter
    of not incrementing, rather than of subtracting what the suffix added.

    :param current: The version as written in `pyproject.toml`.
    :returns: The release triple.
    :raises ValueError: if *current* is not a version, or is not `MAJOR.MINOR.PATCH`.
    """
    try:
        parsed = Version(current)
    except InvalidVersion as exc:
        raise ValueError(f'{current!r} is not a version: {exc}') from exc
    parts = parsed.base_version.split('.')
    if len(parts) != 3:
        raise ValueError(
            f'{current!r} has a base version of {parsed.base_version!r}, which '
            f'is not MAJOR.MINOR.PATCH; this scheme does not publish it')
    return int(parts[0]), int(parts[1]), int(parts[2])


def bump(current: str, how: str) -> str:
    """
    Return the base version *current* becomes under the bump *how*.

    :param current: The version as written in `pyproject.toml`.
    :param how: One of `BUMPS`.
    :returns: A `MAJOR.MINOR.PATCH` string with no suffix.
    :raises ValueError: if *how* is not a known bump, or *current* is unusable.
    """
    if how not in BUMPS:
        raise ValueError(f'{how!r} is not a bump; expected one of '
                         f'{", ".join(BUMPS)}')
    major, minor, patch = base_of(current)
    if how == 'major':
        major, minor, patch = major + 1, 0, 0
    elif how == 'minor':
        minor, patch = minor + 1, 0
    elif how == 'patch':
        patch += 1
    return f'{major}.{minor}.{patch}'


def compute(current: str,
            how: str,
            mode: str,
            prerelease_tag: str = '',
            run_number: str = '') -> str:
    """
    Return the version a dispatch publishes.

    :param current: The version as written in `pyproject.toml`.
    :param how: One of `BUMPS`.
    :param mode:`release`, `prerelease`, `test` or `dry-run`.
    :param prerelease_tag: Required for `prerelease`, rejected otherwise.
    :param run_number: Required for `test`, which keys a throwaway `.devN` on it.
    :returns: The version to tag and publish.
    :raises ValueError: on any combination this scheme does not publish.
    """
    if mode not in ('release', 'prerelease', 'test', 'dry-run'):
        raise ValueError(f'{mode!r} is not a mode')
    if mode != 'prerelease' and prerelease_tag:
        raise ValueError('prerelease_tag is only valid when mode is prerelease')

    version = bump(current, how)

    if mode == 'prerelease':
        if not prerelease_tag:
            raise ValueError('prerelease_tag is required when mode is '
                             'prerelease (e.g. b1, rc1)')
        if not PRERELEASE_TAG.match(prerelease_tag):
            raise ValueError(
                f'{prerelease_tag!r} is not a canonical PEP 440 pre-release '
                f'segment: a, b or rc followed by a number with no leading '
                f'zero (e.g. b1, rc1)')
        version = f'{version}{prerelease_tag}'
    elif mode == 'test':
        if not run_number:
            raise ValueError('run_number is required when mode is test')
        # Valid PEP 440, sorts below the real X.Y.Z, and unique per run so
        # TestPyPI's immutable-version rule never bites on a repeated publish.
        version = f'{version}.dev{run_number}'

    if version == current:
        raise ValueError(
            f'{current!r} would be published again unchanged. A release has to '
            f'move the version: pick a bump that advances past it, or for a '
            f'pre-release use the next pre-release number')

    # The computed string becomes a tag, a PyPI version and a docs directory, so
    # it has to survive a round trip through `packaging` byte for byte.
    if str(Version(version)) != version:
        raise ValueError(
            f'{version!r} is not canonical; `packaging` normalizes it to '
            f'{str(Version(version))!r}, so the tag and the version PyPI '
            f'indexes would differ')
    return version


def _cmd_compute(args: argparse.Namespace) -> int:
    print(
        compute(args.current, args.bump, args.mode, args.prerelease_tag,
                args.run_number))
    return 0


def main(argv: list[str] | None = None) -> int:
    """
    Parse arguments and run the requested subcommand.

    :param argv: Argument list, defaulting to `sys.argv[1:]`.
    :returns: Process exit status.
    """
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='command', required=True)

    comp = sub.add_parser('compute',
                          help='print the version a dispatch would publish')
    comp.add_argument('--current', required=True)
    comp.add_argument('--bump', required=True)
    comp.add_argument('--mode', required=True)
    comp.add_argument('--prerelease-tag', default='', dest='prerelease_tag')
    comp.add_argument('--run-number', default='', dest='run_number')
    comp.set_defaults(func=_cmd_compute)

    args = parser.parse_args(argv)
    try:
        return int(args.func(args))
    except (TypeError, ValueError) as exc:
        # stderr, not stdout: the caller captures stdout to read the version, so
        # an annotation printed there is swallowed into a shell variable and the
        # failing step reports an empty log.
        print(f'::error::{exc}', file=sys.stderr)
        return 1


if __name__ == '__main__':
    sys.exit(main())
