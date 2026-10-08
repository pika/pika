"""
Version arithmetic and tag interpretation for the release.

Two jobs, either side of the moment a release becomes irreversible. `compute` works out the version
a release should carry, and is now mostly a local helper: a real release is a pushed tag, so a human
writes the version into `pyproject.toml` and `pika/__init__.py` and `compute` only says what to
write. `classify` goes the other way, reading a pushed tag and returning what the workflow should do
with it, including the documentation parameters.

Both live here rather than in `release.yaml` for the same reason the alias decision lives in
`docs_site.py`: they are parsing and a state transition, the two things shell expresses worst, and
what they produce becomes a PyPI release and a documentation directory, neither of which can be
taken back.

Expressed as shell both were wrong. `${current%%[a-zA-Z]*}` strips a pre-release suffix correctly,
but every bump then incremented, so once `1.5.0a1` was published no input produced `1.5.0`: `patch`
gave `1.5.1`, `minor` gave `1.6.0`, `major` gave `2.0.0`. Promoting a pre-release meant hand-editing
`pyproject.toml` between two workflow runs. The documentation version for a stable release was
`${major}.${minor}`, where neither variable was ever assigned anywhere in the workflow, so it
evaluated to `.` and `mike` was asked to publish a directory by that name. `packaging` is used
rather than hand-rolled string work because it already knows what `base_version` and `is_prerelease`
mean.

A library, not a command. `.ci/release.py` is the only entry point, so there is one place to look up
what the release can do and one place that talks to git, `gh` and PyPI.
"""

from __future__ import annotations

import re

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

#: Tags this scheme publishes: `X.Y.Z`, optionally with a canonical pre-release
#: segment. Deliberately narrower than PEP 440, which also admits `.devN`, post
#: releases, local versions and epochs. A pushed tag is the whole trigger for a
#: release, so the set of tags that can start one is spelled out rather than
#: inferred: `1.5.0.dev3` is what `mode=test` publishes to TestPyPI and must
#: never reach PyPI, and `1.5.0+local` is not installable from an index at all.
RELEASABLE_TAG = re.compile(r'^\d+\.\d+\.\d+(?:(?:a|b|rc)(?:0|[1-9][0-9]*))?$')


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


def classify(tag: str) -> dict[str, str]:
    """
    Return the release parameters a pushed tag implies.

    The pre-release decision comes from `packaging` rather than from the shape of the tag text.
    `release.yaml` used to take it from a dispatch input on the grounds that reading it off a tag
    was guesswork, which was true of a `case` on substrings and is not true of
    `Version.is_prerelease` on a parsed PEP 440 version.

    The documentation parameters are decided here, with the pre-release decision, because they
    follow from it and because getting them wrong is not recoverable. A stable release owns
    `MAJOR.MINOR` and takes the `latest` alias; a pre-release publishes under its full version and
    takes no alias, so that a `1.5.0rc1` deploy cannot move readers off `1.4`.

    :param tag: The pushed tag, which must equal the version in `pyproject.toml`.
    :returns: Keys `mode`, `version`, `docs_version`, `docs_aliases` and `docs_set_default`, ready
        to append to `$GITHUB_OUTPUT`.
    :raises ValueError: if *tag* is not a tag this scheme publishes.
    """
    if not RELEASABLE_TAG.match(tag):
        raise ValueError(
            f'{tag!r} is not a releasable tag: expected X.Y.Z, optionally with '
            f'a canonical pre-release segment (e.g. 1.5.0, 1.5.0b1, 1.5.0rc2)')

    # `RELEASABLE_TAG` admits leading zeros in each component, which `packaging`
    # strips: `1.05.0` normalizes to `1.5.0`. Tagging that would publish a
    # version PyPI indexes under a name the tag does not carry.
    if str(Version(tag)) != tag:
        raise ValueError(
            f'{tag!r} is not canonical; `packaging` normalizes it to '
            f'{str(Version(tag))!r}, so the tag and the version PyPI indexes '
            f'would differ')

    version = Version(tag)
    if version.is_prerelease:
        return {
            'mode': 'prerelease',
            'version': tag,
            'docs_version': tag,
            'docs_aliases': '',
            'docs_set_default': 'false',
        }

    major, minor = version.release[0], version.release[1]
    return {
        'mode': 'release',
        'version': tag,
        'docs_version': f'{major}.{minor}',
        'docs_aliases': 'latest',
        'docs_set_default': 'true',
    }
