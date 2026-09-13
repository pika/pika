#!/usr/bin/env python3
"""
Decisions for the documentation-site deploy workflow.

`mike` records published versions and their aliases in a `versions.json`.

Both subcommands read that structure as JSON on stdin, so the caller can supply either the local
branch (`mike list --json`) or what is actually published (`git show`).

The alias decision lives here, not in the workflow, because it governs a site-wide change: the
`latest` alias also backs the site-root redirect and every `latest/` URL compiled into a released
wheel. Expressed as shell it was both untestable and wrong, comparing versions with `sort -V`, which
ranks `1.5.0rc1` above `1.5` and `dev` above everything.

Every failure here is a hard failure. An earlier version treated an unreadable payload as "no
versions published", which reads as "nothing holds the alias" and hands the alias over: exactly the
outcome the check exists to prevent.
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import Any

from packaging.version import InvalidVersion, Version

#: The version name published from `main`. It is a version rather than an alias,
#: and it holds `latest` only until the first real release takes over.
DEV_VERSION = 'dev'


def load_versions(stream: Any) -> list[dict[str, Any]]:
    """
    Parse a `versions.json` payload from *stream*.

    :param stream: Readable text stream holding the JSON document.
    :returns: The parsed list of version entries, which may be empty.
    :raises ValueError: if the payload is empty or is not a JSON list. `mike list --json` prints
        ``[]`` for a missing branch, so an empty payload means the command failed rather than that
        nothing is published.
    """
    text = stream.read()
    if not text.strip():
        raise ValueError(
            'empty versions payload; expected at least "[]" (did the '
            'mike command fail?)')
    data = json.loads(text)
    if not isinstance(data, list):
        raise ValueError(f'expected a JSON list of versions, got {type(data)}')
    return data


def alias_holder(versions: list[dict[str, Any]], alias: str) -> str:
    """
    Return the version currently holding *alias*, or the empty string if none does.

    :param versions: Parsed `versions.json` entries.
    :param alias: The alias to look up, e.g. ``latest``.
    """
    for entry in versions:
        if alias in (entry.get('aliases') or []):
            return str(entry.get('version', ''))
    return ''


def find_version(versions: list[dict[str, Any]],
                 version: str) -> dict[str, Any] | None:
    """
    Return the entry for *version*, or None when it is not published.

    :param versions: Parsed `versions.json` entries.
    :param version: The version name to look for.
    """
    for entry in versions:
        if entry.get('version') == version:
            return entry
    return None


def is_prerelease(name: str) -> bool:
    """
    True if *name* is a PEP 440 pre-release.

    A name that is not a version at all, such as ``dev``, is not a pre-release; the callers handle
    those separately.

    :param name: A version name as published by `mike`.
    """
    try:
        return Version(name).is_prerelease
    except InvalidVersion:
        return False


def should_move_alias(versions: list[dict[str, Any]], version: str,
                      alias: str) -> tuple[bool, str]:
    """
    Decide whether *version* may take *alias* from whichever version holds it.

    The rules, in the order they are applied:

    1. A pre-release never takes an alias. `latest` drives the site root and the URLs in released
       wheels, so it points at released documentation only.
    2. Refreshing the current holder is always allowed.
    3. An unheld alias is free to take, which is what bootstraps a new site.
    4. Any real version supersedes `dev`, which holds the alias only until the first release.
    5. `dev` never takes the alias back off a real version.
    6. Otherwise the newer version wins, compared by PEP 440 rather than lexically.

    :param versions: Parsed `versions.json` entries.
    :param version: The version being published.
    :param alias: The alias being requested.
    :returns: Tuple of (may move, human-readable reason).
    :raises ValueError: if two real versions cannot be compared, since silently declining or
        silently moving would both be wrong.
    """
    holder = alias_holder(versions, alias)

    if is_prerelease(version):
        return False, (f'{version} is a pre-release, which never takes '
                       f'{alias!r}')
    if holder == version:
        return True, f'{alias!r} already points at {version}; refreshing it'
    if not holder:
        return True, f'nothing holds {alias!r} yet'
    if holder == DEV_VERSION:
        return True, (f'{alias!r} is on the {DEV_VERSION} bootstrap; '
                      f'{version} supersedes it')
    if version == DEV_VERSION:
        return False, (f'{alias!r} belongs to release {holder}; '
                       f'{DEV_VERSION} leaves it there')

    try:
        moving_forward = Version(version) >= Version(holder)
    except InvalidVersion as exc:
        raise ValueError(f'cannot compare {version!r} with the current holder '
                         f'{holder!r} of {alias!r}: {exc}') from exc

    if moving_forward:
        return True, f'{version} is newer than the current holder {holder}'
    return False, (f'{alias!r} stays on {holder}; {version} is older, so '
                   f'moving it would roll the site backward')


def _cmd_alias_decision(args: argparse.Namespace) -> int:
    move, reason = should_move_alias(load_versions(sys.stdin), args.version,
                                     args.alias)
    print('true' if move else 'false')
    print(reason, file=sys.stderr)
    return 0


def _cmd_alias_holder(args: argparse.Namespace) -> int:
    holder = alias_holder(load_versions(sys.stdin), args.alias)
    if holder:
        print(holder)
    return 0


def _cmd_verify(args: argparse.Namespace) -> int:
    versions = load_versions(sys.stdin)
    entry = find_version(versions, args.version)
    if entry is None:
        published = ', '.join(
            sorted(str(e.get('version', '?')) for e in versions)) or 'nothing'
        print(f'::error::{args.version} is not published; found: {published}')
        return 1

    have = set(entry.get('aliases') or [])
    missing = [alias for alias in args.aliases.split() if alias not in have]
    if missing:
        print(f'::error::{args.version} is published but missing '
              f'alias(es) {" ".join(missing)}')
        return 1

    print(f'{args.version} is published with aliases '
          f'[{" ".join(sorted(have))}]')
    return 0


def main(argv: list[str] | None = None) -> int:
    """
    Parse arguments and run the requested subcommand.

    :param argv: Argument list, defaulting to `sys.argv[1:]`.
    :returns: Process exit status.
    """
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='command', required=True)

    decision = sub.add_parser(
        'alias-decision',
        help='print true or false: may this version take this alias')
    decision.add_argument('--version', required=True)
    decision.add_argument('--alias', required=True)
    decision.set_defaults(func=_cmd_alias_decision)

    holder = sub.add_parser(
        'alias-holder', help='print the version holding an alias, or nothing')
    holder.add_argument('--alias', required=True)
    holder.set_defaults(func=_cmd_alias_holder)

    verify = sub.add_parser(
        'verify', help='fail unless a version is published with its aliases')
    verify.add_argument('--version', required=True)
    verify.add_argument('--aliases', default='')
    verify.set_defaults(func=_cmd_verify)

    args = parser.parse_args(argv)
    try:
        return int(args.func(args))
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        # stderr, not stdout: `alias-decision`'s caller captures stdout to read
        # the verdict, so an annotation printed there is swallowed into a shell
        # variable and the failing step reports an empty log.
        print(f'::error::{exc}', file=sys.stderr)
        return 1


if __name__ == '__main__':
    sys.exit(main())
