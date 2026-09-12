#!/usr/bin/env python3
"""
Helpers for the documentation-site deploy workflow.

`mike` records published versions and their aliases in a `versions.json`.

Both subcommands read that structure as JSON on stdin, so the caller can supply either the local
branch (`mike list --json`) or what is actually published (`git show`).

This lives outside the workflow YAML for two reasons: a `run:` block scalar cannot hold column-zero
Python, and logic that decides whether to move a site-wide alias should be directly testable.
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import Any


def _load(stream: Any) -> list[dict[str, Any]]:
    """
    Parse a `versions.json` payload, treating anything unusable as empty.

    An absent or unparsable payload is the first-deploy case: `mike list` fails when `gh-pages` does
    not exist yet, and the caller pipes its empty output here.
    """
    try:
        data = json.load(stream)
    except (json.JSONDecodeError, UnicodeDecodeError):
        return []
    return data if isinstance(data, list) else []


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


def _cmd_alias_holder(args: argparse.Namespace) -> int:
    holder = alias_holder(_load(sys.stdin), args.alias)
    if holder:
        print(holder)
    return 0


def _cmd_verify(args: argparse.Namespace) -> int:
    versions = _load(sys.stdin)
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
    return int(args.func(args))


if __name__ == '__main__':
    sys.exit(main())
