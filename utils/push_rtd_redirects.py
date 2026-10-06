#!/usr/bin/env python3
"""
Push the ReadTheDocs redirect rules in `utils/rtd_redirects.json` to RTD.

The Sphinx docs that used to live on `pika.readthedocs.io` are retired; the
current site is MkDocs on `pika.github.io/pika`. RTD redirects keep the years of
indexed links working. See RELEASE.md for the procedure and gh-1620 for the
background.

There is no maintained Python client for the RTD API: the `readthedocs` and
`readthedocs-client` names on PyPI are reservations with no uploaded files, so
this talks to API v3 over stdlib `urllib`, the way `utils/regen_spec.py` does.

Reads the token from `--token-file` (default `~/.config/rtd-token`) or the
`RTD_TOKEN` environment variable. Nothing authenticated happens without
`--apply`; the default is a dry run that prints every request it would send.

Usage::

    python3 utils/push_rtd_redirects.py              # dry run, no token needed
    python3 utils/push_rtd_redirects.py --list       # show what RTD has now
    python3 utils/push_rtd_redirects.py --apply      # create and update
    python3 utils/push_rtd_redirects.py --verify     # follow the old URLs

Re-running is safe. Existing rules matching a mapping entry are left alone, ones
that differ are updated in place, and rules on RTD that the mapping does not
describe are reported but never deleted, so anything configured by hand in the
dashboard survives.
"""

from __future__ import annotations

import argparse
import json
import os
import pathlib
import sys
import urllib.error
import urllib.request

API = 'https://app.readthedocs.org/api/v3'
MAPPING = pathlib.Path(__file__).resolve().parent / 'rtd_redirects.json'
DEFAULT_TOKEN_FILE = pathlib.Path.home() / '.config' / 'rtd-token'

# RTD answers 403 to urllib's default `Python-urllib/3.x` on the documentation
# host, for any method, so `--verify` has to introduce itself. The API host does
# not care, but sending it everywhere keeps one rule.
USER_AGENT = 'pika-rtd-redirects (+https://github.com/pika/pika)'

# The fields a rule is compared on. `pk`, `created` and `modified` are server
# state, and `position` is left to RTD unless a rule sets it, so none of them
# take part in deciding whether an existing rule already says what we want.
COMPARED = ('from_url', 'to_url', 'type', 'http_status', 'force', 'enabled')


def read_token(token_file):
    """
    Return the API token, or None when no source is configured.

    A file is preferred over an environment variable so the value does not show up in a process
    listing or a shell history.
    """
    env = os.environ.get('RTD_TOKEN')
    if env:
        return env.strip()
    if token_file.is_file():
        return token_file.read_text(encoding='utf-8').strip()
    return None


def load_mapping():
    """Return `(project, rules)` with each rule's defaults filled in."""
    data = json.loads(MAPPING.read_text(encoding='utf-8'))
    defaults = data.get('defaults', {})
    rules = []
    for rule in data['redirects']:
        merged = dict(defaults)
        merged.update(rule)
        rules.append(merged)
    seen = [r['from_url'] for r in rules]
    duplicates = {u for u in seen if seen.count(u) > 1}
    if duplicates:
        raise SystemExit(f'duplicate from_url in {MAPPING.name}: '
                         f'{sorted(duplicates)}')
    return data['project'], rules


def request(method, url, token, payload=None):
    """
    Perform one API call and return the decoded body, or `{}` for an empty response.

    A `urllib.error.HTTPError` carries the server's body, which is where RTD explains a rejected
    field, so it is read and re-raised as a `SystemExit` naming the field rather than a bare 400.
    """
    data = None if payload is None else json.dumps(payload).encode('utf-8')
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header('Authorization', f'Token {token}')
    req.add_header('User-Agent', USER_AGENT)
    if data is not None:
        req.add_header('Content-Type', 'application/json')
    try:
        with urllib.request.urlopen(req) as response:
            body = response.read().decode('utf-8')
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode('utf-8', 'replace')[:800]
        raise SystemExit(
            f'{method} {url} failed: HTTP {exc.code}\n{detail}') from exc
    return json.loads(body) if body.strip() else {}


def fetch_existing(project, token):
    """Return every redirect RTD currently holds, following pagination."""
    url = f'{API}/projects/{project}/redirects/'
    found = []
    while url:
        page = request('GET', url, token)
        found.extend(page.get('results', []))
        url = page.get('next')
    return found


def differences(wanted, actual):
    """Return the compared fields where `actual` does not match `wanted`."""
    return {
        field: (actual.get(field), wanted[field])
        for field in COMPARED
        if field in wanted and actual.get(field) != wanted[field]
    }


def plan(rules, existing):
    """Return `(to_create, to_update, extra)` without contacting RTD."""
    by_from = {r['from_url']: r for r in existing}
    to_create, to_update = [], []
    for rule in rules:
        match = by_from.get(rule['from_url'])
        if match is None:
            to_create.append(rule)
        else:
            delta = differences(rule, match)
            if delta:
                to_update.append((match, rule, delta))
    described = {r['from_url'] for r in rules}
    extra = [r for r in existing if r['from_url'] not in described]
    return to_create, to_update, extra


def verify(rules):
    """
    Follow each old URL and report where it lands.

    Checks `/en/stable/` specifically: a page redirect is version-agnostic, so proving one version
    resolves is the cheapest evidence that the rule is live at all.
    """
    failures = 0
    for rule in rules:
        old = f"https://pika.readthedocs.io/en/stable{rule['from_url']}"
        req = urllib.request.Request(old, method='HEAD')
        req.add_header('User-Agent', USER_AGENT)
        try:
            with urllib.request.urlopen(req) as response:
                landed = response.geturl()
        except urllib.error.HTTPError as exc:
            print(f'  HTTP {exc.code:<4} {old}')
            failures += 1
            continue
        ok = landed.rstrip('/') == rule['to_url'].rstrip('/')
        print(f"  {'ok  ' if ok else 'WRONG'} {old}\n        -> {landed}")
        failures += not ok
    return failures


def main():
    # Spelled out rather than sliced from `__doc__`, which is None under -OO.
    parser = argparse.ArgumentParser(
        description='Push the ReadTheDocs redirect rules in '
        'utils/rtd_redirects.json to RTD.')
    parser.add_argument('--apply',
                        action='store_true',
                        help='send the requests; without this, print them only')
    parser.add_argument('--list',
                        action='store_true',
                        help='print the redirects RTD currently holds')
    parser.add_argument('--verify',
                        action='store_true',
                        help='follow each old URL and report where it lands')
    parser.add_argument('--token-file',
                        type=pathlib.Path,
                        default=DEFAULT_TOKEN_FILE,
                        help=f'default {DEFAULT_TOKEN_FILE}')
    args = parser.parse_args()

    project, rules = load_mapping()
    print(f'{MAPPING.name}: {len(rules)} rules for project {project!r}')

    if args.verify:
        print('\nFollowing the old URLs:')
        failures = verify(rules)
        print(
            f'\n{len(rules) - failures}/{len(rules)} land on the mapped target')
        return 1 if failures else 0

    token = read_token(args.token_file)
    if token is None:
        if args.apply or args.list:
            raise SystemExit(
                f'no token: set RTD_TOKEN or write one to {args.token_file}')
        print('\nNo token found, so RTD was not contacted. Showing the rules '
              'that would be created on a project with none:\n')
        for rule in rules:
            print(f"  POST   {rule['from_url']}\n"
                  f"      -> {rule['to_url']}  "
                  f"({rule['type']}, {rule['http_status']}, "
                  f"force={rule['force']})")
        print('\nRun with --list once a token is available to diff against '
              'what RTD already holds.')
        return 0

    existing = fetch_existing(project, token)
    print(f'RTD currently holds {len(existing)} redirect(s)')

    if args.list:
        for rule in sorted(existing, key=lambda r: r.get('position', 0)):
            print(f"  pk={rule['pk']:<6} {rule['type']:<22} "
                  f"{rule['from_url']}\n      -> {rule['to_url']}  "
                  f"({rule['http_status']}, force={rule['force']}, "
                  f"enabled={rule['enabled']})")
        return 0

    to_create, to_update, extra = plan(rules, existing)
    unchanged = len(rules) - len(to_create) - len(to_update)
    print(
        f'\nplan: {len(to_create)} to create, {len(to_update)} to update, '
        f'{unchanged} already correct, {len(extra)} on RTD not in the mapping')

    for rule in to_create:
        print(f"  create  {rule['from_url']} -> {rule['to_url']}")
    for match, rule, delta in to_update:
        shown = ', '.join(
            f'{f}: {was!r} -> {now!r}' for f, (was,
                                               now) in sorted(delta.items()))
        print(f"  update  pk={match['pk']} {rule['from_url']}  ({shown})")
    for rule in extra:
        print(f"  leave   pk={rule['pk']} {rule['from_url']} "
              f"-> {rule['to_url']}  (not in the mapping; not deleted)")

    if not args.apply:
        print('\nDry run. Re-run with --apply to send these.')
        return 0

    for rule in to_create:
        body = {k: rule[k] for k in COMPARED if k in rule}
        request('POST', f'{API}/projects/{project}/redirects/', token, body)
        print(f"  created {rule['from_url']}")
    for match, rule, _ in to_update:
        body = {k: rule[k] for k in COMPARED if k in rule}
        request('PUT', f"{API}/projects/{project}/redirects/{match['pk']}/",
                token, body)
        print(f"  updated pk={match['pk']} {rule['from_url']}")
    print('\nDone. Re-run with --verify to confirm the old URLs land '
          'on the new site.')
    return 0


if __name__ == '__main__':
    sys.exit(main())
