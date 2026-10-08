"""
The `HISTORY.md` entry for a release, from the commits the release contains.

Replaces `github_changelog_generator`, which fits pika badly: it crawls every tag and every closed
issue by date, pika has tags back to `v0.9a` and a 1,380-line changelog, it needs a Ruby gem and a
token nothing else in the release path uses, and it writes the heading link from the *previous* tag,
which is why the committed 1.4.3 entry points at `tree/1.4.2`.

**The milestone is definitive; the commit range only cross-checks.** The milestone is where the
editorial decision about a release is recorded, and pika curates it, so this never second-guesses it.
What the range is for is catching the three ways the milestone and the history can disagree, each
reported and none acted on:

- merged in the range with no milestone, which wants milestoning
- merged in the range carrying a *different* milestone, which wants checking. Never move a milestone
  that has already shipped: #1596 says 1.4.1 and #1678 says 1.4.4, and both merged inside
  `1.4.0..HEAD`
- in the milestone but closed before the previous tag, which is almost certainly mis-milestoned.
  #1558 is the live example: closed 2026-05-05, shipped in 1.4.0 the next day, and still milestoned
  1.5.0

An earlier attempt inverted this and let the range decide membership. That is wrong for pika: it made
the generator paper over milestone data errors instead of surfacing them, and it dropped 20 of the 62
numbers in the committed 1.4.0 entry, because pika closes plenty of issues with no linking pull
request.

`gh` rather than a GitHub client library, deliberately. There is no official Python client: GitHub
maintains Octokit for JavaScript, Ruby, .NET and Terraform only, and lists every Python option as third
party. Meanwhile `gh` is already load-bearing - `release.py pr` and `release.yaml` both shell out to it
- and already authenticated on a maintainer's machine and through `GH_TOKEN` in Actions. A library
would mean two ways to reach GitHub plus a token to wire up, which is the `CHANGELOG_GITHUB_TOKEN`
problem this file exists to remove.

Two queries, never one per pull request: `gh pr list` returns title, author, milestone and closing
issues together, and `gh issue list` returns titles and labels. An earlier version made one
`gh pr view` per stray, fourteen subprocesses at about a second each, ninety-eight in the worst case.
"""

from __future__ import annotations

import datetime
import json
import re
import subprocess
from typing import Any, Callable

#: How many rows each query asks for. `gh` applies this and exits 0 with no
#: warning, so a saturated answer would silently lose entries; `_rows` refuses
#: instead. Pagination is the one thing a client library would have given us.
#: Both queries are date-bounded to the release window, so 500 is ample: the
#: widest window to date holds 104 pull requests and 55 issues.
LIMIT = 500

REPO = 'pika/pika'
REPO_URL = f'https://github.com/{REPO}'

#: Label to section heading. Everything unmatched falls to `CLOSED_ISSUES`, the
#: name the historical entries use. Kept in step with `.github/release.yml`,
#: which categorises the GitHub release notes from the same labels; a test
#: asserts the mapping, not merely the label set.
SECTIONS = (
    ('C-enhancement', '**Implemented enhancements:**'),
    ('C-bug', '**Fixed bugs:**'),
    ('A-documentation', '**Documentation:**'),
)

CLOSED_ISSUES = '**Closed issues:**'
MERGED_PULLS = '**Merged pull requests:**'

#: Both merge styles. `1.4.0..HEAD` holds 96 merge-style subjects and 2
#: squash-style, so both occur despite the first dominating.
MERGE_SUBJECT = re.compile(r'^Merge pull request #(\d+) ')
SQUASH_SUBJECT = re.compile(r'\(#(\d+)\)$')

#: Escaped inside titles, because `HISTORY.md` is included verbatim into
#: `docs/changelog.md` and rendered by mkdocs-material. The committed entries
#: escape exactly these: `_` 26 times, `(` and `)` 14 each, `[` and `]` twice.
#: An unescaped `_pair_` or `[text](url)` becomes markup on the published site,
#: Measured, not guessed: the committed region escapes `#` 106 times, `_` 26,
#: `(` and `)` 14 each, `[` and `]` twice and `<` once, and leaves backticks
#: alone because a backtick in a title is deliberate code formatting.
MARKDOWN = re.compile(r'([_()\[\]<#])')


class ChangelogError(OSError):
    """
    A command this module runs failed, or answered something unusable.

    Derived from `OSError` so `release.py main` reports it as one line rather than a traceback; it
    catches `CheckFailed`, `OSError`, `TypeError` and `ValueError`.
    """


def _run(tool: str, *args: str) -> str:
    """
    Run *tool* and return its stdout.

    :param tool:`gh` or `git`.
    :param args: Arguments after the tool.
    :returns: Captured stdout.
    :raises ChangelogError: if it fails.
    """
    done = subprocess.run((tool, *args),
                          capture_output=True,
                          text=True,
                          check=False)
    if done.returncode != 0:
        raise ChangelogError(f'{tool} {" ".join(args)} failed: '
                             f'{done.stderr.strip() or done.stdout.strip()}')
    return done.stdout


def _gh(*args: str) -> str:
    """
    Run `gh`.

    :param args: Arguments after `gh`.
    :returns: Captured stdout.
    """
    return _run('gh', *args)


def _git(*args: str) -> str:
    """
    Run git.

    :param args: Arguments after `git`.
    :returns: Captured stdout.
    """
    return _run('git', *args)


def _json(raw: str, what: str) -> Any:
    """
    Parse *raw* as JSON, naming the query when it is not.

    :param raw: Captured stdout.
    :param what: The query, for the message.
    :returns: The parsed value.
    :raises ChangelogError: if *raw* is not JSON.
    """
    try:
        return json.loads(raw or '[]')
    except ValueError as exc:
        raise ChangelogError(f'gh {what} did not answer JSON: {exc}. It '
                             f'returned {raw[:200]!r}') from exc


def _rows(raw: str, what: str) -> list:
    """
    Parse a list answer, refusing one that saturated `--limit`.

    :param raw: Captured stdout.
    :param what: The query, for the message.
    :returns: The parsed rows.
    :raises ChangelogError: if unusable, or exactly `LIMIT` long.
    """
    rows = _json(raw, what)
    if len(rows) >= LIMIT:
        raise ChangelogError(
            f'gh {what} returned {len(rows)} rows, the --limit of {LIMIT}, so '
            f'the answer is probably truncated and the changelog would lose '
            f'entries silently. Raise LIMIT in changelog.py')
    return rows


def escape(title: str) -> str:
    """
    Return *title* with markdown-significant characters escaped.

    :param title: An issue or pull-request title, as GitHub stores it.
    :returns: The title, safe to interpolate into a list item.
    """
    return MARKDOWN.sub(r'\\\1', title)


def author_of(item: dict) -> tuple[str, str]:
    """
    Return a pull request's author as `(name, url)`.

    Three cases, all present in the live data. A person renders as their login. A GitHub App arrives
    as `login: "app/dependabot"` with `is_bot: true`, and the committed entries render that as
    `dependabot[bot]` linking to `/apps/dependabot`, because `github.com/app/dependabot` is a 404
    and ten of the fourteen unmilestoned pull requests in `1.4.0..HEAD` are that bot. A deleted
    account arrives as `author: null`, which GitHub itself renders as `ghost`.

    :param item: The `gh` JSON for one pull request.
    :returns: The displayed name and the profile URL.
    """
    author = item.get('author') or {}
    login = author.get('login') or 'ghost'
    if author.get('is_bot') and login.startswith('app/'):
        slug = login[len('app/'):]
        return f'{slug}[bot]', f'https://github.com/apps/{slug}'
    return login, f'https://github.com/{login}'


def section_for(labels: list[str]) -> str:
    """
    Return the section heading an issue with *labels* belongs under.

    :param labels: Label names on the issue.
    :returns: One of the `SECTIONS` headings, or `CLOSED_ISSUES`.
    """
    for label, heading in SECTIONS:
        if label in labels:
            return heading
    return CLOSED_ISSUES


def merged_numbers(since: str,
                   until: str = 'HEAD',
                   git: Callable[..., str] = _git) -> list[int]:
    """
    Return the pull-request numbers merged in a commit range.

    :param since: The tag the range starts after.
    :param until: The ref the range ends at.
    :param git: Injected git runner.
    :returns: Numbers, newest commit first.
    """
    found = []
    for subject in git('log', '--format=%s', f'{since}..{until}').splitlines():
        match = MERGE_SUBJECT.match(subject) or SQUASH_SUBJECT.search(subject)
        if match:
            number = int(match.group(1))
            if number not in found:
                found.append(number)
    return found


def tag_date(tag: str, git: Callable[..., str] = _git) -> str:
    """
    Return the commit date of *tag*, as `YYYY-MM-DD`.

    Used only to bound the two queries to the release window so `--limit` cannot saturate.
    Membership is decided by the commit range, never by this date.

    :param tag: The tag to date.
    :param git: Injected git runner.
    :returns: The date.
    """
    return git('log', '-1', '--format=%cs', tag).strip()


def milestone_issues(milestone: str,
                     gh: Callable[..., str] = _gh) -> list[dict]:
    """
    Return the closed issues the milestone carries.

    `gh issue list` excludes pull requests, which is what makes the two queries separable.

    :param milestone: The milestone title, e.g. `1.5.0`.
    :param gh: Injected `gh` runner.
    :returns: Rows with `number`, `title`, `labels` and `closedAt`.
    """
    raw = gh('issue', 'list', '--milestone', milestone, '--state', 'closed',
             '--limit', str(LIMIT), '--json', 'number,title,labels,closedAt')
    return _rows(raw, 'issue list')


def milestone_pulls(milestone: str, gh: Callable[..., str] = _gh) -> list[dict]:
    """
    Return the merged pull requests the milestone carries.

    :param milestone: The milestone title.
    :param gh: Injected `gh` runner.
    :returns: Rows with `number`, `title`, `author`, `labels` and `mergedAt`.
    """
    raw = gh('pr', 'list', '--state',
             'merged', '--search', f'milestone:{milestone}', '--limit',
             str(LIMIT), '--json', 'number,title,author,labels,mergedAt')
    return _rows(raw, 'pr list')


def merged_in_window(since_date: str,
                     gh: Callable[..., str] = _gh) -> dict[int, str]:
    """
    Return every pull request merged since *since_date* and the milestone it names.

    Used only for the cross-check, in one query rather than one `gh pr view` per pull request: an
    earlier version made fourteen subprocesses at about a second each, and ninety-eight in the worst
    case.

    :param since_date:`YYYY-MM-DD`, the start of the window.
    :param gh: Injected `gh` runner.
    :returns: Maps number to the milestone title, or `''` when it has none.
    """
    raw = gh('pr', 'list', '--state',
             'merged', '--search', f'merged:>={since_date}', '--limit',
             str(LIMIT), '--json', 'number,milestone')
    return {
        row['number']: (row.get('milestone') or {}).get('title') or ''
        for row in _rows(raw, 'pr list')
    }


def render(version: str,
           previous: str,
           issues: list[dict],
           pulls: list[dict],
           released: datetime.date | None = None) -> str:
    """
    Return the `HISTORY.md` entry.

    The format is the one the existing entries use, because this sits above 1,379 lines of it:
    descending by number within each section, titles escaped, and the `\\#` link form. Issues and
    pull requests are grouped together under a label's heading, which is how the committed 1.4.0
    entry has #1579 and #1561 under `**Implemented enhancements:**`; only the catch-all separates
    them.

    :param version: The version being released.
    :param previous: The version the range starts after.
    :param issues: Rows with `number`, `title` and `labels`.
    :param pulls: Rows with `number`, `title`, `labels` and `author`.
    :param released: The release date, defaulting to today in UTC.
    :returns: The entry, ending in a single newline.
    """
    day = (released or
           datetime.datetime.now(datetime.timezone.utc).date()).isoformat()
    lines = [
        f'## [{version}]({REPO_URL}/tree/{version}) ({day})',
        '',
        f'[Full Changelog]({REPO_URL}/compare/{previous}...{version})',
    ]

    grouped: dict[str, list[dict]] = {}
    spare_issues: list[dict] = []
    spare_pulls: list[dict] = []
    for item in list(issues) + list(pulls):
        names = [
            label['name'] if isinstance(label, dict) else label
            for label in item.get('labels') or []
        ]
        heading = section_for(names)
        if heading == CLOSED_ISSUES:
            (spare_pulls if 'author' in item else spare_issues).append(item)
        else:
            grouped.setdefault(heading, []).append(item)

    def newest(items: list[dict]) -> list[dict]:
        """
        Return *items* newest first, which is how every committed section reads.

        :param items: Rows with a `number`.
        :returns: The rows, descending.
        """
        return sorted(items, key=lambda item: item['number'], reverse=True)

    def bullet(item: dict) -> str:
        """
        Return one list item, in the form the committed entries use.

        :param item: An issue or pull-request row.
        :returns: The rendered bullet.
        """
        if 'author' in item:
            name, url = author_of(item)
            return (f'- {escape(item["title"])} '
                    f'[\\#{item["number"]}]'
                    f'({REPO_URL}/pull/{item["number"]}) ([{name}]({url}))')
        return (f'- {escape(item["title"])} '
                f'[\\#{item["number"]}]({REPO_URL}/issues/{item["number"]})')

    for heading in [heading for _, heading in SECTIONS]:
        if heading in grouped:
            lines += ['', heading, '']
            lines += [bullet(item) for item in newest(grouped[heading])]
    for heading, items in ((CLOSED_ISSUES, spare_issues), (MERGED_PULLS,
                                                           spare_pulls)):
        if items:
            lines += ['', heading, '']
            lines += [bullet(item) for item in newest(items)]

    return '\n'.join(lines) + '\n'


def collect(
        milestone: str,
        previous: str,
        until: str = 'HEAD',
        git: Callable[..., str] = _git,
        gh: Callable[..., str] = _gh) -> tuple[list[dict], list[dict], dict]:
    """
    Return what the milestone carries, plus how the history disagrees with it.

    The milestone decides membership. The range decides nothing; it only populates the report.

    :param milestone: The milestone title, which is definitive.
    :param previous: The tag the cross-check range starts after.
    :param until: The ref the cross-check range ends at.
    :param git: Injected git runner.
    :param gh: Injected `gh` runner.
    :returns:`(issues, pulls, report)`, where `report` maps a concern to what raises it.
    """
    issues = milestone_issues(milestone, gh)
    pulls = milestone_pulls(milestone, gh)
    carried = {row['number'] for row in pulls}

    since_date = tag_date(previous, git)
    in_window = merged_in_window(since_date, gh)
    report: dict[str, list] = {'none': [], 'elsewhere': [], 'stale': []}

    for number in merged_numbers(previous, until, git):
        if number in carried or number not in in_window:
            # Already accounted for, or a `(#N)` subject naming something that is
            # not a pull request merged in this window. pika has 21 of the latter,
            # and an earlier version ran `gh pr view` on them and aborted.
            continue
        named = in_window[number]
        (report['none'] if not named else
         report['elsewhere']).append(number if not named else (number, named))

    # In the milestone but finished before the previous tag, so it shipped in an
    # earlier release and the milestone is wrong. #1558 is the live example.
    for row in issues:
        when = (row.get('closedAt') or '')[:10]
        if when and when < since_date:
            report['stale'].append((row['number'], when))
    for row in pulls:
        when = (row.get('mergedAt') or '')[:10]
        if when and when < since_date:
            report['stale'].append((row['number'], when))

    return issues, pulls, report
