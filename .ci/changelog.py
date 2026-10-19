"""
The `HISTORY.md` entry for a release, from the milestone it carries.

The milestone decides membership, because that is where the editorial decision about a release is
recorded. The commit range only cross-checks it, reporting three ways the two can disagree and
acting on none of them: merged in the range with no milestone, merged carrying a different
milestone, and in the milestone but closed before the previous tag.

Two queries, never one per pull request: `gh pr list` returns title, author, milestone and closing
issues together, and `gh issue list` returns titles and labels. Replaces
`github_changelog_generator`; see docs/internal/release.md for why, and for the measurements behind
the constants below.
"""

from __future__ import annotations

import datetime
import json
import re
import subprocess
from typing import Any, Callable

#: How many rows each query asks for. `gh` saturates silently, so `_rows`
#: refuses an answer this long; see docs/internal/release.md.
LIMIT = 500

REPO = 'pika/pika'
REPO_URL = f'https://github.com/{REPO}'

#: Label to section heading; anything unmatched falls to `CLOSED_ISSUES`. Kept
#: in step with `.github/release.yml`, which a test asserts.
SECTIONS = (
    ('C-enhancement', '**Implemented enhancements:**'),
    ('C-bug', '**Fixed bugs:**'),
    ('A-documentation', '**Documentation:**'),
)

CLOSED_ISSUES = '**Closed issues:**'
MERGED_PULLS = '**Merged pull requests:**'

#: Both merge styles, which both occur. The doubled anchoring is not
#: redundant: see docs/internal/release.md.
MERGE_SUBJECT = re.compile(r'^Merge pull request #(\d+) ')
SQUASH_SUBJECT = re.compile(r'\(#(\d+)\)$')

#: Escaped inside titles, because `HISTORY.md` renders on the published site.
#: The set matches what the committed entries escape; see
#: docs/internal/release.md.
MARKDOWN = re.compile(r'([_()\[\]<#])')

#: Markdown does not interpret a code span, so escaping inside one renders the
#: backslash; see docs/internal/release.md.
CODE_SPAN = re.compile(r'`[^`]*`')

#: Closed, but not fixed. An issue closed for either of these reasons did not
#: ship in the release that happens to carry its milestone.
UNSHIPPED_REASONS = frozenset({'NOT_PLANNED', 'DUPLICATE'})


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
    :raises ChangelogError: if unusable, or `LIMIT` rows or longer.
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
    Return *title* with markdown-significant characters escaped outside code spans.

    :param title: An issue or pull-request title, as GitHub stores it.
    :returns: The title, safe to interpolate into a list item.
    """
    out = []
    end = 0
    for span in CODE_SPAN.finditer(title):
        out.append(MARKDOWN.sub(r'\\\1', title[end:span.start()]))
        out.append(span.group(0))
        end = span.end()
    out.append(MARKDOWN.sub(r'\\\1', title[end:]))
    return ''.join(out)


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
    Return the commit date of *tag* in UTC, as `YYYY-MM-DD`.

    Used only to bound the two queries to the release window so `--limit` cannot saturate.
    Membership is decided by the commit range, never by this date.

    UTC rather than `%cs`, which renders the date in the offset the commit recorded. GitHub's
    `merged:>=` compares UTC dates, so the two disagree by a day whenever a commit's offset carries
    it across midnight, and the direction that matters is the dangerous one: a commit stamped
    `2026-05-07T08:00+09:00` is `2026-05-06` in UTC, so `%cs` starts the window a day late and
    anything merged in between is dropped from the cross-check without a word. Measured, not
    reasoned: `%cs` says 2026-05-07 for that commit and the UTC date says 2026-05-06.

    :param tag: The tag to date.
    :param git: Injected git runner.
    :returns: The date, in UTC.
    """
    stamp = git('log', '-1', '--format=%ct', tag).strip()
    try:
        seconds = int(stamp)
    except ValueError as exc:
        raise ChangelogError(f'git gave {stamp!r} for the commit date of '
                             f'{tag}, which is not a timestamp') from exc
    return datetime.datetime.fromtimestamp(
        seconds, datetime.timezone.utc).date().isoformat()


def milestone_issues(
        milestone: str,
        gh: Callable[..., str] = _gh) -> tuple[list[dict], list[dict]]:
    """
    Return the closed issues the milestone carries.

    `gh issue list` excludes pull requests, which is what makes the two queries separable.

    Closed is not the same as done. `--state closed` also returns issues closed as not planned or as
    duplicates, and listing those as shipped claims the release fixed something nobody fixed. pika
    has three `NOT_PLANNED` among its most recent sixty closed issues, so this is a live problem
    rather than a hypothetical one. They are separated out rather than dropped, because an issue
    closed as not planned but still carrying a release milestone is a data error the operator should
    see.

    The test is exclusion rather than `stateReason == 'COMPLETED'`: an issue closed before GitHub
    recorded a reason answers `null`, and that should stay in the entry rather than vanish from it.

    :param milestone: The milestone title, e.g. `1.5.0`.
    :param gh: Injected `gh` runner.
    :returns:`(shipped, unplanned)`, both rows with `number`, `title`, `labels` and `closedAt`.
    """
    raw = gh('issue',
             'list', '--milestone', milestone, '--state', 'closed', '--limit',
             str(LIMIT), '--json', 'number,title,labels,closedAt,stateReason')
    shipped: list[dict] = []
    unplanned: list[dict] = []
    for row in _rows(raw, 'issue list'):
        reason = row.get('stateReason')
        (unplanned if reason in UNSHIPPED_REASONS else shipped).append(row)
    return shipped, unplanned


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
    issues, unplanned = milestone_issues(milestone, gh)
    pulls = milestone_pulls(milestone, gh)
    carried = {row['number'] for row in pulls}

    since_date = tag_date(previous, git)
    in_window = merged_in_window(since_date, gh)
    report: dict[str, list] = {
        'none': [],
        'elsewhere': [],
        'stale': [],
        'unplanned': [row['number'] for row in unplanned],
    }

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
