"""
The `HISTORY.md` entry for a release, built from the milestone.

Replaces `github_changelog_generator`, which fits pika badly. That tool crawls every tag and every
closed issue by date, and pika has tags back to `v0.9a` and a 1,380-line changelog; it needs a Ruby
gem and a token nothing else in the release path uses; and it writes the heading link from the
*previous* tag, which is why the 1.4.3 entry in `HISTORY.md` points at `tree/1.4.2`. Above all it
ignores milestones, and pika curates those rigorously: 1.5.0 carries 38 closed issues and 84 merged
pull requests that the generator simply discards.

The shape here is `rabbitmq-dotnet-client`'s `tools/generate-changelog.sh`, in Python with the GitHub
queries injectable so the tests do not reach the network. The milestone is authoritative; the commit
range catches what escaped it.

Two rules about tags live close together and mean opposite things, so they are spelled out:

- The range *starts* at the newest tag **reachable from HEAD**, which `git describe` answers. The
  question is "what is new on this branch", and reachability is exactly that.
- Whether a version has already shipped is a question of **PEP 440 order**, not reachability, because
  1.4.1 through 1.4.4 were cut from `1.4.x` and are not ancestors of `main`. That rule lives in
  `release.py previous_release`.

Reasoning from one to the other produces a changelog that re-lists three released versions, or a
release that goes backwards. They are not the same question.
"""

from __future__ import annotations

import datetime
import json
import re
import subprocess
import sys
from typing import Callable

REPO = 'pika/pika'
REPO_URL = f'https://github.com/{REPO}'

#: Label to section heading, for issues. Everything unmatched falls to
#: `CLOSED_ISSUES`, which is what the historical entries called it. `C-refactor`,
#: `C-performance`, `dependencies` and `github_actions` land there deliberately:
#: the entries this file has to stay consistent with only ever had two sections.
SECTIONS = (
    ('C-enhancement', '**Implemented enhancements:**'),
    ('C-bug', '**Fixed bugs:**'),
    ('A-documentation', '**Documentation:**'),
)

CLOSED_ISSUES = '**Closed issues:**'
MERGED_PULLS = '**Merged pull requests:**'

#: Both merge styles. pika squash-merges nothing today - all 53 merges in the
#: most recent range are the first form - but a future squash should not vanish
#: silently from a release's changelog.
MERGE_SUBJECT = re.compile(r'^Merge pull request #(\d+) ')
SQUASH_SUBJECT = re.compile(r'\(#(\d+)\)$')


def _gh(*args: str) -> str:
    """
    Run `gh` and return its stdout.

    :param args: Arguments after `gh`.
    :returns: Captured stdout.
    :raises RuntimeError: if `gh` fails.
    """
    done = subprocess.run(('gh', *args),
                          capture_output=True,
                          text=True,
                          check=False)
    if done.returncode != 0:
        raise RuntimeError(f'gh {" ".join(args)} failed: '
                           f'{done.stderr.strip() or done.stdout.strip()}')
    return done.stdout


def _git(*args: str) -> str:
    """
    Run git and return its stdout.

    :param args: Arguments after `git`.
    :returns: Captured stdout.
    :raises RuntimeError: if git fails.
    """
    done = subprocess.run(('git', *args),
                          capture_output=True,
                          text=True,
                          check=False)
    if done.returncode != 0:
        raise RuntimeError(f'git {" ".join(args)} failed: '
                           f'{done.stderr.strip() or done.stdout.strip()}')
    return done.stdout


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


def milestone_issues(milestone: str,
                     gh: Callable[..., str] = _gh) -> list[dict]:
    """
    Return the closed issues in *milestone*.

    `gh issue list` excludes pull requests, which is what makes the two queries separable.

    :param milestone: The milestone title, e.g. `1.5.0`.
    :param gh: Injected `gh` runner.
    :returns: Dicts with `number`, `title` and `labels`.
    """
    raw = gh('issue', 'list', '--milestone', milestone, '--state', 'closed',
             '--limit', '500', '--json', 'number,title,labels')
    return [{
        'number': item['number'],
        'title': item['title'],
        'labels': [label['name'] for label in item['labels']],
    } for item in json.loads(raw or '[]')]


def milestone_pulls(milestone: str, gh: Callable[..., str] = _gh) -> list[dict]:
    """
    Return the merged pull requests in *milestone*.

    :param milestone: The milestone title.
    :param gh: Injected `gh` runner.
    :returns: Dicts with `number`, `title` and `author`.
    """
    raw = gh('pr', 'list', '--state', 'merged', '--search',
             f'milestone:{milestone}', '--limit', '500', '--json',
             'number,title,author')
    return [{
        'number': item['number'],
        'title': item['title'],
        'author': item['author']['login'],
    } for item in json.loads(raw or '[]')]


def merged_numbers(since: str,
                   until: str = 'HEAD',
                   git: Callable[..., str] = _git) -> list[int]:
    """
    Return the pull-request numbers merged in a commit range.

    :param since: The tag the range starts after.
    :param until: The ref the range ends at.
    :param git: Injected git runner.
    :returns: Numbers, in the order the commits appear.
    """
    found = []
    for subject in git('log', '--format=%s', f'{since}..{until}').splitlines():
        match = MERGE_SUBJECT.match(subject) or SQUASH_SUBJECT.search(subject)
        if match:
            number = int(match.group(1))
            if number not in found:
                found.append(number)
    return found


def pull_request(number: int, gh: Callable[..., str] = _gh) -> dict:
    """
    Return one pull request's title and author.

    :param number: The pull-request number.
    :param gh: Injected `gh` runner.
    :returns: A dict with `number`, `title` and `author`.
    """
    item = json.loads(
        gh('pr', 'view', str(number), '--json', 'number,title,author'))
    return {
        'number': item['number'],
        'title': item['title'],
        'author': item['author']['login'],
    }


def unmilestoned(since: str,
                 known: set[int],
                 until: str = 'HEAD',
                 git: Callable[..., str] = _git,
                 gh: Callable[..., str] = _gh) -> list[dict]:
    """
    Return merged pull requests in the range that the milestone does not carry.

    They are included rather than ignored, because they are real changes in the release, and
    reported by the caller so the milestone can be corrected. Silently dropping them is how a
    milestone stops reflecting what shipped.

    :param since: The tag the range starts after.
    :param known: Numbers the milestone already supplied.
    :param until: The ref the range ends at.
    :param git: Injected git runner.
    :param gh: Injected `gh` runner.
    :returns: Dicts with `number`, `title` and `author`.
    """
    return [
        pull_request(number, gh)
        for number in merged_numbers(since, until, git)
        if number not in known
    ]


def render(version: str,
           previous: str,
           issues: list[dict],
           pulls: list[dict],
           released: datetime.date | None = None) -> str:
    """
    Return the `HISTORY.md` entry.

    The format is the one the existing entries use, down to the escaped `\\#` and the author link,
    because this file has to live above them without looking different.

    :param version: The version being released.
    :param previous: The version the range starts after.
    :param issues: Closed issues, each with `number`, `title` and `labels`.
    :param pulls: Merged pull requests, each with `number`, `title` and `author`.
    :param released: The release date, defaulting to today.
    :returns: The entry, ending in a newline.
    """
    # UTC rather than the local date: a release cut late in the evening would
    # otherwise be dated differently depending on who cut it.
    day = (released or
           datetime.datetime.now(datetime.timezone.utc).date()).isoformat()
    lines = [
        f'## [{version}]({REPO_URL}/tree/{version}) ({day})',
        '',
        f'[Full Changelog]({REPO_URL}/compare/{previous}...{version})',
    ]

    grouped: dict[str, list[dict]] = {}
    for issue in issues:
        grouped.setdefault(section_for(issue['labels']), []).append(issue)

    for _, heading in SECTIONS:
        if heading in grouped:
            lines += ['', heading, '']
            lines += [
                f'- {issue["title"]} '
                f'[\\#{issue["number"]}]({REPO_URL}/issues/{issue["number"]})'
                for issue in sorted(grouped[heading], key=lambda i: i['number'])
            ]
    if CLOSED_ISSUES in grouped:
        lines += ['', CLOSED_ISSUES, '']
        lines += [
            f'- {issue["title"]} '
            f'[\\#{issue["number"]}]({REPO_URL}/issues/{issue["number"]})'
            for issue in sorted(grouped[CLOSED_ISSUES],
                                key=lambda i: i['number'])
        ]

    if pulls:
        lines += ['', MERGED_PULLS, '']
        lines += [
            f'- {pull["title"]} '
            f'[\\#{pull["number"]}]({REPO_URL}/pull/{pull["number"]}) '
            f'([{pull["author"]}](https://github.com/{pull["author"]}))'
            for pull in sorted(pulls, key=lambda p: p['number'])
        ]

    return '\n'.join(lines) + '\n'


def generate(
    milestone: str,
    version: str,
    previous: str,
    until: str = 'HEAD',
    git: Callable[..., str] = _git,
    gh: Callable[..., str] = _gh,
    report: Callable[[str],
                     None] = lambda message: print(message, file=sys.stderr)
) -> str:
    """
    Return the entry for *version*, from its milestone plus the commit range.

    *milestone* and *version* are separate because a pre-release draws on the milestone of the
    version it leads to: 1.5.0a1 is milestone 1.5.0. Rendering takes the version, so the heading and
    the compare link name the tag actually being cut.

    :param milestone: The milestone title to draw content from.
    :param version: The version being released, which the entry names.
    :param previous: The tag the range starts after.
    :param until: The ref the range ends at.
    :param git: Injected git runner.
    :param gh: Injected `gh` runner.
    :param report: Where to send the note about unmilestoned pull requests.
    :returns: The rendered entry.
    """
    issues = milestone_issues(milestone, gh)
    pulls = milestone_pulls(milestone, gh)
    strays = unmilestoned(previous, {pull['number'] for pull in pulls}, until,
                          git, gh)
    if strays:
        report(f'{len(strays)} merged pull requests in {previous}..{until} '
               f'carry no {milestone} milestone; they are included below. '
               f'Milestone them to silence this:')
        for stray in strays:
            report(f'  #{stray["number"]} {stray["title"]}')
    return render(version, previous, issues, pulls + strays)
