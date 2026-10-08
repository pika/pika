"""
Tests for the changelog generator in `.ci/changelog.py`.

The GitHub queries are injectable, so none of this reaches the network.

Two choices here were learned the hard way. The format test reads the 1.4.2 entry **out of
`HISTORY.md`** rather than quoting it: an earlier version hand-typed both sides, its fixture dropped
' on Windows' from the real title, and that self-consistency is why an ascending sort, missing title
escaping and a doubled blank line all survived a review. The file is the standard to match, the more
so because the committed entries were hand-edited after generation.

And the fakes assert the **whole** command vector. Dispatching on the first two arguments and
ignoring the rest left 13 of 29 mutants alive, including swapping the milestone for the version in
both queries, dropping `--state closed`, and reversing the commit range, which silently empties the
cross-check.
"""

from __future__ import annotations

import datetime
import importlib.util
import json
import pathlib
import re
import unittest
from typing import ClassVar

_ROOT = pathlib.Path(__file__).resolve().parents[2]
_SPEC = importlib.util.spec_from_file_location('changelog',
                                               _ROOT / '.ci' / 'changelog.py')
assert _SPEC is not None and _SPEC.loader is not None
changelog = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(changelog)


class Recorder:
    """A `gh` and git stand-in that records every call in full."""

    def __init__(self, answers=None, log='', tagged='2026-05-06'):
        """
        :param answers: Maps the first two arguments to the rows to return.
        :param log: What `git log --format=%s` returns.
        :param tagged: What `git log --format=%cs` returns.
        """
        self.answers = answers or {}
        self.log = log
        self.tagged = tagged
        self.calls: list[tuple[str, ...]] = []

    def gh(self, *args):
        """
        :param args: Arguments after `gh`.
        :returns: The canned JSON.
        """
        self.calls.append(args)
        return json.dumps(self.answers.get(args[:2], []))

    def git(self, *args):
        """
        :param args: Arguments after `git`.
        :returns: Canned output.
        """
        self.calls.append(args)
        if '--format=%cs' in args:
            return self.tagged + '\n'
        return self.log

    def call(self, *prefix):
        """
        :param prefix: Leading arguments to look for.
        :returns: The single matching call.
        """
        found = [c for c in self.calls if c[:len(prefix)] == prefix]
        assert len(found) == 1, f'{prefix} matched {len(found)} calls'
        return found[0]


class FormatTests(unittest.TestCase):
    """The rendered entry, pinned to the committed file rather than to itself."""

    def committed(self, tag, following):
        """
        :param tag: The entry to read.
        :param following: The entry after it, which bounds the read.
        :returns: The committed block, ending in one newline.
        """
        text = (_ROOT / 'HISTORY.md').read_text(encoding='utf-8')
        return text[text.index(f'## [{tag}]('):text.index(f'## [{following}]('
                                                         )].rstrip() + '\n'

    def test_it_reproduces_the_committed_1_4_2_entry_byte_for_byte(self):
        # Both titles come out of the file, so nothing here can drift from it.
        block = self.committed('1.4.2', '1.4.1')
        issue_title = re.search(r'^- (.*) \[\\#1639\]', block,
                                re.MULTILINE).group(1)
        pull_title = re.search(r'^- (.*) \[\\#1642\]', block,
                               re.MULTILINE).group(1)
        self.assertEqual(
            changelog.render('1.4.2',
                             '1.4.1', [{
                                 'number': 1639,
                                 'title': issue_title,
                                 'labels': [],
                             }], [{
                                 'number': 1642,
                                 'title': pull_title,
                                 'labels': [],
                                 'author': {
                                     'login': 'lukebakken',
                                     'is_bot': False,
                                 },
                             }],
                             released=datetime.date(2026, 7, 23)), block)

    def test_the_heading_names_the_version_being_cut(self):
        # `github_changelog_generator` wrote the previous tag here, which is why
        # the committed 1.4.3 entry links to `tree/1.4.2`.
        entry = changelog.render('1.4.3', '1.4.2', [], [])
        self.assertIn('tree/1.4.3', entry)
        self.assertIn('compare/1.4.2...1.4.3', entry)

    def test_sections_read_newest_first(self):
        entry = changelog.render('9.9.9', '9.9.8', [{
            'number': n,
            'title': str(n),
            'labels': ['C-bug'],
        } for n in (10, 30, 20)], [])
        self.assertEqual([int(n) for n in re.findall(r'\\#(\d+)', entry)],
                         [30, 20, 10])

    def test_a_labelled_pull_request_joins_the_labelled_section(self):
        # The committed 1.4.0 entry has #1579 and #1561 under
        # `**Implemented enhancements:**`, with author links.
        entry = changelog.render('9.9.9', '9.9.8', [], [{
            'number': 7,
            'title': 'a feature',
            'labels': [{
                'name': 'C-enhancement'
            }],
            'author': {
                'login': 'someone'
            },
        }])
        self.assertIn('**Implemented enhancements:**', entry)
        self.assertNotIn(changelog.MERGED_PULLS, entry)
        self.assertIn('([someone](https://github.com/someone))', entry)

    def test_it_ends_in_exactly_one_newline(self):
        entry = changelog.render('9.9.9', '9.9.8', [], [])
        self.assertTrue(entry.endswith('\n'))
        self.assertFalse(entry.endswith('\n\n'))


class EscapeTests(unittest.TestCase):
    """`HISTORY.md` is served verbatim on the documentation site."""

    def test_it_escapes_what_the_committed_entries_escape(self):
        self.assertEqual(changelog.escape('utcfromtimestamp() is deprecated'),
                         r'utcfromtimestamp\(\) is deprecated')
        self.assertEqual(changelog.escape("in '__init__.pyi'"),
                         r"in '\_\_init\_\_.pyi'")
        self.assertEqual(changelog.escape('see [docs](url)'),
                         r'see \[docs\]\(url\)')

    def test_it_leaves_backticks_alone(self):
        # Measured: the committed region escapes `#_()[]<` and no backticks,
        # because a backtick in a title is deliberate code formatting.
        self.assertEqual(changelog.escape('a check using `ruff`'),
                         'a check using `ruff`')

    def test_each_escaped_character_appears_escaped_in_the_file(self):
        text = (_ROOT / 'HISTORY.md').read_text(encoding='utf-8')
        generated = text[:text.index('## Version History')]
        for character in '_()[]<':
            with self.subTest(character=character):
                self.assertIn('\\' + character, generated)


class AuthorTests(unittest.TestCase):
    """Three author shapes, all present in the live data."""

    def test_a_person(self):
        self.assertEqual(changelog.author_of({'author': {
            'login': 'luke'
        }}), ('luke', 'https://github.com/luke'))

    def test_a_github_app(self):
        # `gh` reports `app/dependabot`; `github.com/app/dependabot` is a 404.
        self.assertEqual(
            changelog.author_of({
                'author': {
                    'login': 'app/dependabot',
                    'is_bot': True
                },
            }), ('dependabot[bot]', 'https://github.com/apps/dependabot'))

    def test_a_deleted_account(self):
        self.assertEqual(changelog.author_of({'author': None}),
                         ('ghost', 'https://github.com/ghost'))

    def test_the_bot_form_matches_the_committed_entries(self):
        name, url = changelog.author_of({
            'author': {
                'login': 'app/dependabot',
                'is_bot': True
            },
        })
        self.assertIn(f'([{name}]({url}))',
                      (_ROOT / 'HISTORY.md').read_text(encoding='utf-8'))


class SectionTests(unittest.TestCase):
    """Which heading a set of labels lands under."""

    def test_the_three_mapped_labels(self):
        self.assertEqual(changelog.section_for(['C-enhancement']),
                         '**Implemented enhancements:**')
        self.assertEqual(changelog.section_for(['C-bug']), '**Fixed bugs:**')
        self.assertEqual(changelog.section_for(['A-documentation']),
                         '**Documentation:**')

    def test_everything_else_falls_to_the_catch_all(self):
        for labels in (['C-refactor'], ['C-performance'], ['dependencies'],
                       ['github_actions'], ['A-typing'], []):
            with self.subTest(labels=labels):
                self.assertEqual(changelog.section_for(labels),
                                 changelog.CLOSED_ISSUES)

    def test_the_first_mapped_label_wins(self):
        self.assertEqual(changelog.section_for(['C-bug', 'C-enhancement']),
                         '**Implemented enhancements:**')

    def test_release_yml_maps_each_label_to_the_same_section(self):
        # The mapping, not the label set: comparing sets let `C-bug` and
        # `C-enhancement` swap titles with CI green, which is the same silent
        # miscategorisation this work exists to fix.
        text = (_ROOT / '.github' / 'release.yml').read_text(encoding='utf-8')
        pairs, title = {}, None
        for line in text.splitlines():
            heading = re.match(r'\s+- title: "(.*)"', line)
            if heading:
                title = heading.group(1)
                continue
            label = re.match(r'\s+- (\S+)$', line)
            if label and title and label.group(1) != '"*"':
                pairs[label.group(1)] = f'**{title}:**'
        self.assertEqual(pairs, dict(changelog.SECTIONS))

    def test_release_yml_keeps_its_catch_all(self):
        # The `"*"` is what was blamed for everything falling through; removing
        # it must not pass unnoticed.
        self.assertIn('- "*"', (_ROOT / '.github' /
                                'release.yml').read_text(encoding='utf-8'))


class MergedNumbersTests(unittest.TestCase):
    """Reading pull-request numbers out of a commit range."""

    def test_both_merge_styles(self):
        recorder = Recorder(log='Merge pull request #1727 from pika/gh-1675\n'
                            'Fix the thing (#1700)\n')
        self.assertEqual(changelog.merged_numbers('1.4.0', git=recorder.git),
                         [1727, 1700])

    def test_the_range_is_passed_in_the_right_order(self):
        recorder = Recorder()
        changelog.merged_numbers('1.4.0', 'HEAD', git=recorder.git)
        self.assertEqual(recorder.call('log'),
                         ('log', '--format=%s', '1.4.0..HEAD'))

    def test_other_subjects_are_ignored(self):
        recorder = Recorder(log='pika 1.4.2\nMentions #1234 in passing\n')
        self.assertEqual(changelog.merged_numbers('1.4.0', git=recorder.git),
                         [])

    def test_a_number_is_reported_once(self):
        recorder = Recorder(log='Merge pull request #5 from a\n'
                            'Merge pull request #5 from a\n')
        self.assertEqual(changelog.merged_numbers('1.4.0', git=recorder.git),
                         [5])


class QueryTests(unittest.TestCase):
    """The queries themselves, asserted in full."""

    def test_the_milestone_issue_query(self):
        recorder = Recorder()
        changelog.milestone_issues('1.5.0', gh=recorder.gh)
        self.assertEqual(recorder.call('issue', 'list'),
                         ('issue', 'list', '--milestone', '1.5.0', '--state',
                          'closed', '--limit', str(changelog.LIMIT), '--json',
                          'number,title,labels,closedAt'))

    def test_the_milestone_pull_query(self):
        recorder = Recorder()
        changelog.milestone_pulls('1.5.0', gh=recorder.gh)
        self.assertEqual(recorder.call('pr', 'list'),
                         ('pr', 'list', '--state', 'merged', '--search',
                          'milestone:1.5.0', '--limit', str(changelog.LIMIT),
                          '--json', 'number,title,author,labels,mergedAt'))

    def test_the_window_query_for_the_cross_check(self):
        recorder = Recorder()
        changelog.merged_in_window('2026-05-06', gh=recorder.gh)
        self.assertEqual(recorder.call('pr', 'list'),
                         ('pr', 'list', '--state', 'merged', '--search',
                          'merged:>=2026-05-06', '--limit', str(
                              changelog.LIMIT), '--json', 'number,milestone'))


class LimitTests(unittest.TestCase):
    """A saturated `--limit` is refused, not silently truncated."""

    def test_a_saturated_answer_is_refused(self):
        rows = [{
            'number': n,
            'title': str(n),
            'labels': []
        } for n in range(changelog.LIMIT)]
        with self.assertRaises(changelog.ChangelogError) as caught:
            changelog.milestone_issues('1.5.0', gh=lambda *a: json.dumps(rows))
        self.assertIn(str(changelog.LIMIT), str(caught.exception))

    def test_one_short_of_it_is_fine(self):
        rows = [{
            'number': n,
            'title': str(n),
            'labels': []
        } for n in range(changelog.LIMIT - 1)]
        self.assertEqual(
            len(
                changelog.milestone_issues('1.5.0',
                                           gh=lambda *a: json.dumps(rows))),
            changelog.LIMIT - 1)


class DegenerateInputTests(unittest.TestCase):
    """What arrives when something is wrong."""

    def test_an_empty_answer_is_not_an_error(self):
        for answer in ('', '[]', '[]\n'):
            with self.subTest(answer=answer):
                self.assertEqual(
                    changelog.milestone_issues(
                        '1.5.0', gh=lambda *a, answer=answer: answer), [])

    def test_malformed_json_names_the_query(self):
        with self.assertRaises(changelog.ChangelogError) as caught:
            changelog.milestone_issues('1.5.0', gh=lambda *a: 'not json')
        self.assertIn('issue list', str(caught.exception))

    def test_a_failing_tool_raises_an_oserror(self):
        # `release.py main` catches OSError, so this reports as one line.
        self.assertTrue(issubclass(changelog.ChangelogError, OSError))


class CollectTests(unittest.TestCase):
    """The milestone decides membership; the range only reports disagreement."""

    ISSUES: ClassVar[list] = [
        {
            'number': 99,
            'title': 'in the milestone',
            'labels': [],
            'closedAt': '2026-06-01T00:00:00Z'
        },
        {
            'number': 50,
            'title': 'shipped earlier',
            'labels': [],
            'closedAt': '2026-05-05T00:00:00Z'
        },
    ]
    PULLS: ClassVar[list] = [
        {
            'number': 10,
            'title': 'ten',
            'labels': [],
            'author': {
                'login': 'a'
            },
            'mergedAt': '2026-06-01T00:00:00Z'
        },
    ]

    def _collect(self, log, window):
        """
        :param log: Commit subjects in the cross-check range.
        :param window: Maps a number to the milestone it names.
        :returns: `(issues, pulls, report)`.
        """
        recorder = Recorder(answers={
            ('issue', 'list'): self.ISSUES,
            ('pr', 'list'): self.PULLS,
        },
                            log=log)
        calls = {'n': 0}

        def gh(*args):
            # `pr list` serves both the milestone query and the window query.
            if args[:2] == ('pr', 'list') and 'merged:>=2026-05-06' in args:
                return json.dumps([{
                    'number': number,
                    'milestone': {
                        'title': named
                    } if named else None,
                } for number, named in window.items()])
            calls['n'] += 1
            return recorder.gh(*args)

        return changelog.collect('1.5.0', '1.4.0', git=recorder.git, gh=gh)

    def test_membership_comes_from_the_milestone(self):
        issues, pulls, _ = self._collect('', {})
        self.assertEqual([i['number'] for i in issues], [99, 50])
        self.assertEqual([p['number'] for p in pulls], [10])

    def test_a_pull_request_with_no_milestone_is_reported_not_included(self):
        _, pulls, report = self._collect('Merge pull request #11 from b\n',
                                         {11: ''})
        self.assertEqual(report['none'], [11])
        self.assertNotIn(11, [p['number'] for p in pulls])

    def test_a_different_milestone_is_reported_separately(self):
        # Conflating this with "no milestone" produced dangerous advice: moving
        # #1596 onto 1.5.0 would move work that shipped in 1.4.1.
        _, _, report = self._collect('Merge pull request #12 from c\n',
                                     {12: '1.4.1'})
        self.assertEqual(report['elsewhere'], [(12, '1.4.1')])
        self.assertEqual(report['none'], [])

    def test_something_finished_before_the_previous_tag_is_reported_stale(self):
        # #1558 is the live case: in milestone 1.5.0, closed the day before
        # 1.4.0 shipped, and already in the committed 1.4.0 entry.
        report = self._collect('', {})[2]
        self.assertEqual(report['stale'], [(50, '2026-05-05')])

    def test_a_number_that_is_not_a_merged_pull_request_is_ignored(self):
        # A `(#N)` subject naming an issue. An earlier version ran `gh pr view`
        # on it and aborted the whole operation; pika has 21 such subjects.
        _, _, report = self._collect('Fix a thing (#307)\n', {})
        self.assertEqual(report['none'], [])
        self.assertEqual(report['elsewhere'], [])


if __name__ == '__main__':
    unittest.main()
