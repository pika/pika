"""
Tests for the changelog generator in `.ci/changelog.py`.

The GitHub queries are injectable, so these never reach the network. What they are mostly about is
the rendered format: the entry is prepended above a thousand lines of released history and has to be
indistinguishable from it, down to the escaped `\\#` and the trailing author link. A format drift
here is not a crash, it is a changelog that looks wrong forever.

The other thing checked here is that `SECTIONS` and `.github/release.yml` name the same labels.
`release.yml` listed six labels pika does not have, so every pull request fell through its catch-all
and the generated release notes were never categorised. Two files deciding the same thing from the
same labels is a drift risk, so the agreement is asserted rather than assumed.
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


def fake_gh(issues=(), pulls=(), views=None):
    """
    Return a `gh` stand-in answering the three queries the generator makes.

    :param issues: What `issue list` returns.
    :param pulls: What `pr list` returns.
    :param views: Maps a number to what `pr view` returns.
    :returns: A callable with `_gh`'s signature.
    """

    def run(*args):
        if args[:2] == ('issue', 'list'):
            return json.dumps(list(issues))
        if args[:2] == ('pr', 'list'):
            return json.dumps(list(pulls))
        if args[:2] == ('pr', 'view'):
            return json.dumps((views or {})[int(args[2])])
        raise AssertionError(f'unexpected gh {args}')

    return run


def fake_git(subjects=()):
    """
    Return a git stand-in answering `log --format=%s`.

    :param subjects: Commit subjects, newest first.
    :returns: A callable with `_git`'s signature.
    """
    return lambda *args: '\n'.join(subjects) + '\n' if subjects else ''


class SectionTests(unittest.TestCase):
    """Which heading a set of labels lands under."""

    def test_the_three_mapped_labels(self):
        self.assertEqual(changelog.section_for(['C-enhancement']),
                         '**Implemented enhancements:**')
        self.assertEqual(changelog.section_for(['C-bug']), '**Fixed bugs:**')
        self.assertEqual(changelog.section_for(['A-documentation']),
                         '**Documentation:**')

    def test_everything_else_is_a_closed_issue(self):
        # Deliberate: the entries this has to sit above only ever had two
        # sections, so `C-refactor` and the rest do not get their own.
        for labels in (['C-refactor'], ['C-performance'], ['dependencies'],
                       ['github_actions'], ['A-typing'], []):
            with self.subTest(labels=labels):
                self.assertEqual(changelog.section_for(labels),
                                 '**Closed issues:**')

    def test_the_first_mapped_label_wins(self):
        # An issue labelled both takes the earlier section, so the output is
        # deterministic rather than dependent on label order.
        self.assertEqual(changelog.section_for(['C-bug', 'C-enhancement']),
                         '**Implemented enhancements:**')

    def test_release_yml_names_the_same_labels(self):
        # Parsed with a regex rather than `yaml`, which is not a test
        # dependency and is not worth becoming one to compare two files. A
        # label entry is a bare scalar list item; `- title: "..."` has a space
        # after its colon, so it does not match.
        text = (_ROOT / '.github' / 'release.yml').read_text(encoding='utf-8')
        from_yaml = {
            label for label in re.findall(r'^\s+- (\S+)$', text, re.MULTILINE)
            if label != '"*"'
        }
        self.assertEqual({label for label, _ in changelog.SECTIONS}, from_yaml)

    def test_every_label_it_names_is_namespaced(self):
        # `release.yml` listed `enhancement`, `bug` and `documentation`, which
        # pika does not have; the namespace prefix is the tell.
        for label, _ in changelog.SECTIONS:
            with self.subTest(label=label):
                self.assertRegex(label, r'^[CA]-')


class RenderTests(unittest.TestCase):
    """The format, which has to match a thousand lines of released history."""

    ISSUE: ClassVar[dict] = {
        'number': 1639,
        'title': 'Importing pika.adapters can break asyncio subprocesses',
        'labels': [],
    }
    PULL: ClassVar[dict] = {
        'number': 1642,
        'title': 'Stop mutating global asyncio event loop policy on import',
        'author': 'lukebakken',
    }

    def test_it_reproduces_a_released_entry_exactly(self):
        entry = changelog.render('1.4.2',
                                 '1.4.1', [self.ISSUE], [self.PULL],
                                 released=datetime.date(2026, 7, 23))
        self.assertEqual(
            entry, '## [1.4.2](https://github.com/pika/pika/tree/1.4.2) '
            '(2026-07-23)\n'
            '\n'
            '[Full Changelog](https://github.com/pika/pika/compare/'
            '1.4.1...1.4.2)\n'
            '\n'
            '**Closed issues:**\n'
            '\n'
            '- Importing pika.adapters can break asyncio subprocesses '
            '[\\#1639](https://github.com/pika/pika/issues/1639)\n'
            '\n'
            '**Merged pull requests:**\n'
            '\n'
            '- Stop mutating global asyncio event loop policy on import '
            '[\\#1642](https://github.com/pika/pika/pull/1642) '
            '([lukebakken](https://github.com/lukebakken))\n')

    def test_the_heading_names_the_version_being_cut(self):
        # `github_changelog_generator` wrote the *previous* tag here, which is
        # why the committed 1.4.3 entry links to `tree/1.4.2`.
        entry = changelog.render('1.4.3', '1.4.2', [], [])
        self.assertIn('## [1.4.3](https://github.com/pika/pika/tree/1.4.3)',
                      entry)
        self.assertIn('compare/1.4.2...1.4.3', entry)

    def test_sections_appear_in_a_fixed_order(self):
        issues = [
            {
                'number': 3,
                'title': 'c',
                'labels': ['A-documentation']
            },
            {
                'number': 1,
                'title': 'a',
                'labels': ['C-bug']
            },
            {
                'number': 2,
                'title': 'b',
                'labels': ['C-enhancement']
            },
            {
                'number': 4,
                'title': 'd',
                'labels': ['C-refactor']
            },
        ]
        headings = [
            line for line in changelog.render('9.9.9', '9.9.8', issues,
                                              []).splitlines()
            if line.startswith('**')
        ]
        self.assertEqual(headings, [
            '**Implemented enhancements:**',
            '**Fixed bugs:**',
            '**Documentation:**',
            '**Closed issues:**',
        ])

    def test_entries_are_sorted_by_number_within_a_section(self):
        issues = [{
            'number': n,
            'title': str(n),
            'labels': ['C-bug']
        } for n in (30, 10, 20)]
        numbers = [
            int(line.split('\\#')[1].split(']')[0])
            for line in changelog.render('9.9.9', '9.9.8', issues,
                                         []).splitlines()
            if line.startswith('- ')
        ]
        self.assertEqual(numbers, [10, 20, 30])

    def test_an_empty_section_is_omitted(self):
        entry = changelog.render('9.9.9', '9.9.8', [], [])
        self.assertNotIn('**', entry)
        self.assertTrue(entry.endswith('\n'))


class MergedNumbersTests(unittest.TestCase):
    """Reading pull-request numbers out of a commit range."""

    def test_merge_commits(self):
        self.assertEqual(
            changelog.merged_numbers(
                '1.4.0',
                git=fake_git([
                    'Merge pull request #1727 from pika/gh-1675',
                    'Merge pull request #1726 from pika/fix'
                ])), [1727, 1726])

    def test_squashed_commits(self):
        # pika squash-merges nothing today, but a future squash should not
        # vanish from a release's changelog.
        self.assertEqual(
            changelog.merged_numbers('1.4.0',
                                     git=fake_git(['Fix the thing (#1700)'])),
            [1700])

    def test_other_subjects_are_ignored(self):
        self.assertEqual(
            changelog.merged_numbers('1.4.0',
                                     git=fake_git([
                                         'pika 1.4.2',
                                         'Revert "something (#1)" badly',
                                         'Mentions #1234 in passing'
                                     ])), [])

    def test_a_number_is_reported_once(self):
        self.assertEqual(
            changelog.merged_numbers('1.4.0',
                                     git=fake_git([
                                         'Merge pull request #5 from a',
                                         'Merge pull request #5 from a'
                                     ])), [5])


class UnmilestonedTests(unittest.TestCase):
    """Pull requests the milestone does not carry are found and included."""

    def test_only_the_ones_the_milestone_lacks(self):
        strays = changelog.unmilestoned(
            '1.4.0', {7},
            git=fake_git([
                'Merge pull request #7 from a', 'Merge pull request #9 from b'
            ]),
            gh=fake_gh(views={
                9: {
                    'number': 9,
                    'title': 'stray',
                    'author': {
                        'login': 'someone'
                    },
                }
            }))
        self.assertEqual(strays, [{
            'number': 9,
            'title': 'stray',
            'author': 'someone'
        }])

    def test_generate_reports_them_and_still_includes_them(self):
        notes = []
        entry = changelog.generate(
            '1.5.0',
            '1.5.0',
            '1.4.0',
            git=fake_git(['Merge pull request #9 from b']),
            gh=fake_gh(views={
                9: {
                    'number': 9,
                    'title': 'stray',
                    'author': {
                        'login': 'someone'
                    },
                }
            }),
            report=notes.append)
        self.assertIn('carry no 1.5.0 milestone', notes[0])
        self.assertIn('#9 stray', notes[1])
        self.assertIn('[\\#9](https://github.com/pika/pika/pull/9)', entry)


class GenerateTests(unittest.TestCase):
    """The milestone and the version are not the same thing."""

    def test_a_prerelease_draws_on_the_base_milestone_but_names_itself(self):
        entry = changelog.generate('1.5.0',
                                   '1.5.0a1',
                                   '1.4.0',
                                   git=fake_git(),
                                   gh=fake_gh(issues=[{
                                       'number': 1,
                                       'title': 'a thing',
                                       'labels': [{
                                           'name': 'C-bug'
                                       }],
                                   }]),
                                   report=lambda message: None)
        self.assertIn('## [1.5.0a1]', entry)
        self.assertIn('tree/1.5.0a1', entry)
        self.assertIn('compare/1.4.0...1.5.0a1', entry)
        self.assertIn('**Fixed bugs:**', entry)


if __name__ == '__main__':
    unittest.main()
