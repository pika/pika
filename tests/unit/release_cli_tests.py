"""
Tests for the release command line in `.ci/release.py`.

The operations that touch the repository are driven through `--dry-run` or with the git layer
patched, because the point of this file is the argument handling and the refusals, not git. Two
things here are load-bearing beyond their size.

`classify` is the one operation a machine runs: `release.yaml` appends its stdout straight to
`$GITHUB_OUTPUT`, so a stray line or a half-written failure becomes a workflow variable holding
nonsense. `compute` serves both that workflow and a human picking the next version, so it is checked
with the full flag set the workflow sends and with the shorter one a person types.
"""

from __future__ import annotations

import contextlib
import importlib.util
import io
import pathlib
import unittest
from unittest import mock

_CI = pathlib.Path(__file__).resolve().parents[2] / '.ci'
_SPEC = importlib.util.spec_from_file_location('release', _CI / 'release.py')
assert _SPEC is not None and _SPEC.loader is not None
release = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(release)


def run(argv):
    """
    Run the command line and capture both streams.

    :param argv: Arguments after the program name.
    :returns:`(status, stdout, stderr)`.
    """
    out, err = io.StringIO(), io.StringIO()
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
        status = release.main(argv)
    return status, out.getvalue(), err.getvalue()


def _fixture_root(stack):
    """
    Point `release._ROOT` at a throwaway tree holding the files it reads.

    :param stack: An entered `ExitStack` owning the temporary directory.
    :returns: The root path.
    """
    import tempfile
    root = pathlib.Path(stack.enter_context(tempfile.TemporaryDirectory()))
    (root / 'pika').mkdir()
    (root / '.github').mkdir()
    (root / 'pyproject.toml').write_text('[project]\nversion = "1.5.0"\n')
    (root / 'pika' / '__init__.py').write_text("__version__ = '1.5.0'\n")
    (root / 'HISTORY.md').write_text('# Changelog\n\n## [1.4.4](x) (2026)\n')
    (root / release.PREAMBLE).write_text('')
    stack.enter_context(mock.patch.object(release, '_ROOT', root))
    return root


def _pr_command(version='1.5.0a1', milestone=None, passthrough=(), **overrides):
    """
    Return the `gh pr create` argument vector `cmd_pr` would run.

    :param version: What `pyproject.toml` declares.
    :param milestone:`--milestone`, or None for the derived default.
    :param passthrough: Arguments after `--`.
    :param overrides: Further attributes for the parsed namespace.
    :returns: The captured command, as a list.
    """
    captured = []

    def fake_run(command, dry_run=False):
        captured.append(list(command))
        return ''

    args = mock.Mock(dry_run=True,
                     label=None,
                     assignee=None,
                     milestone=milestone,
                     passthrough=passthrough,
                     **overrides)
    with contextlib.ExitStack() as stack:
        stack.enter_context(mock.patch.object(release, 'run', fake_run))
        stack.enter_context(
            mock.patch.object(release, 'current_branch',
                              lambda: f'pika-{version}'))
        stack.enter_context(
            mock.patch.object(release.tag_release, 'git', lambda *a, **k: ''))
        stack.enter_context(
            mock.patch.object(release.tag_release, 'version_in',
                              lambda *a: version))
        release.cmd_pr(args)
    return next(c for c in captured if c[:3] == ['gh', 'pr', 'create'])


def _flatten(command):
    """
    Return *command* as a flat list of strings.

    :param command: An argument vector, possibly nested.
    :returns: The flattened list.
    """
    out = []
    for item in command:
        out.extend(item) if isinstance(item, list) else out.append(item)
    return out


class ClassifyTests(unittest.TestCase):
    """The machine-facing operation, so its output shape is the contract."""

    def test_output_is_bare_github_output_lines(self):
        status, out, _ = run(['classify', '--tag', '1.5.0'])
        self.assertEqual(status, 0)
        self.assertEqual(out.splitlines(), [
            'mode=release',
            'version=1.5.0',
            'docs_version=1.5',
            'docs_aliases=latest',
            'docs_set_default=true',
        ])

    def test_a_prerelease_takes_no_alias(self):
        _, out, _ = run(['classify', '--tag', '1.5.0a1'])
        self.assertIn('docs_aliases=\n', out)
        self.assertIn('docs_set_default=false', out)

    def test_a_refused_tag_writes_nothing_to_stdout(self):
        # A partial write would land half the parameters in `$GITHUB_OUTPUT`
        # and the step would carry on with the rest defaulted to empty.
        status, out, err = run(['classify', '--tag', 'v1.5.0'])
        self.assertEqual(status, 1)
        self.assertEqual(out, '')
        self.assertIn('error:', err)


class ComputeTests(unittest.TestCase):
    """One operation, two callers: `release.yaml` and a person."""

    def test_the_workflows_full_invocation(self):
        status, out, _ = run([
            'compute', '--current', '1.4.0', '--bump', 'minor', '--mode',
            'test', '--run-number', '99'
        ])
        self.assertEqual(status, 0)
        self.assertEqual(out.strip(), '1.5.0.dev99')

    def test_mode_is_inferred_from_a_prerelease_tag(self):
        with mock.patch.object(release.tag_release, 'version_in',
                               lambda *a: '1.4.0'):
            status, out, _ = run(
                ['compute', '--bump', 'minor', '--prerelease-tag', 'a1'])
        self.assertEqual(status, 0)
        self.assertEqual(out.strip(), '1.5.0a1')

    def test_current_defaults_to_the_file(self):
        with mock.patch.object(release.tag_release, 'version_in',
                               lambda *a: '1.5.0rc1'):
            _, out, _ = run(['compute', '--bump', 'none'])
        self.assertEqual(out.strip(), '1.5.0')

    def test_a_bad_combination_fails_with_empty_stdout(self):
        status, out, err = run([
            'compute', '--current', '1.4.0', '--bump', 'minor', '--mode',
            'dry-run', '--prerelease-tag', 'a1'
        ])
        self.assertEqual(status, 1)
        self.assertEqual(out, '')
        self.assertIn('prerelease', err)

    def test_bump_is_required(self):
        status, _, err = run(['compute', '--current', '1.4.0'])
        self.assertEqual(status, 1)
        self.assertIn('--bump', err)


class PassthroughTests(unittest.TestCase):
    """`--` forwards to the tool an operation drives."""

    def test_arguments_after_the_separator_are_collected(self):
        captured = {}

        def fake(args):
            captured['passthrough'] = args.passthrough
            return 0

        with mock.patch.object(release, 'cmd_classify', fake):
            release.main(['classify', '--tag', '1.5.0', '--', '--draft'])
        self.assertEqual(captured['passthrough'], ('--draft',))

    def test_no_separator_means_no_passthrough(self):
        captured = {}

        def fake(args):
            captured['passthrough'] = args.passthrough
            return 0

        with mock.patch.object(release, 'cmd_classify', fake):
            release.main(['classify', '--tag', '1.5.0'])
        self.assertEqual(captured['passthrough'], ())


class PullRequestDefaultsTests(unittest.TestCase):
    """What `pr` labels, assigns and milestones, and how overrides behave."""

    def _args(self, argv):
        """
        Parse a `pr` invocation.

        :param argv: Arguments after the operation name.
        :returns: The parsed namespace.
        """
        return release.build_parser().parse_args(['pr', *argv])

    def test_the_defaults_are_absent_from_the_namespace(self):
        # `action='append'` appends to a default instead of replacing it, so the
        # defaults cannot live in the parser: `--label C-bug` would have meant
        # `A-packaging` and `C-bug`, with no way to drop the first.
        args = self._args([])
        self.assertIsNone(args.label)
        self.assertIsNone(args.assignee)
        self.assertIsNone(args.milestone)

    def test_an_override_replaces_rather_than_appends(self):
        args = self._args(['--label', 'C-bug', '--assignee', 'michaelklishin'])
        self.assertEqual(args.label, ['C-bug'])
        self.assertEqual(args.assignee, ['michaelklishin'])

    def test_a_prerelease_takes_the_milestone_of_the_version_it_leads_to(self):
        # Asserted on the command `gh` would receive, not on a re-derivation of
        # the same f-string, which would pass however `cmd_pr` behaved.
        command = _pr_command(version='1.5.0a1')
        self.assertEqual(command[command.index('--milestone') + 1], '1.5.0')

    def test_the_milestone_can_be_opted_out_of(self):
        self.assertNotIn('--milestone', _flatten(_pr_command(milestone='')))

    def test_passthrough_precedes_the_flags_the_operation_owns(self):
        # Both `gh` and the changelog generator take the last occurrence of a
        # repeated flag, so appending passthrough would let a caller retarget
        # `--base` or rewrite `--title`.
        flat = _flatten(_pr_command(passthrough=('--base', '1.4.x')))
        self.assertLess(flat.index('1.4.x'), flat.index('main'))

    def test_the_defaults_are_what_agents_md_asks_for(self):
        self.assertEqual(release.DEFAULT_LABELS, ('A-packaging',))
        self.assertEqual(release.DEFAULT_ASSIGNEES, ('lukebakken',))


class DryRunTests(unittest.TestCase):
    """Which operations can rehearse, and which have nothing to rehearse."""

    def test_the_mutating_operations_offer_a_dry_run(self):
        for operation in ('bump', 'pr', 'tag', 'changelog'):
            with self.subTest(operation=operation):
                args = release.build_parser().parse_args(
                    [operation, '--dry-run'])
                self.assertTrue(args.dry_run)

    def test_the_read_only_operations_do_not(self):
        # Documented as such, so the claim is checked rather than repeated.
        for operation, extra in (('compute', []), ('classify',
                                                   ['--tag',
                                                    '1.5.0']), ('check', [])):
            with contextlib.ExitStack() as stack:
                stack.enter_context(self.subTest(operation=operation))
                stack.enter_context(self.assertRaises(SystemExit))
                release.build_parser().parse_args(
                    [operation, *extra, '--dry-run'])


class PreviousReleaseTests(unittest.TestCase):
    """Which tag a changelog is generated since."""

    TAGS = ('v0.9.5\n0.10.0\n1.3.1\n1.4.0\n1.4.1\n1.4.2\n1.4.3\n1.4.4\n'
            '1.4.0b0\n')

    def test_the_newest_by_version_not_by_reachability(self):
        # `git describe` would answer 1.4.0 on `main`, because 1.4.1 through
        # 1.4.4 were cut from `1.4.x` and are not ancestors of it.
        with mock.patch.object(release.tag_release, 'git',
                               lambda *a, **k: self.TAGS):
            self.assertEqual(release.previous_release(), '1.4.4')

    def test_tags_from_before_this_scheme_are_skipped(self):
        with contextlib.ExitStack() as stack:
            stack.enter_context(
                mock.patch.object(release.tag_release, 'git',
                                  lambda *a, **k: 'v0.9.5\nv0.9.4\n'))
            stack.enter_context(self.assertRaises(release.CheckFailed))
            release.previous_release()

    def test_a_prerelease_does_not_outrank_its_release(self):
        with mock.patch.object(release.tag_release, 'git',
                               lambda *a, **k: '1.5.0\n1.5.0rc1\n1.5.0a1\n'):
            self.assertEqual(release.previous_release(), '1.5.0')

    def test_no_tags_at_all_is_an_error_rather_than_an_empty_flag(self):
        with contextlib.ExitStack() as stack:
            stack.enter_context(
                mock.patch.object(release.tag_release, 'git',
                                  lambda *a, **k: ''))
            stack.enter_context(self.assertRaises(release.CheckFailed))
            release.previous_release()


class BumpTests(unittest.TestCase):
    """What `bump` refuses, and the order it refuses in."""

    def test_a_backwards_version_is_refused(self):
        # `--bump none --prerelease-tag b1` off 1.4.0 yields 1.4.0b1, which sorts
        # below the published 1.4.4. Nothing else catches it.
        with mock.patch.object(release, 'previous_release', lambda: '1.4.4'):
            status, _, err = run(['bump', '--version', '1.4.0b1', '--dry-run'])
        self.assertEqual(status, 1)
        self.assertIn('does not move past 1.4.4', err)

    def test_the_branch_is_not_created_before_the_files_are_validated(self):
        # `write_version` used to raise after `checkout -b`, leaving a branch for
        # the next run to refuse.
        commands = []
        with contextlib.ExitStack() as stack:
            root = _fixture_root(stack)
            (root / 'pyproject.toml').write_text('[project]\nname = "pika"\n')
            stack.enter_context(
                mock.patch.object(release,
                                  'run',
                                  lambda c, dry_run=False: commands.append(c)))
            stack.enter_context(
                mock.patch.object(release, 'previous_release', lambda: '1.0.0'))
            stack.enter_context(
                mock.patch.object(release, 'require_clean_main', lambda: None))
            stack.enter_context(
                mock.patch.object(release.tag_release, 'git',
                                  lambda *a, **k: ''))
            status, _, err = run(['bump', '--version', '1.5.0'])
        self.assertEqual(status, 1)
        self.assertIn('does not declare a version', err)
        self.assertEqual(commands, [])


class ReadinessTests(unittest.TestCase):
    """What `check` and `tag` both refuse."""

    def test_both_version_files_are_reported(self):
        # The friendlier copy of this loop dropped half of it, so a wrong
        # `pyproject.toml` was reported only as a disagreement in `__init__.py`.
        with contextlib.ExitStack() as stack:
            _fixture_root(stack)
            stack.enter_context(
                mock.patch.object(release, 'previous_release', lambda: '1.0.0'))
            stack.enter_context(
                mock.patch.object(release.tag_release, 'on_pypi',
                                  lambda v: False))
            problems = release.readiness('1.9.9')
        self.assertTrue(any('pyproject.toml' in p for p in problems), problems)
        # The rendered path, not a hardcoded `/`: Windows reports
        # `pika\\__init__.py`.
        init = str(pathlib.Path('pika/__init__.py'))
        self.assertTrue(any(init in p for p in problems), problems)

    def test_a_ready_version_has_no_problems(self):
        with contextlib.ExitStack() as stack:
            root = _fixture_root(stack)
            (root /
             'HISTORY.md').write_text('# Changelog\n\n## [1.5.0](x) (2026)\n')
            stack.enter_context(
                mock.patch.object(release, 'previous_release', lambda: '1.4.4'))
            stack.enter_context(
                mock.patch.object(release.tag_release, 'on_pypi',
                                  lambda v: False))
            self.assertEqual(release.readiness('1.5.0'), [])


class ResolveVersionTests(unittest.TestCase):
    """How an operation decides which version it is acting on."""

    def test_version_and_bump_are_alternatives(self):
        args = mock.Mock(version='1.5.0', bump='minor', prerelease_tag='')
        with self.assertRaises(release.CheckFailed):
            release.resolve_version(args)

    def test_neither_is_refused(self):
        args = mock.Mock(version='', bump='', prerelease_tag='')
        with self.assertRaises(release.CheckFailed):
            release.resolve_version(args)

    def test_an_explicit_version_is_taken_as_given(self):
        args = mock.Mock(version='1.5.0b2', bump='', prerelease_tag='')
        self.assertEqual(release.resolve_version(args), '1.5.0b2')


class WriteVersionTests(unittest.TestCase):
    """Writing the version into the two files that declare it."""

    def _tree(self, stack, pyproject, init):
        """
        Build a repository root holding the two version files.

        :param stack: An entered `ExitStack` owning the temporary directory.
        :param pyproject: Contents of `pyproject.toml`.
        :param init: Contents of `pika/__init__.py`.
        :returns: The root path.
        """
        import tempfile
        root = pathlib.Path(stack.enter_context(tempfile.TemporaryDirectory()))
        (root / 'pika').mkdir()
        (root / 'pyproject.toml').write_text(pyproject)
        (root / 'pika' / '__init__.py').write_text(init)
        stack.enter_context(mock.patch.object(release, '_ROOT', root))
        return root

    def test_both_files_are_rewritten(self):
        with contextlib.ExitStack() as stack:
            root = self._tree(stack, '[project]\nversion = "1.4.0"\n',
                              "__version__ = '1.4.0'\nX = 1\n")
            with contextlib.redirect_stdout(io.StringIO()):
                release.write_version('1.5.0a1')
            self.assertIn('version = "1.5.0a1"',
                          (root / 'pyproject.toml').read_text())
            self.assertIn("__version__ = '1.5.0a1'",
                          (root / 'pika' / '__init__.py').read_text())
            # Only the version line; the rest of the module is untouched.
            self.assertIn('X = 1', (root / 'pika' / '__init__.py').read_text())

    def test_a_dry_run_writes_nothing(self):
        with contextlib.ExitStack() as stack:
            root = self._tree(stack, '[project]\nversion = "1.4.0"\n',
                              "__version__ = '1.4.0'\n")
            with contextlib.redirect_stdout(io.StringIO()):
                release.write_version('1.5.0a1', dry_run=True)
            self.assertIn('version = "1.4.0"',
                          (root / 'pyproject.toml').read_text())

    def test_a_file_with_no_version_is_an_error(self):
        with contextlib.ExitStack() as stack:
            self._tree(stack, '[project]\nname = "pika"\n',
                       "__version__ = '1.4.0'\n")
            with self.assertRaises(release.CheckFailed):
                release.write_version('1.5.0a1')


class NotesAgreeTests(unittest.TestCase):
    """`HISTORY.md` and the release-notes preamble are a pair."""

    HISTORY = ('# Changelog\n\n## Upgrading to 1.5.0\n\n'
               '### A thing\n\nIt changed.\n\n'
               '## [1.4.0](https://example.invalid) (2026-05-06)\n')
    PREAMBLE = '## Upgrading to 1.5.0\n\n### A thing\n\nIt changed.\n'

    def _files(self, stack, history, preamble):
        """
        Build a root holding the two documents.

        :param stack: An entered `ExitStack` owning the temporary directory.
        :param history: Contents of `HISTORY.md`.
        :param preamble: Contents of the preamble.
        :returns: The root path.
        """
        import tempfile
        root = pathlib.Path(stack.enter_context(tempfile.TemporaryDirectory()))
        (root / '.github').mkdir()
        (root / 'HISTORY.md').write_text(history)
        (root / release.PREAMBLE).write_text(preamble)
        stack.enter_context(mock.patch.object(release, '_ROOT', root))
        return root

    def test_identical_notes_agree(self):
        with contextlib.ExitStack() as stack:
            self._files(stack, self.HISTORY, self.PREAMBLE)
            self.assertEqual(release.notes_agree(), '')

    def test_the_preamble_may_add_a_trailer(self):
        # It does: it ends by pointing at the changelog, which would be
        # self-referential in the changelog itself.
        with contextlib.ExitStack() as stack:
            self._files(stack, self.HISTORY,
                        self.PREAMBLE + '\nAlso see the changelog.\n')
            self.assertEqual(release.notes_agree(), '')

    def test_a_divergence_is_reported_with_its_position(self):
        with contextlib.ExitStack() as stack:
            self._files(stack, self.HISTORY,
                        self.PREAMBLE.replace('It changed.', 'It altered.'))
            divergence = release.notes_agree()
        self.assertIn('line 5', divergence)
        self.assertIn('altered', divergence)

    def test_the_intro_paragraph_is_compared_too(self):
        # Anchoring on the first `### ` skipped the paragraph under
        # `## Upgrading`, so a reworded intro passed.
        with contextlib.ExitStack() as stack:
            self._files(stack, self.HISTORY,
                        self.PREAMBLE.replace('### A thing', '### Another'))
            self.assertIn('diverge', release.notes_agree())

    def test_a_truncated_preamble_is_reported(self):
        with contextlib.ExitStack() as stack:
            self._files(stack, self.HISTORY,
                        '## Upgrading to 1.5.0\n\n### A thing\n')
            self.assertIn('truncated', release.notes_agree())

    def test_a_preamble_without_the_heading_says_so(self):
        with contextlib.ExitStack() as stack:
            self._files(stack, self.HISTORY, '### A thing\n\nIt changed.\n')
            self.assertIn('no', release.notes_agree())

    def test_an_absent_preamble_agrees_vacuously(self):
        # `release.yaml` treats absent as "generated notes only", and RELEASE.md
        # says to delete the file when a release needs no guidance.
        with contextlib.ExitStack() as stack:
            root = self._files(stack, self.HISTORY, self.PREAMBLE)
            (root / release.PREAMBLE).unlink()
            self.assertEqual(release.notes_agree(), '')

    def test_an_empty_preamble_agrees_vacuously(self):
        with contextlib.ExitStack() as stack:
            self._files(stack, self.HISTORY, '')
            self.assertEqual(release.notes_agree(), '')

    def test_a_crlf_preamble_is_not_reported_as_drift(self):
        # `splitlines` collapses CRLF where `startswith` did not, which reported
        # a drift that was only a line ending, with a negative count.
        with contextlib.ExitStack() as stack:
            root = self._files(stack, self.HISTORY, self.PREAMBLE)
            # Bytes, because `write_text` translates newlines and turned an
            # explicit CRLF into CRCRLF on Windows.
            (root / release.PREAMBLE).write_bytes(
                self.PREAMBLE.replace('\n', '\r\n').encode('utf-8'))
            self.assertEqual(release.notes_agree(), '')


if __name__ == '__main__':
    unittest.main()
