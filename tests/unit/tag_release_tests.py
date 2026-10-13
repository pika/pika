"""
Tests for the release tagger in `.ci/tag_release.py`.

Pushing a tag is what publishes, so the value of this module is in the refusals: every check exists
because the mistake it catches is either unrecoverable on PyPI or produces a tag that names the
wrong commit. The ordering is part of the contract too, since the cheap local checks must fail
before the one that needs the network.
"""

import contextlib
import importlib.util
import pathlib
import unittest
from unittest import mock

# `.ci` is not a package and its name is not a valid identifier, so the module
# is loaded by path rather than imported.
_CI = pathlib.Path(__file__).resolve().parents[2] / '.ci'
_SPEC = importlib.util.spec_from_file_location('tag_release',
                                               _CI / 'tag_release.py')
assert _SPEC is not None and _SPEC.loader is not None
tag_release = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(tag_release)

KEY = 'B1B82CC0CF84BA70147EBD05D99DE30E43EAE440'


class TagCommandTests(unittest.TestCase):
    """The invocation that creates the tag, spelled out rather than pasted."""

    def test_the_exact_argument_vector(self):
        self.assertEqual(tag_release.tag_command('1.5.0a1', KEY), (
            'git',
            'tag',
            '--annotate',
            '--sign',
            f'--local-user={KEY}',
            '--message=pika 1.5.0a1',
            '1.5.0a1',
        ))

    def test_annotated_and_signed_are_not_optional(self):
        # `--follow-tags` and `git describe` both ignore lightweight tags, and
        # the published tags are signed.
        command = tag_release.tag_command('1.5.0', KEY)
        self.assertIn('--annotate', command)
        self.assertIn('--sign', command)

    def test_the_message_carries_no_shell_quoting(self):
        # Passed as one argv element to `subprocess`, never through a shell, so
        # embedded quotes would end up in the tag message itself.
        command = tag_release.tag_command('1.5.0', KEY)
        self.assertEqual(command[-2], '--message=pika 1.5.0')


class VersionInTests(unittest.TestCase):
    """Reading the version out of the two files that declare it."""

    def test_pyproject_and_init_patterns(self):
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            (root / 'pyproject.toml'
            ).write_text('[project]\nname = "pika"\nversion = "1.5.0a1"\n')
            (root / 'init.py').write_text("__version__ = '1.5.0a1'\n")
            with mock.patch.object(tag_release, '_ROOT', root):
                self.assertEqual(
                    tag_release.version_in(pathlib.Path('pyproject.toml'),
                                           tag_release.PYPROJECT_VERSION),
                    '1.5.0a1')
                self.assertEqual(
                    tag_release.version_in(pathlib.Path('init.py'),
                                           tag_release.INIT_VERSION), '1.5.0a1')

    def test_a_file_with_no_version_is_an_error(self):
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            (root / 'pyproject.toml').write_text('[project]\nname = "pika"\n')
            with mock.patch.object(tag_release, '_ROOT',
                                   root), self.assertRaises(
                                       tag_release.CheckFailed):
                tag_release.version_in(pathlib.Path('pyproject.toml'),
                                       tag_release.PYPROJECT_VERSION)


class CheckTests(unittest.TestCase):
    """Every reason to refuse, and the order they are refused in."""

    HEAD = 'a' * 40

    def _git(self, responses):
        """
        Return a `git` stand-in answering by first argument.

        :param responses: Maps the git subcommand to its stdout.
        :returns: A callable with `git`'s signature.
        """

        def fake(*args, check=True):
            for key, value in responses.items():
                if args[:len(key)] == key:
                    return value
            return ''

        return fake

    @contextlib.contextmanager
    def _patched(self, responses, version='1.5.0a1', on_pypi=False):
        """
        Patch git, the version files and PyPI for one call to `check`.

        An `ExitStack` rather than several `with` clauses so no test needs a line
        continuation: `with a, b:` cannot be wrapped in parentheses until Python
        3.9 and pika still supports 3.7, and a backslash continuation is
        reformatted differently by `docformatter` depending on the Python it runs
        under, which turned into a green local run and a red CI one.

        :param responses: Maps a git subcommand to its stdout.
        :param version: What both version files declare.
        :param on_pypi: Whether PyPI already holds the version.
        :yields: Nothing; the patches are active for the block.
        """
        with contextlib.ExitStack() as stack:
            stack.enter_context(
                mock.patch.object(tag_release, 'git', self._git(responses)))
            stack.enter_context(
                mock.patch.object(tag_release, 'version_in',
                                  lambda *a: version))
            stack.enter_context(
                mock.patch.object(tag_release, 'on_pypi', lambda v: on_pypi))
            yield

    @staticmethod
    def _never_called(*args, **kwargs):
        """
        Fail if consulted.

        :raises AssertionError: always.
        """
        raise AssertionError('should not have been consulted')

    def _clean_responses(self, overrides=None):
        """
        Return git answers for a repository that passes every check.

        Taken as a mapping rather than keyword arguments because the keys are tuples of git
        arguments, which `**` cannot express.

        :param overrides: Replace or add individual answers.
        :returns: Maps a git subcommand to its stdout.
        """
        responses = {
            ('rev-parse', '--abbrev-ref', 'HEAD'): 'main',
            ('status', '--porcelain'): '',
            ('rev-parse', 'HEAD'): self.HEAD,
            ('rev-parse', 'origin/main'): self.HEAD,
            ('tag', '--list'): '',
            ('ls-remote',): '',
        }
        responses.update(overrides or {})
        return responses

    def _refuses(self, responses, tag='1.5.0a1', **patches):
        """
        Assert `check` refuses, and return the reason it gave.

        An `ExitStack` keeps this to one `with`, which is what lets each caller below be a single
        statement.

        :param responses: Maps a git subcommand to its stdout.
        :param tag: The version passed to `check`.
        :param patches: Forwarded to `_patched`.
        :returns: The exception message.
        """
        with contextlib.ExitStack() as stack:
            stack.enter_context(self._patched(responses, **patches))
            caught = stack.enter_context(
                self.assertRaises(tag_release.CheckFailed))
            tag_release.check(tag)
        return str(caught.exception)

    def test_a_clean_repository_passes_and_returns_the_implied_parameters(self):
        with self._patched(self._clean_responses()):
            implied = tag_release.check('1.5.0a1')
        self.assertEqual(implied['mode'], 'prerelease')
        self.assertEqual(implied['docs_aliases'], '')

    def test_an_unreleasable_version_is_refused_before_touching_git(self):
        # First check in the function: it needs no repository state, and git
        # calls would be wasted on a version that can never be published.
        with contextlib.ExitStack() as stack:
            stack.enter_context(
                mock.patch.object(tag_release, 'git', self._never_called))
            stack.enter_context(self.assertRaises(tag_release.CheckFailed))
            tag_release.check('v1.5.0')

    def test_wrong_branch(self):
        overrides = {('rev-parse', '--abbrev-ref', 'HEAD'): 'gh-1728'}
        self.assertIn('main', self._refuses(self._clean_responses(overrides)))

    def test_dirty_tree(self):
        overrides = {('status', '--porcelain'): ' M pika/channel.py'}
        self.assertIn('clean', self._refuses(self._clean_responses(overrides)))

    def test_head_behind_the_remote(self):
        overrides = {('rev-parse', 'origin/main'): 'b' * 40}
        self.assertIn('pull', self._refuses(self._clean_responses(overrides)))

    def test_version_files_disagree_with_the_tag(self):
        self.assertIn('1.4.0',
                      self._refuses(self._clean_responses(), version='1.4.0'))

    def test_tag_already_exists_locally(self):
        overrides = {('tag', '--list'): '1.5.0a1'}
        self.assertIn('locally',
                      self._refuses(self._clean_responses(overrides)))

    def test_tag_already_exists_on_origin(self):
        overrides = {('ls-remote',): f'{self.HEAD}\trefs/tags/1.5.0a1'}
        self.assertIn('origin', self._refuses(self._clean_responses(overrides)))

    def test_version_already_on_pypi(self):
        self.assertIn('immutable',
                      self._refuses(self._clean_responses(), on_pypi=True))

    def test_pypi_can_be_skipped(self):
        with self._patched(self._clean_responses()), mock.patch.object(
                tag_release, 'on_pypi', self._never_called):
            tag_release.check('1.5.0a1', verify_pypi=False)


class SigningKeyTests(unittest.TestCase):
    """The tags are signed, so a missing key is an error rather than a warning."""

    def test_the_override_wins(self):
        with mock.patch.object(tag_release, 'git', lambda *a, **k: 'ignored'):
            self.assertEqual(tag_release.signing_key(KEY), KEY)

    def test_git_configuration_is_the_default(self):
        with mock.patch.object(tag_release, 'git', lambda *a, **k: KEY):
            self.assertEqual(tag_release.signing_key(''), KEY)

    def test_no_key_anywhere_is_refused(self):
        with mock.patch.object(tag_release, 'git',
                               lambda *a, **k: ''), self.assertRaises(
                                   tag_release.CheckFailed):
            tag_release.signing_key('')


class VerifyTagTests(unittest.TestCase):
    """
    The other side of the push: a tag that already exists and may not have come from `tag`.

    `check` guards the maintainer's machine, where failing is free. These guard the workflow, which
    is the last place anything can refuse before `twine` makes it permanent. Nothing obliges a
    maintainer to create the tag with `release.py tag`, so a hand-rolled `git tag -s` has to meet
    the same bar.
    """

    SHA = 'b' * 40
    SIGNED = (
        'object ' + 'c' * 40 + '\ntype commit\ntag 1.5.0\n\npika 1.5.0\n'
        '-----BEGIN PGP SIGNATURE-----\nabc\n-----END PGP SIGNATURE-----\n')

    def _git(self, responses):
        """
        Return a `git` stand-in answering by leading arguments.

        A response may be an exception instance, which is raised instead of returned: that is how
        `merge-base --is-ancestor` reports "no", and the refusal depends on it.

        :param responses: Maps the git subcommand to its stdout, or to an exception.
        :returns: A callable with `git`'s signature.
        """

        def fake(*args, check=True):
            for key, value in responses.items():
                if args[:len(key)] == key:
                    if isinstance(value, Exception):
                        raise value
                    return value
            return ''

        return fake

    def _clean_responses(self, overrides=None):
        """
        Return git answers for a pushed tag that passes every check.

        :param overrides: Replace or add individual answers.
        :returns: Maps a git subcommand to its stdout.
        """
        responses = {
            ('cat-file', '-t'): 'tag',
            ('cat-file', 'tag'): self.SIGNED,
            ('fetch',): '',
            ('merge-base', '--is-ancestor'): '',
        }
        responses.update(overrides or {})
        return responses

    @contextlib.contextmanager
    def _patched(self,
                 responses,
                 version='1.5.0',
                 tags=('1.4.0', '1.4.4'),
                 on_pypi=False):
        """
        Patch git, the version files, the tag list and PyPI for one call.

        The `newest_released` stand-in takes a tag list and honours `exclude`, rather than answering
        a fixed version. A stand-in that ignored `exclude` let `verify_tag` refuse every release:
        the tag exists by the time it runs, so without the exclusion the version is its own
        predecessor.

        :param responses: Maps a git subcommand to its stdout, or to an exception.
        :param version: What both version files declare.
        :param tags: The releasable tags the repository holds.
        :param on_pypi: Whether PyPI already holds the version.
        :yields: Nothing; the patches are active for the block.
        """

        def newest_released(root=None, exclude=''):
            kept = [name for name in tags if name != exclude]
            return max(kept,
                       key=tag_release.release_version.Version) if kept else ''

        with contextlib.ExitStack() as stack:
            stack.enter_context(
                mock.patch.object(tag_release, 'git', self._git(responses)))
            stack.enter_context(
                mock.patch.object(tag_release, 'version_in',
                                  lambda *a: version))
            stack.enter_context(
                mock.patch.object(tag_release, 'newest_released',
                                  newest_released))
            stack.enter_context(
                mock.patch.object(tag_release, 'on_pypi', lambda v: on_pypi))
            yield

    def _refuses(self, responses, tag='1.5.0', **patches):
        """
        Assert `verify_tag` refuses, and return the reason it gave.

        :param responses: Maps a git subcommand to its stdout, or to an exception.
        :param tag: The pushed tag.
        :param patches: Forwarded to `_patched`.
        :returns: The exception message.
        """
        with contextlib.ExitStack() as stack:
            stack.enter_context(self._patched(responses, **patches))
            caught = stack.enter_context(
                self.assertRaises(tag_release.CheckFailed))
            tag_release.verify_tag(tag, self.SHA)
        return str(caught.exception)

    def test_a_signed_tag_on_main_passes_and_returns_the_parameters(self):
        with self._patched(self._clean_responses()):
            implied = tag_release.verify_tag('1.5.0', self.SHA)
        self.assertEqual(implied['mode'], 'release')

    def test_an_unreleasable_tag_is_refused_before_touching_git(self):

        def never(*args, **kwargs):
            raise AssertionError('should not have been consulted')

        with contextlib.ExitStack() as stack:
            stack.enter_context(mock.patch.object(tag_release, 'git', never))
            stack.enter_context(self.assertRaises(tag_release.CheckFailed))
            tag_release.verify_tag('v1.5.0', self.SHA)

    def test_a_lightweight_tag_is_refused(self):
        overrides = {('cat-file', '-t'): 'commit'}
        self.assertIn('not an annotated tag',
                      self._refuses(self._clean_responses(overrides)))

    def test_an_unsigned_annotated_tag_is_refused(self):
        overrides = {('cat-file', 'tag'): 'object x\ntype commit\n\npika\n'}
        self.assertIn('no signature',
                      self._refuses(self._clean_responses(overrides)))

    def test_every_armour_git_can_produce_is_accepted(self):
        # `git tag --sign` honours `gpg.format`, so all three are correct output.
        for armour in ('-----BEGIN PGP SIGNATURE-----',
                       '-----BEGIN SSH SIGNATURE-----',
                       '-----BEGIN SIGNED MESSAGE-----'):
            with self.subTest(armour=armour):
                overrides = {('cat-file', 'tag'): f'tag 1.5.0\n\n{armour}\n'}
                with self._patched(self._clean_responses(overrides)):
                    tag_release.verify_tag('1.5.0', self.SHA)

    def test_version_files_disagree_with_the_tag(self):
        reason = self._refuses(self._clean_responses(), version='1.4.4')
        self.assertIn("not '1.5.0'", reason)

    def test_a_commit_that_is_not_on_main_is_refused(self):
        # `merge-base --is-ancestor` exits nonzero rather than printing, so the
        # stand-in raises to reproduce what `git` does.
        overrides = {
            ('merge-base', '--is-ancestor'):
                tag_release.CheckFailed('git merge-base failed'),
        }
        self.assertIn('not an ancestor',
                      self._refuses(self._clean_responses(overrides)))

    def test_a_version_older_than_the_newest_release_is_refused(self):
        # The check the workflow's shell copy never had. 1.4.3 was never
        # published and is lower than 1.4.4, so every other guard passes it and
        # `main` would have been published as 1.4.3.
        reason = self._refuses(self._clean_responses(),
                               tag='1.4.3',
                               version='1.4.3',
                               tags=('1.4.0', '1.4.4'))
        self.assertIn('does not move past 1.4.4', reason)

    def test_re_tagging_the_newest_release_is_refused(self):
        # 1.4.4 excludes itself, so 1.4.0 is the predecessor it must beat and
        # does not. Re-tagging a released version is caught here and on PyPI.
        reason = self._refuses(self._clean_responses(),
                               tag='1.4.4',
                               version='1.4.4',
                               tags=('1.4.0', '1.4.4', '1.4.5'))
        self.assertIn('does not move past 1.4.5', reason)

    def test_a_first_release_with_no_predecessor_passes(self):
        with self._patched(self._clean_responses(), tags=()):
            tag_release.verify_tag('1.5.0', self.SHA)

    def test_a_version_already_on_pypi_is_refused(self):
        self.assertIn('already on PyPI',
                      self._refuses(self._clean_responses(), on_pypi=True))

    def test_re_tagging_a_published_version_is_refused_by_pypi(self):
        # The exclusion means the ordering guard no longer sees 1.4.4 as its own
        # predecessor, so PyPI is what refuses a re-pushed released tag.
        reason = self._refuses(self._clean_responses(),
                               tag='1.4.4',
                               version='1.4.4',
                               tags=('1.4.0', '1.4.3', '1.4.4'),
                               on_pypi=True)
        self.assertIn('already on PyPI', reason)

    def test_pypi_can_be_skipped(self):
        with self._patched(self._clean_responses(), on_pypi=True):
            tag_release.verify_tag('1.5.0', self.SHA, verify_pypi=False)


if __name__ == '__main__':
    unittest.main()
