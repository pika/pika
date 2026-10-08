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


if __name__ == '__main__':
    unittest.main()
