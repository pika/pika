"""
End-to-end tests for the release operations against a real git repository.

The other release test modules mock git. This one does not: it builds a throwaway repository with a
throwaway remote, points the helpers at it, and runs the operations for real. That distinction
earned its place. Every bug in this area that mocks did not catch was about the interaction with git
or the filesystem rather than about logic: a pre-validation step that read a different file from the
one it guarded, because the two modules have separate roots and mocks hid the difference; a body
file written under `.git`, which is a regular file in a linked worktree; a branch created before the
step that could fail; and a documented sequence of operations that could not execute because each
one left the tree in a state the next refused.

Only `gh` is faked, because it reaches the network. Every `git` command runs. Tag creation drops
`--sign`, since a signing key cannot be assumed on a runner; that the real command signs is asserted
in `tag_release_tests.py`.
"""

from __future__ import annotations

import contextlib
import importlib.util
import io
import json
import os
import pathlib
import shutil
import subprocess
import sys
import tempfile
import time
import unittest
from unittest import mock

# Linux only, deliberately. These exercise git interaction, which does not vary
# with the Python version, so running them on every leg of the matrix is twenty
# times redundant - and the two platforms it drags in are the ones a release is
# never cut from. Both cost a round of red CI for reasons that had nothing to do
# with the release: Windows cannot delete git's read-only object files, and macOS
# runners keep a running RabbitMQ inside `.ci/`, which the sandbox was copying.
# A release is cut from one Linux machine; this is where testing it belongs.
_LINUX_ONLY = unittest.skipUnless(sys.platform.startswith('linux'),
                                  'the release is only ever cut on Linux')

_CI = pathlib.Path(__file__).resolve().parents[2] / '.ci'
_SPEC = importlib.util.spec_from_file_location('release', _CI / 'release.py')
assert _SPEC is not None and _SPEC.loader is not None
release = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(release)

PREAMBLE_TEXT = ('## Upgrading to 9.9.0\n'
                 '\n'
                 '### A thing changed\n'
                 '\n'
                 'It changed.\n')
HISTORY_TEXT = ('# Changelog\n'
                '\n'
                '## Upgrading to 9.9.0\n'
                '\n'
                '### A thing changed\n'
                '\n'
                'It changed.\n'
                '\n'
                '## [9.8.0](https://example.invalid/9.8.0) (2026-01-01)\n'
                '\n'
                '- something\n'
                '\n'
                '## Version History\n'
                '\n'
                'Hand-written.\n')


def disposable_directory(stack: contextlib.ExitStack) -> pathlib.Path:
    """
    Return a temporary directory whose removal cannot fail the test.

    `TemporaryDirectory` is not usable for a git repository. On Windows git marks its object files
    read-only, so removal raises `PermissionError`; on macOS removal intermittently raises `OSError:
    Directory not empty`. Both are cleanup, not the assertion, and both failed every sandbox test on
    those platforms while the assertions themselves passed.

    `ignore_errors` rather than an error handler, because `shutil.rmtree`'s `onerror` is deprecated
    from 3.12 and `onexc` does not exist before it, and this has to work from 3.7 to 3.15.

    :param stack: An entered `ExitStack` to register the removal with.
    :returns: The directory.
    """
    path = pathlib.Path(tempfile.mkdtemp())

    def remove() -> None:
        for _ in range(3):
            # Writable first: nothing can delete git's read-only objects.
            for entry in path.rglob('*'):
                try:
                    entry.chmod(0o700)
                except OSError:
                    pass
            shutil.rmtree(str(path), ignore_errors=True)
            if not path.exists():
                return
            time.sleep(0.2)

    stack.callback(remove)
    return path


class Sandbox:
    """A disposable repository the release operations can be run against."""

    def __init__(self, stack: contextlib.ExitStack, version: str = '9.8.0'):
        """
        Build the repository, its remote, and a `main` that matches the remote.

        :param stack: An entered `ExitStack` owning the temporary directories.
        :param version: The version the two files start at.
        """
        base = disposable_directory(stack)
        # Hermetic: no global or system git configuration, and `HOME` inside the
        # sandbox. Without this the tests read whatever the developer's machine
        # happens to set, which is how a `user.signingkey` requirement passed
        # locally and failed all 37 CI legs. The patch covers the environment
        # rather than this class's own git calls, because the code under test
        # spawns its own subprocesses.
        stack.enter_context(
            mock.patch.dict(
                os.environ, {
                    'GIT_CONFIG_GLOBAL': os.devnull,
                    'GIT_CONFIG_SYSTEM': os.devnull,
                    'HOME': str(base),
                }))
        self.root = base / 'work'
        self.origin = base / 'origin.git'
        self.root.mkdir()
        self.commands: list[tuple[str, ...]] = []
        # What the fake `gh` reports for the changelog's milestone queries.
        self.issues: list[dict] = [{
            'number': 9001,
            'title': 'A thing that was fixed',
            'labels': [{
                'name': 'C-bug'
            }],
        }]
        self.pulls: list[dict] = [{
            'number': 9002,
            'title': 'Fix the thing',
            'author': {
                'login': 'lukebakken'
            },
        }]

        # `symbolic-ref` rather than `init --initial-branch`, which needs git
        # 2.28, and an explicit identity and signing state rather than whatever
        # the ambient configuration happens to be: a global `commit.gpgsign` or
        # `tag.gpgsign` would otherwise decide whether these tests can commit.
        self.git('init', '--quiet')
        self.git('symbolic-ref', 'HEAD', 'refs/heads/main')
        self.git('config', 'user.name', 'Test')
        self.git('config', 'user.email', 'test@example.invalid')
        self.git('config', 'commit.gpgsign', 'false')
        self.git('config', 'tag.gpgsign', 'false')
        # `cmd_tag` refuses without one, by design. The value is never used,
        # because `tag_command` is stubbed to drop `--sign`.
        self.git('config', 'user.signingkey', 'TESTKEY')
        subprocess.run(('git', 'init', '--quiet', '--bare', str(self.origin)),
                       check=True)
        self.git('remote', 'add', 'origin', str(self.origin))

        (self.root / 'pika').mkdir()
        (self.root / '.github').mkdir()
        self.write_version(version)
        (self.root / 'HISTORY.md').write_text(HISTORY_TEXT, encoding='utf-8')
        (self.root / release.PREAMBLE).write_text(PREAMBLE_TEXT,
                                                  encoding='utf-8')
        # The helpers resolve their own location, so a copy inside the sandbox
        # makes `_ROOT` the sandbox for both modules at once. Only the Python
        # files: `copytree` of the whole directory also copied `.ci/macos`,
        # which on a macOS runner holds a *running* RabbitMQ installation whose
        # mnesia files are created and removed while the copy walks them.
        (self.root / '.ci').mkdir()
        for helper in sorted(_CI.glob('*.py')):
            shutil.copy2(str(helper), str(self.root / '.ci' / helper.name))

        self.git('add', '--all')
        self.git('commit', '--quiet', '--message', f'pika {version}')
        self.git('tag', '--annotate', '--message', f'pika {version}', version)
        self.git('push', '--quiet', 'origin', 'main')
        self.git('fetch', '--quiet', 'origin')

        stack.enter_context(mock.patch.object(release, '_ROOT', self.root))
        stack.enter_context(
            mock.patch.object(release.tag_release, '_ROOT', self.root))
        stack.enter_context(mock.patch.object(release, 'run', self._run))
        stack.enter_context(
            mock.patch.object(release.tag_release, 'tag_command',
                              self._tag_command))

    def git(self, *args: str) -> str:
        """
        Run git in the sandbox and return its stdout.

        :param args: Arguments after `git`.
        :returns: Captured stdout, stripped.
        """
        done = subprocess.run(('git', *args),
                              cwd=self.root,
                              capture_output=True,
                              text=True,
                              check=True)
        return done.stdout.strip()

    @staticmethod
    def _tag_command(version: str, signing_key: str) -> tuple[str, ...]:
        """
        Return an annotated but unsigned tag command.

        :param version: The version to tag.
        :param signing_key: Ignored; a runner has no key.
        :returns: The argument vector.
        """
        return ('git', 'tag', '--annotate', f'--message=pika {version}',
                version)

    def _run(self, command, dry_run: bool = False) -> str:
        """
        Execute git for real, record everything, and fake what reaches the network.

        :param command: The argument vector.
        :param dry_run: Record without executing.
        :returns: Captured stdout, or a canned answer.
        """
        command = tuple(command)
        self.commands.append(command)
        if dry_run:
            return ''
        if command[0] == 'git':
            # The stubbed `tag_command` drops `--sign`, so the tag really is
            # unsigned and `--verify` really does fail. Stubbing the check that
            # the stub invalidated keeps the rest of the path real; that the
            # production command signs is asserted in `tag_release_tests.py`.
            if command[1:3] == ('tag', '--verify'):
                return ''
            return self.git(*command[1:])
        if command[0] == 'gh':
            # `pr list` without `--search` is the "is one already open" probe
            # and must answer empty; with `--search` it is the changelog's
            # milestone query and must answer JSON. Only `pr create` returns a
            # URL.
            if command[1:3] == ('issue', 'list'):
                return json.dumps(self.issues)
            if command[1:3] == ('pr', 'list'):
                if '--search' in command:
                    return json.dumps(self.pulls)
                return ''
            if command[1:3] == ('pr', 'view'):
                return json.dumps({
                    'number': int(command[3]),
                    'title': f'stray {command[3]}',
                    'author': {
                        'login': 'someone'
                    },
                })
            return 'https://example.invalid/pull/1'
        if command[0] == 'github_changelog_generator':
            raise AssertionError('the generator was replaced by changelog.py')
        raise AssertionError(f'unexpected command {command}')

    def write_version(self, version: str) -> None:
        """
        Write *version* into both files that declare it.

        :param version: The version to write.
        """
        (self.root / 'pyproject.toml').write_text(
            f'[project]\nname = "pika"\nversion = "{version}"\n',
            encoding='utf-8')
        (self.root / 'pika' / '__init__.py').write_text(
            f"__version__ = '{version}'\n", encoding='utf-8')


def invoke(argv):
    """
    Run one operation and capture both streams.

    :param argv: Arguments after the program name.
    :returns:`(status, stdout, stderr)`.
    """
    out, err = io.StringIO(), io.StringIO()
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
        status = release.main(argv)
    return status, out.getvalue(), err.getvalue()


@_LINUX_ONLY
class DocumentedFlowTests(unittest.TestCase):
    """The four steps `RELEASE.md` prescribes, run in the order it gives."""

    def test_bump_then_changelog_then_pr_then_tag(self):
        with contextlib.ExitStack() as stack:
            box = Sandbox(stack)

            status, _, err = invoke(['bump', '--version', '9.9.0'])
            self.assertEqual(status, 0, err)
            self.assertEqual(box.git('rev-parse', '--abbrev-ref', 'HEAD'),
                             'pika-9.9.0')
            self.assertEqual(box.git('log', '-1', '--format=%s'), 'pika 9.9.0')
            self.assertIn('version = "9.9.0"',
                          (box.root / 'pyproject.toml').read_text())

            # The step that could not run at all before: `changelog` after
            # `bump`, on the branch, leaving the tree clean for `pr`.
            status, _, err = invoke(['changelog', '--version', '9.9.0'])
            self.assertEqual(status, 0, err)
            self.assertIn('## [9.9.0]',
                          (box.root / 'HISTORY.md').read_text(encoding='utf-8'))
            self.assertEqual(box.git('status', '--porcelain'), '')

            status, _, err = invoke(['pr'])
            self.assertEqual(status, 0, err)

            # Merge it, as review would, and tag from `main`.
            box.git('checkout', '--quiet', 'main')
            box.git('merge', '--quiet', '--no-ff', '--no-edit', 'pika-9.9.0')
            box.git('push', '--quiet', 'origin', 'main')
            box.git('fetch', '--quiet', 'origin')

            with mock.patch.object(release.tag_release, 'on_pypi',
                                   lambda v: False):
                status, _, err = invoke(['check'])
                self.assertEqual(status, 0, err)
                status, _, err = invoke(['tag', '--push'])
            self.assertEqual(status, 0, err)
            self.assertEqual(box.git('cat-file', '-t', '9.9.0'), 'tag')
            self.assertIn(
                '9.9.0',
                box.git('ls-remote', '--tags', 'origin', 'refs/tags/9.9.0'))

    def test_changelog_before_bump_is_refused_rather_than_wedging_the_tree(
            self):
        # The documented order used to be changelog first, which left
        # `HISTORY.md` modified on `main` and no way forward.
        with contextlib.ExitStack() as stack:
            box = Sandbox(stack)
            status, _, err = invoke(['changelog', '--version', '9.9.0'])
            self.assertEqual(status, 1)
            self.assertIn('bump', err)
            self.assertEqual(box.git('status', '--porcelain'), '')


@_LINUX_ONLY
class BumpSandboxTests(unittest.TestCase):
    """What `bump` refuses, against a real repository."""

    def test_a_dirty_tree_is_refused_and_no_branch_is_created(self):
        with contextlib.ExitStack() as stack:
            box = Sandbox(stack)
            (box.root / 'pyproject.toml').write_text(
                '[project]\nversion = "9.8.0"\n# edit\n', encoding='utf-8')
            status, _, err = invoke(['bump', '--version', '9.9.0'])
            branches = box.git('branch', '--list')
        self.assertEqual(status, 1)
        self.assertIn('not clean', err)
        self.assertNotIn('pika-9.9.0', branches)

    def test_a_version_file_without_a_version_leaves_no_branch_behind(self):
        with contextlib.ExitStack() as stack:
            box = Sandbox(stack)
            (box.root / 'pyproject.toml').write_text(
                '[project]\nname = "pika"\n', encoding='utf-8')
            box.git('commit', '--quiet', '--all', '--message', 'drop it')
            box.git('push', '--quiet', 'origin', 'main')
            box.git('fetch', '--quiet', 'origin')
            status, _, err = invoke(['bump', '--version', '9.9.0'])
            branches = box.git('branch', '--list')
        self.assertEqual(status, 1)
        self.assertIn('does not declare a version', err)
        self.assertNotIn('pika-9.9.0', branches)

    def test_head_behind_the_remote_is_refused(self):
        with contextlib.ExitStack() as stack:
            box = Sandbox(stack)
            box.git('commit', '--quiet', '--allow-empty', '--message', 'ahead')
            status, _, err = invoke(['bump', '--version', '9.9.0'])
        self.assertEqual(status, 1)
        self.assertIn('pull first', err)


@_LINUX_ONLY
class TagSandboxTests(unittest.TestCase):
    """What `tag` refuses once the bump has landed."""

    def _landed(self, stack, version='9.9.0'):
        """
        Return a sandbox whose `main` carries *version* and its changelog entry.

        :param stack: An entered `ExitStack`.
        :param version: The version to land.
        :returns: The sandbox.
        """
        box = Sandbox(stack)
        box.write_version(version)
        history = (box.root / 'HISTORY.md').read_text(encoding='utf-8')
        (box.root / 'HISTORY.md').write_text(history.replace(
            '## [9.8.0]', f'## [{version}](x) (2026)\n\n- thing\n\n'
            f'## [9.8.0]'),
                                             encoding='utf-8')
        box.git('commit', '--quiet', '--all', '--message', f'pika {version}')
        box.git('push', '--quiet', 'origin', 'main')
        box.git('fetch', '--quiet', 'origin')
        return box

    def test_a_tag_that_disagrees_with_the_files_is_refused(self):
        with contextlib.ExitStack() as stack:
            self._landed(stack, '9.9.0')
            stack.enter_context(
                mock.patch.object(release.tag_release, 'on_pypi',
                                  lambda v: False))
            status, _, err = invoke(['tag', '--version', '9.9.1'])
        self.assertEqual(status, 1)
        self.assertIn('9.9.1', err)

    def test_a_backwards_version_is_refused(self):
        # 9.8.0 is already tagged in the sandbox, so 9.8.0a1 sorts below it.
        with contextlib.ExitStack() as stack:
            self._landed(stack, '9.8.0a1')
            stack.enter_context(
                mock.patch.object(release.tag_release, 'on_pypi',
                                  lambda v: False))
            status, _, err = invoke(['tag'])
        self.assertEqual(status, 1)
        self.assertIn('does not move past', err)

    def test_a_missing_changelog_entry_blocks_the_tag(self):
        # `check` and `tag` used to disagree: `check` exited 1 and `tag`
        # succeeded on the next line.
        with contextlib.ExitStack() as stack:
            box = Sandbox(stack)
            box.write_version('9.9.0')
            box.git('commit', '--quiet', '--all', '--message', 'pika 9.9.0')
            box.git('push', '--quiet', 'origin', 'main')
            box.git('fetch', '--quiet', 'origin')
            stack.enter_context(
                mock.patch.object(release.tag_release, 'on_pypi',
                                  lambda v: False))
            check_status, _, _ = invoke(['check'])
            tag_status, _, err = invoke(['tag'])
        self.assertEqual(check_status, 1)
        self.assertEqual(tag_status, 1)
        self.assertIn('not ready', err)

    def test_an_existing_tag_is_refused(self):
        with contextlib.ExitStack() as stack:
            box = self._landed(stack, '9.9.0')
            box.git('tag', '--annotate', '--message', 'pika 9.9.0', '9.9.0')
            stack.enter_context(
                mock.patch.object(release.tag_release, 'on_pypi',
                                  lambda v: False))
            status, _, err = invoke(['tag'])
        self.assertEqual(status, 1)
        self.assertIn('already exists', err)

    def test_a_detached_head_is_refused(self):
        with contextlib.ExitStack() as stack:
            box = self._landed(stack, '9.9.0')
            box.git('checkout', '--quiet', '--detach', 'HEAD')
            stack.enter_context(
                mock.patch.object(release.tag_release, 'on_pypi',
                                  lambda v: False))
            status, _, err = invoke(['tag'])
        self.assertEqual(status, 1)
        self.assertIn('not', err)


@_LINUX_ONLY
class PushShapeTests(unittest.TestCase):
    """How the tag reaches the remote, which decides whether a run starts."""

    def test_the_tag_is_pushed_by_explicit_refspec(self):
        # A bare `git push origin <tag>` honours `push.followTags`, which pushes
        # every annotated tag reachable from HEAD. More than three at once and
        # GitHub creates no event at all, so the release silently does not
        # happen while the script reports that it has.
        with contextlib.ExitStack() as stack:
            box = Sandbox(stack)
            box.write_version('9.9.0')
            history = (box.root / 'HISTORY.md').read_text(encoding='utf-8')
            (box.root / 'HISTORY.md').write_text(history.replace(
                '## [9.8.0]', '## [9.9.0](x) (2026)\n\n- thing\n\n'
                '## [9.8.0]'),
                                                 encoding='utf-8')
            box.git('commit', '--quiet', '--all', '--message', 'pika 9.9.0')
            box.git('push', '--quiet', 'origin', 'main')
            box.git('fetch', '--quiet', 'origin')
            box.git('config', 'push.followTags', 'true')
            for extra in ('9.8.1', '9.8.2', '9.8.3'):
                box.git('tag', '--annotate', '--message', extra, extra)
            stack.enter_context(
                mock.patch.object(release.tag_release, 'on_pypi',
                                  lambda v: False))
            status, _, err = invoke(['tag', '--push'])
            pushes = [c for c in box.commands if c[:2] == ('git', 'push')]
            remote = box.git('ls-remote', '--tags', 'origin')
        self.assertEqual(status, 0, err)
        self.assertEqual(
            pushes, [('git', 'push', '--no-follow-tags', 'origin',
                      'refs/tags/9.9.0:refs/tags/9.9.0')],
            'an explicit refspec is not enough: --follow-tags treats a tag as '
            'a ref-tip and sends its reachable siblings too')
        # Only the tag asked for. The sandbox's own `push origin main` predates
        # `push.followTags`, so 9.8.0 was never pushed.
        self.assertEqual(
            sorted(
                line.split('refs/tags/')[1]
                for line in remote.splitlines()
                if '^{}' not in line), ['9.9.0'])


@_LINUX_ONLY
class BackwardsGuardTests(unittest.TestCase):
    """The guard that keeps a release from going backwards cannot be optional."""

    def _at(self, stack, version):
        """
        Return a sandbox whose `main` carries *version* and its changelog entry.

        :param stack: An entered `ExitStack`.
        :param version: The version to land.
        :returns: The sandbox.
        """
        box = Sandbox(stack)
        box.write_version(version)
        history = (box.root / 'HISTORY.md').read_text(encoding='utf-8')
        (box.root / 'HISTORY.md').write_text(history.replace(
            '## [9.8.0]', f'## [{version}](x) (2026)\n\n- thing\n\n'
            f'## [9.8.0]'),
                                             encoding='utf-8')
        box.git('commit', '--quiet', '--all', '--message', f'pika {version}')
        box.git('push', '--quiet', 'origin', 'main')
        box.git('fetch', '--quiet', 'origin')
        stack.enter_context(
            mock.patch.object(release.tag_release, 'on_pypi', lambda v: False))
        return box

    def test_skip_readiness_does_not_defeat_it(self):
        # The refusal message names `--skip-readiness`, so the flag it
        # advertises must not be the flag that disables the backwards check.
        with contextlib.ExitStack() as stack:
            box = self._at(stack, '9.7.0')
            status, _, err = invoke(['tag', '--skip-readiness', '--push'])
            remote = box.git('ls-remote', '--tags', 'origin')
        self.assertEqual(status, 1)
        self.assertIn('does not move past', err)
        self.assertNotIn('9.7.0', remote)

    def test_tags_only_on_the_remote_still_count(self):
        # Patch releases are cut from maintenance branches, so the newest
        # released version is often absent from a fresh clone's tag list.
        with contextlib.ExitStack() as stack:
            box = self._at(stack, '9.8.5')
            box.git('tag', '--annotate', '--message', 'pika 9.9.9', '9.9.9')
            box.git('push', '--quiet', 'origin', 'refs/tags/9.9.9')
            box.git('tag', '--delete', '9.9.9')
            status, _, err = invoke(['tag'])
        self.assertEqual(status, 1)
        self.assertIn('9.9.9', err)


@_LINUX_ONLY
class ChangelogSandboxTests(unittest.TestCase):
    """
    `changelog` writes and commits, so what it refuses matters.

    What the entry *contains* is covered by `changelog_tests.py`, which renders without touching
    git. Three tests here went with `github_changelog_generator`: two asserted how its stdout was
    parsed, and one that the parsed entry named the right version. Nothing parses anything now, so
    the question cannot arise.
    """

    def _branched(self, stack):
        """
        Return a sandbox on a release branch at 9.9.0.

        :param stack: An entered `ExitStack`.
        :returns: The sandbox.
        """
        box = Sandbox(stack)
        box.git('checkout', '--quiet', '-b', 'pika-9.9.0')
        box.write_version('9.9.0')
        box.git('commit', '--quiet', '--all', '--message', 'pika 9.9.0')
        return box

    def test_a_detached_head_is_refused(self):
        # It commits. On a detached HEAD the commit is unreachable and the next
        # checkout orphans it, with the tool reporting success.
        with contextlib.ExitStack() as stack:
            box = self._branched(stack)
            box.git('checkout', '--quiet', '--detach', 'HEAD')
            status, _, err = invoke(['changelog', '--version', '9.9.0'])
            history = (box.root / 'HISTORY.md').read_text(encoding='utf-8')
        self.assertEqual(status, 1)
        self.assertIn('detached', err)
        self.assertNotIn('## [9.9.0]', history)

    def test_a_dirty_tree_is_refused(self):
        # Anything already modified would be swept into the changelog commit.
        with contextlib.ExitStack() as stack:
            box = self._branched(stack)
            (box.root / 'README.md').write_text('stray\n', encoding='utf-8')
            box.git('add', 'README.md')
            status, _, err = invoke(['changelog', '--version', '9.9.0'])
        self.assertEqual(status, 1)
        self.assertIn('not clean', err)

    def test_it_will_not_compute_a_version_off_the_already_bumped_file(self):
        # `bump` leaves pyproject at the release version, so a `--bump` here
        # compounds and generates an entry for a version the branch is not
        # releasing.
        with contextlib.ExitStack() as stack:
            self._branched(stack)
            # `argparse` exits rather than returning, so the flag not parsing
            # is the assertion.
            with self.assertRaises(SystemExit):
                invoke(['changelog', '--bump', 'patch', '--dry-run'])

    def test_a_version_disagreeing_with_the_files_is_refused(self):
        with contextlib.ExitStack() as stack:
            self._branched(stack)
            status, _, err = invoke(
                ['changelog', '--version', '9.9.1', '--dry-run'])
        self.assertEqual(status, 1)
        self.assertIn('9.9.0', err)

    def test_a_version_already_in_history_is_refused(self):
        with contextlib.ExitStack() as stack:
            box = self._branched(stack)
            history = (box.root / 'HISTORY.md').read_text(encoding='utf-8')
            (box.root / 'HISTORY.md').write_text(history.replace(
                '## [9.8.0]', '## [9.9.0](x) (2026)\n\n'
                '- already\n\n## [9.8.0]'),
                                                 encoding='utf-8')
            box.git('commit', '--quiet', '--all', '--message', 'entry')
            status, _, err = invoke(['changelog', '--dry-run'])
        self.assertEqual(status, 1)
        self.assertIn('already has an entry', err)

    def test_an_unparseable_version_lists_every_problem(self):
        # `refuse_backwards` raises `InvalidVersion`, a `ValueError`, which
        # escaped `readiness` and discarded the problems already collected.
        with contextlib.ExitStack() as stack:
            Sandbox(stack)
            stack.enter_context(
                mock.patch.object(release.tag_release, 'on_pypi',
                                  lambda v: False))
            status, out, err = invoke(['check', '--version', 'garbage'])
        self.assertEqual(status, 1)
        self.assertIn('not a releasable tag', out + err)

    def test_a_missing_file_is_reported_rather_than_traced(self):
        with contextlib.ExitStack() as stack:
            box = Sandbox(stack)
            (box.root / 'HISTORY.md').unlink()
            status, _, err = invoke(['check', '--no-verify-pypi'])
        self.assertEqual(status, 1)
        self.assertNotIn('Traceback', err)
        self.assertIn('HISTORY.md', err)


@_LINUX_ONLY
class AtomicityTests(unittest.TestCase):
    """An operation that fails part-way must not leave work behind."""

    def test_a_failed_tag_verification_removes_the_tag(self):
        # The tag is created before it can be verified, so a verify failure
        # left it behind for the next run to refuse. `cmd_bump` carries the fix
        # for the identical shape, with the reason spelled out.
        with contextlib.ExitStack() as stack:
            box = Sandbox(stack)
            box.write_version('9.9.0')
            history = (box.root / 'HISTORY.md').read_text(encoding='utf-8')
            (box.root / 'HISTORY.md').write_text(history.replace(
                '## [9.8.0]', '## [9.9.0](x) (2026)\n\n- t\n\n'
                '## [9.8.0]'),
                                                 encoding='utf-8')
            box.git('commit', '--quiet', '--all', '--message', 'pika 9.9.0')
            box.git('push', '--quiet', 'origin', 'main')
            box.git('fetch', '--quiet', 'origin')
            stack.enter_context(
                mock.patch.object(release.tag_release, 'on_pypi',
                                  lambda v: False))

            real = box._run

            def failing(command, dry_run=False):
                if tuple(command)[:3] == ('git', 'tag', '--verify'):
                    raise release.CheckFailed("gpg: can't check signature")
                return real(command, dry_run)

            stack.enter_context(mock.patch.object(release, 'run', failing))
            status, _, _ = invoke(['tag'])
            tags = box.git('tag', '--list')
        self.assertEqual(status, 1)
        self.assertNotIn('9.9.0', tags,
                         'a tag that could not be verified must not survive')

    def test_a_failed_bump_commit_leaves_no_branch_checked_out(self):
        with contextlib.ExitStack() as stack:
            box = Sandbox(stack)
            # The commit is made to fail directly. Unsetting the identity is not
            # enough, because `user.useConfigOnly` stops git guessing but not
            # git reading a global `user.email`; a hook or a full disk is the
            # real-world trigger and neither is portable to arrange.
            real = box._run

            def failing(command, dry_run=False):
                if tuple(command)[:2] == ('git', 'commit'):
                    raise release.CheckFailed('pre-commit hook refused')
                return real(command, dry_run)

            stack.enter_context(mock.patch.object(release, 'run', failing))
            status, _, _ = invoke(['bump', '--version', '9.9.0'])
            branch = box.git('rev-parse', '--abbrev-ref', 'HEAD')
            branches = box.git('branch', '--list')
        self.assertEqual(status, 1)
        self.assertEqual(branch, 'main')
        self.assertNotIn('pika-9.9.0', branches)


@_LINUX_ONLY
class NotesBindingTests(unittest.TestCase):
    """The preamble has to be the one for the version being released."""

    def _ready(self, stack, version, history, preamble):
        """
        Return a sandbox at *version* with the given documents.

        :param stack: An entered `ExitStack`.
        :param version: The version to land.
        :param history: Contents of `HISTORY.md`.
        :param preamble: Contents of the preamble.
        :returns: The sandbox.
        """
        box = Sandbox(stack)
        box.write_version(version)
        (box.root / 'HISTORY.md').write_text(history, encoding='utf-8')
        (box.root / release.PREAMBLE).write_text(preamble, encoding='utf-8')
        box.git('commit', '--quiet', '--all', '--message', f'pika {version}')
        box.git('push', '--quiet', 'origin', 'main')
        box.git('fetch', '--quiet', 'origin')
        stack.enter_context(
            mock.patch.object(release.tag_release, 'on_pypi', lambda v: False))
        return box

    HISTORY_TWO = ('# Changelog\n\n'
                   '## Upgrading to 9.9.1\n\n### New\n\nNew thing.\n\n'
                   '## Upgrading to 9.9.0\n\n### Old\n\nOld thing.\n\n'
                   '## [9.9.0](x) (2026)\n\n- a\n\n'
                   '## Version History\n\nHand-written.\n')

    def test_a_kept_older_upgrading_section_is_not_treated_as_drift(self):
        # RELEASE.md keeps previous sections, so the compared region must end
        # at the next heading, not at the next version entry.
        history = self.HISTORY_TWO.replace(
            '## [9.9.0](x) (2026)', '## [9.9.1](x) (2026)\n\n- b\n\n'
            '## [9.9.0](x) (2026)')
        preamble = '## Upgrading to 9.9.1\n\n### New\n\nNew thing.\n'
        with contextlib.ExitStack() as stack:
            self._ready(stack, '9.9.1', history, preamble)
            status, out, err = invoke(['check'])
        self.assertEqual(status, 0, out + err)

    def test_a_preamble_for_the_previous_version_is_refused(self):
        history = self.HISTORY_TWO.replace(
            '## [9.9.0](x) (2026)', '## [9.9.1](x) (2026)\n\n- b\n\n'
            '## [9.9.0](x) (2026)')
        stale = '## Upgrading to 9.9.0\n\n### Old\n\nOld thing.\n'
        with contextlib.ExitStack() as stack:
            self._ready(stack, '9.9.1', history, stale)
            status, out, _ = invoke(['check'])
        self.assertEqual(status, 1)
        self.assertIn('9.9.1', out)


@_LINUX_ONLY
class PreambleStateTests(unittest.TestCase):
    """The preamble states `release.yaml` supports, end to end."""

    def _ready(self, stack):
        """
        Return a sandbox ready to release 9.9.0.

        :param stack: An entered `ExitStack`.
        :returns: The sandbox.
        """
        box = Sandbox(stack)
        box.write_version('9.9.0')
        history = (box.root / 'HISTORY.md').read_text(encoding='utf-8')
        (box.root / 'HISTORY.md').write_text(history.replace(
            '## [9.8.0]', '## [9.9.0](x) (2026)\n\n- thing\n\n'
            '## [9.8.0]'),
                                             encoding='utf-8')
        box.git('commit', '--quiet', '--all', '--message', 'pika 9.9.0')
        box.git('push', '--quiet', 'origin', 'main')
        box.git('fetch', '--quiet', 'origin')
        stack.enter_context(
            mock.patch.object(release.tag_release, 'on_pypi', lambda v: False))
        return box

    def test_an_absent_preamble_does_not_block_the_release(self):
        with contextlib.ExitStack() as stack:
            box = self._ready(stack)
            (box.root / release.PREAMBLE).unlink()
            status, out, err = invoke(['check'])
        self.assertEqual(status, 0, err + out)

    def test_an_empty_preamble_does_not_block_the_release(self):
        with contextlib.ExitStack() as stack:
            box = self._ready(stack)
            (box.root / release.PREAMBLE).write_text('', encoding='utf-8')
            status, out, err = invoke(['check'])
        self.assertEqual(status, 0, err + out)

    def test_a_drifted_preamble_blocks_the_release(self):
        with contextlib.ExitStack() as stack:
            box = self._ready(stack)
            (box.root / release.PREAMBLE).write_text(PREAMBLE_TEXT.replace(
                'It changed.', 'It altered.'),
                                                     encoding='utf-8')
            status, out, _ = invoke(['check'])
        self.assertEqual(status, 1)
        self.assertIn('diverge', out)


if __name__ == '__main__':
    unittest.main()
