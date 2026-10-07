"""
Tests for the release version arithmetic in `.ci/release_version.py`.

The table in `FullCycleTests` is the point of the module. The same table, built by hand in bash, is
what exposed the defect this code replaces: every bump incremented, so a published `1.5.0a1` could
not be promoted to `1.5.0` by any input. A release version becomes a git tag, a PyPI release and a
documentation directory, so a wrong one is not recoverable; it is worth a table rather than a spot-
check.
"""

import importlib.util
import pathlib
import unittest

# `.ci` is not a package and its name is not a valid identifier, so the module
# is loaded by path rather than imported.
_MODULE_PATH = (pathlib.Path(__file__).resolve().parents[2] / '.ci' /
                'release_version.py')
_SPEC = importlib.util.spec_from_file_location('release_version', _MODULE_PATH)
assert _SPEC is not None and _SPEC.loader is not None
release_version = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(release_version)


class BaseOfTests(unittest.TestCase):
    """A version's release triple, whatever suffix it carries."""

    def test_stable_version(self):
        self.assertEqual(release_version.base_of('1.4.3'), (1, 4, 3))

    def test_prerelease_suffix_is_ignored(self):
        # The property that makes promotion work: the suffix contributes nothing
        # to the triple, so holding the version is the whole of promoting it.
        for name in ('1.5.0a1', '1.5.0b2', '1.5.0rc1'):
            with self.subTest(name=name):
                self.assertEqual(release_version.base_of(name), (1, 5, 0))

    def test_dev_and_post_suffixes_are_ignored(self):
        self.assertEqual(release_version.base_of('1.5.0.dev7'), (1, 5, 0))
        self.assertEqual(release_version.base_of('1.5.0.post1'), (1, 5, 0))

    def test_non_version_is_rejected(self):
        with self.assertRaises(ValueError):
            release_version.base_of('not-a-version')

    def test_two_part_version_is_rejected(self):
        # `1.5` parses, but this scheme publishes MAJOR.MINOR.PATCH and the
        # docs directory for `1.5` means something else entirely.
        with self.assertRaises(ValueError):
            release_version.base_of('1.5')


class BumpTests(unittest.TestCase):
    """Each bump, including the one that holds the version."""

    def test_major_zeroes_the_rest(self):
        self.assertEqual(release_version.bump('1.4.3', 'major'), '2.0.0')

    def test_minor_zeroes_the_patch(self):
        self.assertEqual(release_version.bump('1.4.3', 'minor'), '1.5.0')

    def test_patch_increments(self):
        self.assertEqual(release_version.bump('1.4.3', 'patch'), '1.4.4')

    def test_none_holds_the_base_version(self):
        self.assertEqual(release_version.bump('1.4.3', 'none'), '1.4.3')

    def test_none_drops_a_prerelease_suffix(self):
        # The case with no other spelling: promoting a published pre-release.
        self.assertEqual(release_version.bump('1.5.0a1', 'none'), '1.5.0')

    def test_minor_from_a_prerelease_advances_past_it(self):
        # Why `none` had to exist. This is what the workflow did for every
        # bump, so a published 1.5.0a1 could only become 1.6.0.
        self.assertEqual(release_version.bump('1.5.0a1', 'minor'), '1.6.0')

    def test_unknown_bump_is_rejected(self):
        with self.assertRaises(ValueError):
            release_version.bump('1.4.3', 'sideways')


class FullCycleTests(unittest.TestCase):
    """Every step of a pre-release cycle has to be reachable."""

    CYCLE = (
        # (current, bump, mode, prerelease_tag, expected)
        ('1.4.4', 'minor', 'prerelease', 'a1', '1.5.0a1'),
        ('1.5.0a1', 'none', 'prerelease', 'b1', '1.5.0b1'),
        ('1.5.0b1', 'none', 'prerelease', 'rc1', '1.5.0rc1'),
        ('1.5.0rc1', 'none', 'release', '', '1.5.0'),
        ('1.5.0', 'patch', 'release', '', '1.5.1'),
        ('1.5.1', 'minor', 'prerelease', 'a1', '1.6.0a1'),
    )

    def test_each_step_computes_the_intended_version(self):
        for current, how, mode, tag, expected in self.CYCLE:
            with self.subTest(current=current, bump=how, mode=mode, tag=tag):
                self.assertEqual(
                    release_version.compute(current, how, mode, tag), expected)

    def test_the_stale_version_in_main_reaches_the_same_alpha(self):
        # `main` carries 1.4.0 while 1.4.4 is tagged, because the release
        # commits land on the tag. `minor` zeroes the patch, so the staleness
        # does not change this particular answer.
        self.assertEqual(
            release_version.compute('1.4.0', 'minor', 'prerelease', 'a1'),
            release_version.compute('1.4.4', 'minor', 'prerelease', 'a1'))


class ModeTests(unittest.TestCase):
    """Mode decides the suffix, and which inputs are required."""

    def test_dry_run_computes_the_release_version(self):
        self.assertEqual(release_version.compute('1.4.4', 'minor', 'dry-run'),
                         '1.5.0')

    def test_test_mode_keys_a_dev_segment_on_the_run_number(self):
        self.assertEqual(
            release_version.compute('1.4.4', 'minor', 'test', run_number='42'),
            '1.5.0.dev42')

    def test_test_mode_requires_a_run_number(self):
        with self.assertRaises(ValueError):
            release_version.compute('1.4.4', 'minor', 'test')

    def test_prerelease_requires_a_tag(self):
        with self.assertRaises(ValueError):
            release_version.compute('1.4.4', 'minor', 'prerelease')

    def test_a_tag_outside_prerelease_is_rejected(self):
        for mode in ('release', 'test', 'dry-run'):
            with self.subTest(mode=mode), self.assertRaises(ValueError):
                release_version.compute('1.4.4', 'minor', mode, 'b1')

    def test_unknown_mode_is_rejected(self):
        with self.assertRaises(ValueError):
            release_version.compute('1.4.4', 'minor', 'publish')


class PrereleaseTagTests(unittest.TestCase):
    """Only canonical pre-release segments, because the tag has to match PyPI."""

    def test_canonical_tags_are_accepted(self):
        for tag in ('a1', 'b2', 'rc1', 'a0'):
            with self.subTest(tag=tag):
                self.assertTrue(
                    release_version.compute('1.4.4', 'minor', 'prerelease',
                                            tag).endswith(tag))

    def test_leading_zero_is_rejected(self):
        # `b01` normalizes to `b1`, so the git tag and the version PyPI indexes
        # would differ, and only after both were unrecoverable.
        with self.assertRaises(ValueError):
            release_version.compute('1.4.4', 'minor', 'prerelease', 'b01')

    def test_non_canonical_spellings_are_rejected(self):
        for tag in ('alpha1', 'beta1', 'c1', 'a', '1', 'a1.dev0', 'A1'):
            with self.subTest(tag=tag), self.assertRaises(ValueError):
                release_version.compute('1.4.4', 'minor', 'prerelease', tag)


class VersionMustMoveTests(unittest.TestCase):
    """
    The invariant that replaced a ban on input combinations.

    A first draft rejected `prerelease` combined with `none`, reasoning it could re-tag a published
    version. That would also have blocked the ordinary alpha to beta progression, which is most of
    what pre-releases are for. What matters is that the version moves.
    """

    def test_recomputing_the_same_prerelease_is_rejected(self):
        with self.assertRaises(ValueError):
            release_version.compute('1.5.0a1', 'none', 'prerelease', 'a1')

    def test_releasing_the_current_stable_again_is_rejected(self):
        with self.assertRaises(ValueError):
            release_version.compute('1.5.0', 'none', 'release')

    def test_prerelease_with_none_and_a_new_tag_is_allowed(self):
        # The combination the rejected draft would have blocked.
        self.assertEqual(
            release_version.compute('1.5.0a1', 'none', 'prerelease', 'b1'),
            '1.5.0b1')


class CommandLineTests(unittest.TestCase):
    """The workflow reads stdout, so a failure must not print a version."""

    def test_compute_prints_the_version_and_exits_zero(self):
        import contextlib
        import io
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            status = release_version.main([
                'compute', '--current', '1.5.0rc1', '--bump', 'none', '--mode',
                'release'
            ])
        self.assertEqual(status, 0)
        self.assertEqual(out.getvalue().strip(), '1.5.0')

    def test_a_failure_exits_nonzero_with_empty_stdout(self):
        import contextlib
        import io
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            status = release_version.main([
                'compute', '--current', '1.5.0a1', '--bump', 'none', '--mode',
                'prerelease', '--prerelease-tag', 'a1'
            ])
        self.assertEqual(status, 1)
        # Empty, or the workflow captures a half-formed version into a variable.
        self.assertEqual(out.getvalue(), '')
        self.assertIn('::error::', err.getvalue())


if __name__ == '__main__':
    unittest.main()
