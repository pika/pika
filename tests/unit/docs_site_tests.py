"""Tests for the documentation-site deploy helper in `.ci/docs_site.py`."""

import importlib.util
import io
import json
import pathlib
import unittest

# `.ci` is not a package and its name is not a valid identifier, so the module
# is loaded by path rather than imported.
_MODULE_PATH = (pathlib.Path(__file__).resolve().parents[2] / '.ci' /
                'docs_site.py')
_SPEC = importlib.util.spec_from_file_location('docs_site', _MODULE_PATH)
assert _SPEC is not None and _SPEC.loader is not None
docs_site = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(docs_site)


def _versions(*entries):
    """
    Build a `versions.json`-shaped list.

    :param entries: (version, aliases) pairs.
    """
    return [{
        'version': version,
        'title': version,
        'aliases': list(aliases)
    } for version, aliases in entries]


class LoadVersionsTests(unittest.TestCase):

    def test_parses_a_list(self):
        payload = json.dumps(_versions(('1.5', ['latest'])))
        self.assertEqual(
            docs_site.load_versions(io.StringIO(payload))[0]['version'], '1.5')

    def test_empty_list_is_valid(self):
        self.assertEqual(docs_site.load_versions(io.StringIO('[]')), [])

    def test_empty_payload_raises(self):
        """
        An empty payload means the mike command failed.

        `mike list --json` prints `[]` even when the branch is absent, so treating empty as "nothing
        published" would hand over the alias on any error.
        """
        with self.assertRaisesRegex(ValueError, 'did the mike command fail'):
            docs_site.load_versions(io.StringIO('   \n'))

    def test_invalid_json_raises(self):
        with self.assertRaises(json.JSONDecodeError):
            docs_site.load_versions(io.StringIO('error: could not read'))

    def test_non_list_raises(self):
        with self.assertRaises(TypeError):
            docs_site.load_versions(io.StringIO('{"version": "1.5"}'))


class AliasHolderTests(unittest.TestCase):

    def test_finds_the_holder(self):
        versions = _versions(('dev', []), ('1.6', ['latest']), ('1.5', []))
        self.assertEqual(docs_site.alias_holder(versions, 'latest'), '1.6')

    def test_absent_alias_is_empty(self):
        self.assertEqual(
            docs_site.alias_holder(_versions(('1.5', [])), 'latest'), '')


class ShouldMoveAliasTests(unittest.TestCase):

    def _decide(self, versions, version, alias='latest'):
        return docs_site.should_move_alias(versions, version, alias)[0]

    def test_takes_an_unheld_alias(self):
        """The first deploy bootstraps the alias and the site-root redirect."""
        self.assertTrue(self._decide([], 'dev'))

    def test_refreshes_the_current_holder(self):
        self.assertTrue(self._decide(_versions(('1.5', ['latest'])), '1.5'))

    def test_release_supersedes_the_dev_bootstrap(self):
        self.assertTrue(self._decide(_versions(('dev', ['latest'])), '1.5'))

    def test_dev_never_takes_the_alias_back_from_a_release(self):
        """
        A push to `main` must not point `latest` at unreleased docs.

        This is the case a `sort -V` comparison got wrong, since it ranks `dev` above every numeric
        version.
        """
        self.assertFalse(self._decide(_versions(('1.5', ['latest'])), 'dev'))

    def test_newer_release_takes_the_alias(self):
        self.assertTrue(self._decide(_versions(('1.5', ['latest'])), '1.6'))

    def test_older_release_does_not_take_the_alias(self):
        self.assertFalse(self._decide(_versions(('1.6', ['latest'])), '1.5'))

    def test_compares_numerically_not_lexically(self):
        """1.10 is newer than 1.9, which a string comparison gets backwards."""
        self.assertTrue(self._decide(_versions(('1.9', ['latest'])), '1.10'))
        self.assertFalse(self._decide(_versions(('1.10', ['latest'])), '1.9'))

    def test_prerelease_never_takes_the_alias(self):
        """
        `latest` points at released docs only.

        It drives the site root and every `latest/` URL compiled into a released wheel.
        """
        for candidate in ('1.6.0rc1', '1.6.0b1', '1.6.0a1'):
            self.assertFalse(self._decide(_versions(('1.5', ['latest'])),
                                          candidate),
                             msg=candidate)

    def test_prerelease_does_not_take_an_unheld_alias_either(self):
        self.assertFalse(self._decide([], '1.6.0rc1'))

    def test_stable_takes_the_alias_from_its_own_prerelease(self):
        """
        The release must be able to reclaim `latest` from its release candidate.

        `sort -V` ranked `1.5.0rc1` above `1.5`, so the stable release was refused the alias and the
        site root stayed on a release candidate.
        """
        self.assertTrue(self._decide(_versions(('1.5.0rc1', ['latest'])),
                                     '1.5'))

    def test_uncomparable_holder_raises_rather_than_guessing(self):
        with self.assertRaises(ValueError):
            docs_site.should_move_alias(_versions(('nonsense', ['latest'])),
                                        '1.5', 'latest')

    def test_unparsable_candidate_is_refused_even_on_an_empty_site(self):
        """
        A typo must not take the alias by falling through the bootstrap rule.

        Deciding eligibility by parsing rather than by rule order is what closes this: previously
        `1.5.0rcl` reached "nothing holds it yet" and took the alias.
        """
        for name in ('1.5.0rcl', '1.5.0GA', 'nonsense', '1.05'):
            self.assertFalse(self._decide([], name), msg=name)

    def test_unheld_alias_on_a_populated_site_is_refused(self):
        """
        An empty version list is the only evidence of a fresh site.

        `mike` reports no versions when `versions.json` is missing as well as when the branch is
        absent, so a half-rebuilt site would otherwise read as a bootstrap and hand `latest` to
        `dev`.
        """
        populated = _versions(('1.5', []), ('1.6', []))
        self.assertFalse(self._decide(populated, 'dev'))
        self.assertFalse(self._decide(populated, '1.6'))

    def test_equal_versions_spelled_differently_are_refused(self):
        """
        `1.6.0` and `1.6` compare equal but are separate directories to `mike`.

        Publishing the second would leave two full trees for one release, with the alias on
        whichever deployed last and every inbound link pointing at the frozen one.
        """
        self.assertFalse(self._decide(_versions(('1.6', ['latest'])), '1.6.0'))
        self.assertFalse(self._decide(_versions(('1.6.0', ['latest'])), '1.6'))

    def test_epoch_and_local_versions_are_refused(self):
        """
        An epoch outranks every ordinary release, so nothing could supersede it.

        Both would also become directory names containing `!` or `+`.
        """
        for name in ('1!1.0', '1.5+local'):
            self.assertFalse(self._decide(_versions(('9999.0', ['latest'])),
                                          name),
                             msg=name)


class ParseReleaseTests(unittest.TestCase):

    def test_accepts_a_canonical_stable_release(self):
        self.assertIsNotNone(docs_site.parse_release('1.5'))
        self.assertIsNotNone(docs_site.parse_release('1.5.3'))

    def test_rejects_prereleases_and_postreleases(self):
        for name in ('1.5.0rc1', '1.5.0b1', '1.5.0a1', '1.5.0.post1',
                     '1.5.0.dev1'):
            self.assertIsNone(docs_site.parse_release(name), msg=name)

    def test_rejects_unparsable_names(self):
        """
        A typo must not read as a stable release.

        `1.5.0rcl` is a finger-slip for `1.5.0rc1`; answering False to "is this a pre-release" made
        it eligible for the alias.
        """
        for name in ('1.5.0rcl', '1.5.0GA', '1.5.0final', 'nonsense', 'dev'):
            self.assertIsNone(docs_site.parse_release(name), msg=name)

    def test_rejects_non_canonical_spellings(self):
        """
        `1.05` equals `1.5` to `packaging` but is a distinct directory to `mike`.

        Accepting both would publish two trees that compare equal and fight over the alias.
        """
        self.assertIsNone(docs_site.parse_release('1.05'))
        self.assertIsNone(docs_site.parse_release('1.5.0-1'))


class CheckVersionNameTests(unittest.TestCase):

    def test_accepts_the_publishable_forms(self):
        for name in ('dev', '1.5', '1.5.3', '1.6.0rc1', '1.6.0b2'):
            docs_site.check_version_name(name)

    def test_rejects_post_and_dev_releases(self):
        for name in ('1.5.0.post1', '1.5.0.dev1'):
            with self.assertRaises(ValueError, msg=name):
                docs_site.check_version_name(name)

    def test_rejects_epoch_and_local_versions(self):
        for name in ('1!1.0', '1.5+local'):
            with self.assertRaises(ValueError, msg=name):
                docs_site.check_version_name(name)

    def test_rejects_non_canonical_and_unparsable(self):
        for name in ('1.05', '1.5.0rcl', 'wip-1617', ''):
            with self.assertRaises(ValueError, msg=name):
                docs_site.check_version_name(name)


class AliasDecisionCommandTests(unittest.TestCase):
    """
    The CLI contract, which the workflow depends on literally.

    The shell reads stdout and compares it against the string `true`, so anything else means the
    deploy silently stops passing `--update-aliases` and `latest` freezes on whatever held it.
    """

    def _run(self, payload, version, alias='latest'):
        argv = ['alias-decision', '--version', version, '--alias', alias]
        out, err = io.StringIO(), io.StringIO()
        original = (docs_site.sys.stdin, docs_site.sys.stdout,
                    docs_site.sys.stderr)
        docs_site.sys.stdin = io.StringIO(payload)
        docs_site.sys.stdout, docs_site.sys.stderr = out, err
        try:
            code = docs_site.main(argv)
        finally:
            (docs_site.sys.stdin, docs_site.sys.stdout,
             docs_site.sys.stderr) = original
        return code, out.getvalue(), err.getvalue()

    def test_prints_exactly_true(self):
        code, out, _ = self._run('[]', 'dev')
        self.assertEqual(code, 0)
        self.assertEqual(out, 'true\n')

    def test_prints_exactly_false(self):
        payload = json.dumps(_versions(('1.5', ['latest'])))
        code, out, _ = self._run(payload, 'dev')
        self.assertEqual(code, 0)
        self.assertEqual(out, 'false\n')

    def test_reason_goes_to_stderr_not_stdout(self):
        """
        The verdict is the only thing on stdout.

        The caller captures it in a shell variable, so anything else printed there corrupts the
        comparison, and anything printed there instead of stderr vanishes from the log.
        """
        payload = json.dumps(_versions(('1.5', ['latest'])))
        _, out, err = self._run(payload, 'dev')
        self.assertEqual(out.strip(), 'false')
        self.assertIn('latest', err)

    def test_unreadable_payload_exits_nonzero_without_printing_a_verdict(self):
        """
        Failing closed means failing, not answering `true`.

        Treating an unreadable payload as "nothing holds the alias" is what handed the alias over on
        any error.
        """
        for payload in ('', 'error: could not read', '{"not": "a list"}'):
            code, out, err = self._run(payload, 'dev')
            self.assertEqual(code, 1, msg=payload)
            self.assertEqual(out, '', msg=payload)
            self.assertIn('::error::', err, msg=payload)


class VerifyTests(unittest.TestCase):

    def _run(self, versions, version, aliases=''):
        argv = ['verify', '--version', version, '--aliases', aliases]
        original = (docs_site.sys.stdin, docs_site.sys.stdout,
                    docs_site.sys.stderr)
        docs_site.sys.stdin = io.StringIO(json.dumps(versions))
        # Captured, not merely redirected: `_cmd_verify` prints `::error::`
        # annotations, which GitHub parses as workflow commands if they reach the
        # unit job's real stdout.
        docs_site.sys.stdout, docs_site.sys.stderr = io.StringIO(), io.StringIO(
        )
        try:
            return docs_site.main(argv)
        finally:
            (docs_site.sys.stdin, docs_site.sys.stdout,
             docs_site.sys.stderr) = original

    def test_published_with_alias_passes(self):
        self.assertEqual(
            self._run(_versions(('1.5', ['latest'])), '1.5', 'latest'), 0)

    def test_missing_version_fails(self):
        self.assertEqual(
            self._run(_versions(('1.5', ['latest'])), '1.6', 'latest'), 1)

    def test_missing_alias_fails(self):
        """
        `mike` skips its push when a deploy produces no change, and still exits 0.

        The alias assertion is what catches a deploy that silently did not publish.
        """
        self.assertEqual(self._run(_versions(('1.5', [])), '1.5', 'latest'), 1)

    def test_unreadable_payload_fails(self):
        self.assertEqual(self._run_raw('', ['verify', '--version', '1.5']), 1)

    def test_no_aliases_expected_is_the_steady_state(self):
        """
        Every `dev` push after the first verifies with no alias expectation.

        A change to how the alias list is split would break only this path, which is the one the
        automated deploy takes most often.
        """
        self.assertEqual(self._run(_versions(('dev', [])), 'dev'), 0)

    def _run_raw(self, payload, argv):
        original = (docs_site.sys.stdin, docs_site.sys.stdout,
                    docs_site.sys.stderr)
        docs_site.sys.stdin = io.StringIO(payload)
        docs_site.sys.stdout, docs_site.sys.stderr = io.StringIO(), io.StringIO(
        )
        try:
            return docs_site.main(argv)
        finally:
            (docs_site.sys.stdin, docs_site.sys.stdout,
             docs_site.sys.stderr) = original

    def test_check_version_cli_gates_by_exit_code(self):
        """
        The workflow relies on the exit status, which nothing else asserted.

        Wrapping the command in `except ValueError: return 0` would silently disable the input gate.
        """
        self.assertEqual(
            self._run_raw('', ['check-version', '--version', '1.6']), 0)
        self.assertEqual(
            self._run_raw('', ['check-version', '--version', '1.05']), 1)


if __name__ == '__main__':
    unittest.main()
