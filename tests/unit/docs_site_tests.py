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
        with self.assertRaises(ValueError):
            docs_site.load_versions(io.StringIO('   \n'))

    def test_invalid_json_raises(self):
        with self.assertRaises(json.JSONDecodeError):
            docs_site.load_versions(io.StringIO('error: could not read'))

    def test_non_list_raises(self):
        with self.assertRaises(ValueError):
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
            self.assertNotIn('true', out, msg=payload)
            self.assertIn('::error::', err, msg=payload)


class VerifyTests(unittest.TestCase):

    def _run(self, versions, version, aliases=''):
        argv = ['verify', '--version', version, '--aliases', aliases]
        stdin = io.StringIO(json.dumps(versions))
        original = docs_site.sys.stdin
        docs_site.sys.stdin = stdin
        try:
            return docs_site.main(argv)
        finally:
            docs_site.sys.stdin = original

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
        stdin = io.StringIO('')
        original = docs_site.sys.stdin
        docs_site.sys.stdin = stdin
        try:
            self.assertEqual(docs_site.main(['verify', '--version', '1.5']), 1)
        finally:
            docs_site.sys.stdin = original


if __name__ == '__main__':
    unittest.main()
