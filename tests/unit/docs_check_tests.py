"""Tests for .ci/docs_check.py."""

import pathlib
import sys
import tempfile
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[2] / '.ci'))

import docs_check

CLEAN = ('<p>Routed through <a class="autorefs autorefs-internal" '
         'href="#pika.x">add_callback_threadsafe</a> so that.</p>')

LEAKED_ROLE = '<p>Routed through :meth:<code>add_callback_threadsafe</code>.</p>'

#: What a reference looks like once `docformatter` has wrapped inside the target.
UNPARSED_REF = ('<p>Registered via [<code>add_on_connection_blocked_callback'
                '</code>][pika.connection.Conn ection.add_on_connection_'
                'blocked_callback].</p>')


class RenderedTextTests(unittest.TestCase):

    def test_published_source_is_excluded(self):
        """A `details` block holds the republished docstring, which legitimately repeats whatever
        the rendered copy above it contains.
        """
        page = CLEAN + '<details class="quote">:meth:`x` [y][pika.z]</details>'
        text = docs_check.rendered_text(page)
        self.assertNotIn(':meth:', text)
        self.assertNotIn('][pika.', text)

    def test_entities_are_unescaped_before_matching(self):
        text = docs_check.rendered_text('<p>see :meth:&#96;x&#96;</p>')
        self.assertIn(':meth:', text)


class FindingsTests(unittest.TestCase):

    def _tmpdir(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        return pathlib.Path(tmp.name)

    def _site(self, **pages):
        site = self._tmpdir()
        for name, body in pages.items():
            page = site / name / 'index.html'
            page.parent.mkdir(parents=True)
            page.write_text(body, encoding='utf-8')
        return site

    def test_a_clean_site_reports_nothing(self):
        self.assertEqual(docs_check.findings(self._site(ok=CLEAN)), [])

    def test_a_leaked_sphinx_role_is_reported(self):
        found = docs_check.findings(self._site(bad=LEAKED_ROLE))
        self.assertEqual(len(found), 1)
        self.assertIn('1 sphinx role', found[0])

    def test_the_py_domain_prefix_is_also_caught(self):
        found = docs_check.findings(
            self._site(bad='<p>:py:class:<code>x</code></p>'))
        self.assertEqual(len(found), 1)

    def test_an_unparsed_cross_reference_is_reported(self):
        """This is the shape `--strict` cannot see, because autorefs never recognises it as a
        reference to validate.
        """
        found = docs_check.findings(self._site(bad=UNPARSED_REF))
        self.assertEqual(len(found), 1)
        self.assertIn('1 unparsed cross-reference', found[0])

    def test_every_affected_page_is_listed(self):
        found = docs_check.findings(
            self._site(one=LEAKED_ROLE, two=UNPARSED_REF, three=CLEAN))
        self.assertEqual(len(found), 2)


class MainTests(unittest.TestCase):

    def test_a_clean_site_exits_zero(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        site = pathlib.Path(tmp.name)
        (site / 'p').mkdir()
        (site / 'p' / 'index.html').write_text(CLEAN, encoding='utf-8')
        self.assertEqual(docs_check.main([str(site)]), 0)

    def test_a_dirty_site_exits_one(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        site = pathlib.Path(tmp.name)
        (site / 'p').mkdir()
        (site / 'p' / 'index.html').write_text(LEAKED_ROLE, encoding='utf-8')
        self.assertEqual(docs_check.main([str(site)]), 1)

    def test_a_missing_site_exits_two_rather_than_passing(self):
        """An absent directory must not look like a clean build."""
        self.assertEqual(docs_check.main(['/nonexistent-site']), 2)


if __name__ == '__main__':
    unittest.main()
