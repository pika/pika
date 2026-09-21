"""
Tests for the design-document checker in `design/check_docs.py`.

Every check here is asserted twice: once on an input it must reject, and once
on a near-miss it must accept. Both halves earn their place. Three of the
checker's own checks reached `main` inert, each printing `0 problems` and
exiting 0, and twice the *proof* that a check worked was at fault rather than
the check - a planted typo that landed in prose instead of the manifest, and a
coverage floor watching a counter that a passing and a failing citation both
incremented. The near-miss halves exist because two fixes for those introduced
false positives on correct prose, which is the more expensive failure: a
checker that cries wolf gets switched off.

The counters are asserted too. A check can go inert without changing any
verdict on a clean corpus, so `test_poisoned_base_still_reports` pins the case
no coverage metric detects: a bug that suppresses reports while leaving
resolution intact moves no bucket at all.
"""

import importlib.util
import pathlib
import unittest
from typing import ClassVar

# `design/` is not a package, so the module is loaded by path, the way
# `docs_site_tests.py` loads its own subject.
_MODULE_PATH = (pathlib.Path(__file__).resolve().parents[2] / 'design' /
                'check_docs.py')
_SPEC = importlib.util.spec_from_file_location('check_docs', _MODULE_PATH)
assert _SPEC is not None and _SPEC.loader is not None
check_docs = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(check_docs)

_DOC = pathlib.Path('planted.md')


def _members():
    """Parse the real pika source once, as `main` does."""
    problems: list[str] = []
    members = check_docs.load_members(problems)
    return members, problems


class SymbolCitationTests(unittest.TestCase):
    """Backticked `Class.member` citations against the real source."""

    @classmethod
    def setUpClass(cls):
        cls.members, cls.load_problems = _members()

    def _run(self, text):
        problems: list[str] = []
        check_docs.check_symbols(_DOC, text, self.members, problems)
        return problems

    def test_load_members_reports_nothing_on_the_real_tree(self):
        self.assertEqual(self.load_problems, [])

    def test_typo_on_adapter_member_is_reported(self):
        self.assertTrue(self._run('See `Connection.chanel` here.'))

    def test_real_adapter_member_is_accepted(self):
        self.assertEqual(self._run('See `Connection.channel` here.'), [])

    def test_call_form_with_arguments_is_checked(self):
        # Requiring empty parens left 14 citations of this shape unchecked and
        # absent from the coverage denominator.
        self.assertTrue(self._run('See `Connection.no_such_call(reason)`.'))

    def test_base_qualified_adapter_only_member_is_reported(self):
        # The base-versus-adapter confusion the documents exist to keep
        # straight: `_recovery` is proposed on the adapter, not on the base.
        self.assertTrue(
            self._run('See `pika.connection.Connection._recovery`.'))

    def test_poisoned_base_still_reports(self):
        """
        A typo on the base `Connection` must fail, not be excused.

        `pika.connection.Connection` subclasses `abc.ABC`. When the external-base
        test compared the dotted spelling against a set of bare names it missed
        that, marked the class incomplete, and silently stopped reporting
        anything about it - while every counter stayed byte-identical, which is
        why no coverage bound catches this and this test exists.
        """
        self.assertNotIn('pika.connection.Connection', check_docs.INCOMPLETE)
        self.assertTrue(
            self._run('See `pika.connection.Connection.no_such_member`.'))

    def test_inherited_member_is_accepted(self):
        # `ioloop` is inherited from `BaseConnection`; reporting it was the
        # false positive that the inheritance fold exists to remove.
        self.assertEqual(self._run('See `SelectConnection.ioloop`.'), [])

    def test_unresolvable_base_suppresses_absence_claims(self):
        # `SelectorIOServicesAdapter` inherits from mixins outside `SOURCES`, so
        # absence proves nothing and must not be reported as nonexistence.
        self.assertEqual(
            self._run('See `SelectorIOServicesAdapter.connect_socket`.'), [])

    def test_misspelled_module_is_reported(self):
        self.assertTrue(self._run('See `pika.chanel.Chanel`.'))

    def test_misspelled_class_in_real_module_is_reported(self):
        self.assertTrue(self._run('See `pika.connection.Conection`.'))

    def test_tail_fallback_does_not_verify_a_wrong_module(self):
        # `pika.heartbeat` has no `IOLoop`; resolving through the tail found the
        # adapter's and counted the citation as verified.
        self.assertTrue(
            self._run('See `pika.heartbeat.IOLoop.add_callback_threadsafe`.'))

    def test_nested_frame_class_is_accepted(self):
        self.assertEqual(self._run('See `pika.spec.Basic.Ack`.'), [])

    def test_class_constant_is_accepted(self):
        self.assertEqual(
            self._run('See `pika.connection.Connection.DEFAULT_PORT`.'), [])

    def test_module_level_function_is_accepted(self):
        self.assertEqual(self._run('See `pika.callback.sanitize_prefix`.'), [])

    def test_proposed_class_is_accepted(self):
        self.assertEqual(
            self._run('See `pika.exceptions.ConnectionRecovering`.'), [])

    def test_receiver_expression_is_not_reported(self):
        self.assertEqual(self._run('See `self._ioloop.start`.'), [])

    def test_file_name_is_not_a_citation(self):
        self.assertEqual(self._run('See `select_connection.py`.'), [])

    def test_citation_inside_a_fence_is_skipped(self):
        text = '```python\nch.no_such_member_at_all\n```\n'
        self.assertEqual(self._run(text), [])


class ManifestTests(unittest.TestCase):
    """The file-by-file manifest: bare names, owners and paths."""

    @classmethod
    def setUpClass(cls):
        cls.members, _ = _members()

    def _symbols(self, text):
        problems: list[str] = []
        check_docs.check_manifest_symbols(_DOC, text, self.members, problems)
        return problems

    def _paths(self, text):
        problems: list[str] = []
        check_docs.check_manifest_paths(_DOC, text, problems)
        return problems

    BULLET: ClassVar[
        str] = '- **`pika/adapters/thread_safe_connection.py`**, on `Connection`: '

    def test_bare_name_typo_in_manifest_is_reported(self):
        # Five typos planted here once produced `0 problems`, because the symbol
        # check only sees `Owner.member` and the manifest hoists the owner out.
        self.assertTrue(self._symbols(self.BULLET + 'modified `basic_publsh`.'))

    def test_real_bare_name_is_accepted(self):
        self.assertEqual(self._symbols(self.BULLET + 'modified `close`.'), [])

    def test_proposed_bare_name_is_accepted(self):
        self.assertEqual(self._symbols(self.BULLET + 'new `_transition`.'), [])

    def test_manifest_checked_without_any_heading(self):
        # Gating on heading text made the check collapse on a rename.
        self.assertTrue(self._symbols(self.BULLET + 'modified `basic_publsh`.'))

    def test_manifest_checked_after_a_subheading(self):
        text = '### Deep\n\n' + self.BULLET + 'modified `basic_publsh`.\n'
        self.assertTrue(self._symbols(text))

    def test_early_fence_does_not_exempt_the_manifest(self):
        # The offsets were line-relative but the fence spans absolute, so a
        # fence near the top of a document silently exempted manifest names.
        text = ('```\n' + 'x' * 1200 + '\n```\n\n' + self.BULLET +
                'modified `basic_publsh`.\n')
        self.assertTrue(self._symbols(text))

    def test_bullet_inside_a_fence_is_skipped(self):
        text = '```\n' + self.BULLET + 'modified `basic_publsh`.\n```\n'
        self.assertEqual(self._symbols(text), [])

    def test_missing_file_is_reported(self):
        text = ('- **`pika/adapters/thread_safe_connnection.py`**, on '
                '`Connection`: modified `close`.\n')
        self.assertTrue(self._paths(text))

    def test_existing_file_is_accepted(self):
        self.assertEqual(self._paths(self.BULLET + 'modified `close`.'), [])

    def test_proposed_file_is_accepted(self):
        text = '- **`pika/recovery.py`** (new): the config dataclasses.\n'
        self.assertEqual(self._paths(text), [])


class NameAndPointTests(unittest.TestCase):
    """Manifest sections must name and point, not explain."""

    SECTION: ClassVar[str] = '## Proposed file-by-file changes\n\n'

    def _run(self, text):
        problems: list[str] = []
        check_docs.check_manifest_sections(_DOC, text, problems)
        return problems

    def test_explanation_is_reported(self):
        self.assertTrue(
            self._run(self.SECTION +
                      '- **`a.py`**: it fails because the socket died.\n'))

    def test_explanation_after_a_pointer_is_reported(self):
        # Keeping only the text before the first pointer exempted the whole
        # tail, and these bullets are one long line apiece.
        text = (self.SECTION + '- **`a.py`**: the widget. See "Guard and '
                'exceptions", and it must not do that because of X.\n')
        self.assertTrue(self._run(text))

    def test_pointer_only_bullet_is_accepted(self):
        text = (self.SECTION +
                '- **`a.py`**: the widget. See "Guard and exceptions".\n')
        self.assertEqual(self._run(text), [])

    def test_pointer_verb_inside_a_word_does_not_split(self):
        # Unanchored, the alternation matched inside `proper "`.
        text = (self.SECTION +
                '- **`a.py`**: a proper widget, because it must not break.\n')
        self.assertTrue(self._run(text))

    def test_in_counts_as_a_pointer_verb(self):
        text = (self.SECTION + '- **`a.py`**: the widget, specified in '
                '"Topology replay must not use the blocking wrapper API".\n')
        self.assertEqual(self._run(text), [])

    def test_unrelated_heading_is_not_claimed(self):
        # `startswith` judged `Next steps for the test plan` as a manifest.
        text = ('## Testing\n\n### Next steps for the test plan\n\n'
                'The reason is that replay must be idempotent.\n')
        self.assertEqual(self._run(text), [])

    def test_sibling_subsection_leaves_the_manifest(self):
        text = ('## Plan\n\n### Next steps\n\n- a thing.\n\n'
                '### Something else\n\nIt fails because the socket died.\n')
        self.assertEqual(self._run(text), [])

    def test_subsection_inside_a_manifest_does_not_disable_it(self):
        text = (self.SECTION + '### `pika/connection.py`\n\n'
                '- **`a.py`**: it fails because of X.\n')
        self.assertTrue(self._run(text))


class ListCountTests(unittest.TestCase):
    """A stated count must agree with the list beneath it."""

    def _run(self, text):
        problems: list[str] = []
        check_docs.check_list_counts(_DOC, text, problems)
        return problems

    def test_undercount_is_reported(self):
        self.assertTrue(
            self._run('Two details matter here:\n\n1. a\n2. b\n3. c\n'))

    def test_correct_count_is_accepted(self):
        self.assertEqual(
            self._run('Three details matter here:\n\n1. a\n2. b\n3. c\n'), [])

    def test_bullet_list_is_counted(self):
        self.assertTrue(
            self._run('Two consequences follow:\n\n- a\n- b\n- c\n'))

    def test_adjacent_lists_are_not_merged(self):
        # Skipping blank lines unconditionally merged two lists and called
        # correct markdown defective.
        text = 'Three reasons matter:\n\n1. a\n2. b\n3. c\n\n- x\n- y\n'
        self.assertEqual(self._run(text), [])

    def test_loose_list_of_one_style_is_counted(self):
        self.assertEqual(
            self._run('Three reasons matter:\n\n1. a\n\n2. b\n\n3. c\n'), [])

    def test_pronoun_one_is_not_a_count(self):
        # `One thing remains:` read as the count 1 because `remains` ends in
        # `s` and passed for a plural noun.
        self.assertEqual(self._run('One thing remains:\n\n- a\n- b\n'), [])

    def test_table_is_not_a_list(self):
        self.assertEqual(
            self._run('Two rows matter here:\n\n| a | b |\n|---|---|\n'), [])

    def test_count_inside_a_fence_is_skipped(self):
        self.assertEqual(
            self._run(
                '```\nTwo details matter here:\n\n1. a\n2. b\n3. c\n```\n'), [])


class SpliceTests(unittest.TestCase):
    """A conjunction after a full stop is an orphaned clause."""

    def _run(self, text):
        problems: list[str] = []
        check_docs.check_sentence_splices(_DOC, text, problems)
        return problems

    def test_lowercase_conjunction_after_full_stop_is_reported(self):
        self.assertTrue(self._run('It does this. and then that.\n'))

    def test_capitalised_sentence_start_is_accepted(self):
        self.assertEqual(self._run('It does this. So it works.\n'), [])

    def test_abbreviation_is_accepted(self):
        self.assertEqual(self._run('Tag it (e.g. by comparing identity).\n'),
                         [])

    def test_inline_code_is_skipped(self):
        self.assertEqual(self._run('Use `a. and b` here.\n'), [])


class FenceTests(unittest.TestCase):
    """Fence handling, which has silently exempted whole documents twice."""

    def test_indented_closing_fence_is_seen(self):
        """
        Prose between two blocks must not become a code span.

        Two blocks, not one: with the bug, a single block with an indented
        close simply never closes, so `code_spans` returns nothing and the
        prose is outside every span for the wrong reason. It takes a second
        block to close the first one's dangling state and swallow the prose
        between them - which is also why the fence count stays even and the
        unclosed-fence report never fires. An earlier version of this test
        used one block and passed against the bug.
        """
        text = ('```python\nx = 1\n ```\n\n'
                'prose `Channel.bogus_zz` here\n\n'
                '```python\ny = 2\n```\n')
        spans = check_docs.code_spans(text)
        pos = text.index('`Channel.bogus_zz`')
        self.assertFalse(check_docs.in_code(pos, spans))

    def test_unclosed_fence_is_reported(self):
        problems: list[str] = []
        check_docs.code_spans('```python\nx = 1\n', problems, 'x.md')
        self.assertTrue(problems)

    def test_headings_skip_fenced_content(self):
        text = '# Real H1\n\n```\n## Fake Heading\n```\n\n## Real H2\n'
        heads = check_docs.headings(text)
        self.assertIn('real h1', heads)
        self.assertIn('real h2', heads)
        self.assertNotIn('fake heading', heads)


class NearMissReferenceTests(unittest.TestCase):
    """A quoted string that is nearly a heading is a typo'd pointer."""

    HEADS: ClassVar[set] = {
        'skip-and-continue must reopen the channel', 'guard and exceptions'
    }

    def _run(self, text):
        problems: list[str] = []
        check_docs.check_near_miss_refs(_DOC, text, problems, self.HEADS)
        return problems

    def test_near_miss_is_reported(self):
        # Caught wherever it sits, including after introducers the verb-led
        # check never reaches.
        text = 'the premise of "Skip-and-continue must reopen the chanel"\n'
        self.assertTrue(self._run(text))

    def test_exact_heading_is_accepted(self):
        self.assertEqual(
            self._run('see "Skip-and-continue must reopen the channel"\n'), [])

    def test_ordinary_quoted_prose_is_accepted(self):
        text = 'its docstring reads "Raise if the connection is known closed"\n'
        self.assertEqual(self._run(text), [])


class TestPlanTests(unittest.TestCase):
    """Every numbered test must be scheduled in some phase."""

    PLAN: ClassVar[
        str] = '1. `TestAlpha` - does a thing.\n2. `TestBeta` - another.\n\n'

    def _run(self, text):
        problems: list[str] = []
        check_docs.check_tests_are_phased(_DOC, text, problems)
        return problems

    def test_unscheduled_test_is_reported(self):
        text = self.PLAN + '## Next steps\n\n- Integration: `TestAlpha`.\n'
        self.assertTrue(self._run(text))

    def test_all_scheduled_is_accepted(self):
        text = self.PLAN + '## Next steps\n\n- `TestAlpha`, `TestBeta`.\n'
        self.assertEqual(self._run(text), [])

    def test_no_phase_section_is_accepted(self):
        self.assertEqual(self._run(self.PLAN), [])


class OpenQuestionsPlacementTests(unittest.TestCase):
    """The decisions a reader owes an answer to go first, always."""

    def _run(self, text):
        problems: list[str] = []
        check_docs.check_open_questions_first(_DOC, text, problems)
        return problems

    def test_first_section_is_accepted(self):
        self.assertEqual(
            self._run('# T\n\n## Open questions\n\n- a\n\n## Other\n'), [])

    def test_second_section_is_reported(self):
        self.assertTrue(
            self._run('# T\n\n## Intro\n\n## Open questions\n\n- a\n'))

    def test_last_section_is_reported(self):
        # Where it sat before, and where it drifts back to one insertion at a
        # time if nothing enforces this.
        self.assertTrue(
            self._run('# T\n\n## A\n\n## B\n\n## Open questions\n\n- a\n'))

    def test_document_without_the_section_is_accepted(self):
        self.assertEqual(self._run('# T\n\n## A\n\n## B\n'), [])

    def test_heading_inside_a_fence_is_ignored(self):
        self.assertEqual(
            self._run('# T\n\n## A\n\n```\n## Open questions\n```\n'), [])

    def test_the_real_documents_comply(self):
        design = pathlib.Path(_MODULE_PATH).parent / 'connection-recovery'
        for doc in sorted(design.glob('*.md')):
            with self.subTest(doc=doc.name):
                self.assertEqual(self._run(doc.read_text(encoding='utf-8')), [])


class StaleDeclarationTests(unittest.TestCase):
    """`PROPOSED` entries assert absence, so a landed one must be reported."""

    def test_landed_proposed_member_is_reported(self):
        original = set(check_docs.PROPOSED)
        check_docs.PROPOSED.add('Connection.channel')
        try:
            problems: list[str] = []
            check_docs.load_members(problems)
            self.assertTrue([p for p in problems if 'PROPOSED' in p])
        finally:
            check_docs.PROPOSED.clear()
            check_docs.PROPOSED.update(original)

    def test_clean_proposed_set_reports_nothing(self):
        problems: list[str] = []
        check_docs.load_members(problems)
        self.assertEqual([p for p in problems if 'PROPOSED' in p], [])


class CoverageAccountingTests(unittest.TestCase):
    """The coverage line must be a breakdown, and must not mislead."""

    @classmethod
    def setUpClass(cls):
        cls.members, _ = _members()

    def _tally(self, text):
        tally = {
            'seen': 0,
            'checked': 0,
            'unresolved': 0,
            'skipped_constant': 0,
            'ambiguous': 0,
            'proposed': 0,
            'failed': 0,
        }
        problems: list[str] = []
        check_docs.check_symbols(_DOC, text, self.members, problems, tally)
        return tally

    def test_failing_citation_counts_as_failed_not_verified(self):
        # Incrementing `checked` before the membership test meant a failure was
        # counted as verified, so a floor built on it could not detect anything.
        tally = self._tally('See `Connection.chanel`.')
        self.assertEqual(tally['checked'], 0)
        self.assertEqual(tally['failed'], 1)

    def test_passing_citation_counts_as_verified(self):
        tally = self._tally('See `Connection.channel`.')
        self.assertEqual(tally['checked'], 1)
        self.assertEqual(tally['failed'], 0)

    def test_buckets_sum_to_seen(self):
        text = ('`Connection.channel` `Connection.chanel` `Basic.Ack` '
                '`Connection._transition` `self._x.y` `foo.py`')
        tally = self._tally(text)
        buckets = (tally['checked'] + tally['failed'] + tally['unresolved'] +
                   tally['skipped_constant'] + tally['ambiguous'] +
                   tally['proposed'])
        self.assertEqual(buckets, tally['seen'])


if __name__ == '__main__':
    unittest.main()
