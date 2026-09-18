"""
Check the design documents against the invariants reviews keep catching.

Run from the repository root::

    python3 design/check_docs.py

Exits non-zero and prints one line per problem, then a coverage line.

**Read the coverage line.** This tool does not verify most of what the
documents assert, and two review passes over-trusted it because "0
problems" reads like "everything verified". It reports what it actually
checked so that cannot happen again. Current coverage of symbol citations
is roughly a fifth: resolving a receiver like ``self._channel._state`` or
``ch.is_open`` needs type inference this does not attempt, and members
whose names start with a capital are skipped because the AMQP frame
namespace collides with pika's class names.

What it does check, reliably:

1. Backticked ``Class.member`` citations whose owner resolves to a class in
   ``SOURCES``, against members parsed from the real source. Bare
   ``Connection``/``Channel`` resolve to the adapter per the glossary; a
   base-qualified citation of an adapter-only member fails, which is the
   base-versus-adapter confusion these documents exist to keep straight.
2. ``file:line`` citations, including both ends of a range, against real
   file lengths.
3. Every quoted section name after see/under/per/in/from, against headings
   pooled across the whole tree.
4. Fenced Python blocks parse. Syntax only - a block may still fail at
   import on undefined names, which ``compile()`` cannot see.
5. Markdown hygiene: ASCII only, no trailing whitespace, exactly one H1,
   no hard-wrapped paragraphs, no unclosed fences.
6. Manifest sections name and point rather than explain, so a correction
   made in the prose cannot go stale in the sections an implementer
   builds from.
7. A sentence stating a count agrees with the list beneath it. Adding an
   item and leaving the count alone is a defect a review pass found.
8. No sentence begins with a conjunction after a full stop, which is the
   seam left by inserting text into the middle of a paragraph and
   orphaning the clause that followed.

Every check has been proven to fail on planted defects, several per check.
That matters more than it sounds: the cross-reference check was dead on
arrival, and only planting a defect revealed it.
"""

import ast
import pathlib
import re
import sys

# Sentinel for a bare class name that two modules both define, so a
# citation using it cannot be resolved and must be qualified.
AMBIGUOUS = {'<ambiguous>'}

# The documents' own convention, stated in the glossary: an unqualified
# `Connection` or `Channel` means the adapter class. Encode that rather than
# demanding qualification everywhere, which would fight the prose. Citing a
# base class still requires the module prefix, and a base-qualified citation
# of an adapter-only member therefore fails, which is the case worth catching.
ADAPTER_MODULE = 'pika/adapters/thread_safe_connection.py'
BARE_DEFAULTS = {
    'Connection': ADAPTER_MODULE[:-3].replace('/', '.') + '.Connection',
    'Channel': ADAPTER_MODULE[:-3].replace('/', '.') + '.Channel',
}

ROOT = pathlib.Path(__file__).resolve().parent.parent
DESIGN = ROOT / 'design'

# Modules whose classes documents are allowed to cite. Kept explicit so a
# typo in a module name is a failure rather than a silent skip.
SOURCES = [
    'pika/adapters/thread_safe_connection.py',
    'pika/adapters/select_connection.py',
    'pika/adapters/base_connection.py',
    'pika/adapters/utils/selector_ioloop_adapter.py',
    'pika/connection.py',
    'pika/channel.py',
    'pika/exceptions.py',
    'pika/heartbeat.py',
    'pika/callback.py',
]

# Members these documents propose adding. Listing one here is a deliberate
# declaration that it does not exist yet; anything else missing is an error.
# Keep it short - a long list means the design has drifted from the code.
PROPOSED = {
    'Connection._state',
    'Connection._recovery',
    'Connection._ioloop',
    'Connection._recover_connection',
    'Connection._try_reconnect_once',
    'Connection._reopen_channels_and_recover_topology',
    'Connection._reopen_channel',
    'Connection._recover_topology',
    'Connection._recover_channel',
    'Connection._on_channel_closed_for_recovery',
    'Connection._on_redial_open',
    'Connection._on_redial_error',
    'Connection._transition',
    'Channel._state',
    'Channel._closed',
    'Channel._delivery_tag_offset',
    'Channel._max_seen_delivery_tag',
    'Channel._make_delivery_callback',
    'Channel._register_recovery_close_listener',
}

# Names that look like Class.member but are not ours to check: reference
# clients, other languages, and dotted prose.
FOREIGN = re.compile(
    r'^(?:AutorecoveringChannel|AutorecoveringConnection|RecoveryAware\w*'
    r'|ChannelN|AMQConnection|Utility|RecordedConsumer|QosConfig|Connection\.'
    r'topologyConfiguration|amqp091|struct|copy|enum|dataclasses|typing'
    r'|functools|threading|collections|heapq)$')


def load_members(problems):
    """
    Map each qualified class name to its attributes and methods.

    Keys are ``module.Class`` *and* bare ``Class`` where the bare name is
    unambiguous. Merging the two ``Connection`` classes into one namespace
    would defeat the check entirely, since a member of either would satisfy
    a citation of the other - the adapter-versus-base confusion these
    documents exist to keep straight.
    """
    by_qualified: dict[str, set[str]] = {}
    for rel in SOURCES:
        path = ROOT / rel
        if not path.exists():
            problems.append(f'check_docs: SOURCES entry {rel} does not exist')
            continue
        tree = ast.parse(path.read_text(encoding='utf-8'), filename=str(path))
        for node in ast.walk(tree):
            if not isinstance(node, ast.ClassDef):
                continue
            mod = rel[:-3].replace('/', '.')
            found = by_qualified.setdefault(f'{mod}.{node.name}', set())
            # Direct children only for class-level assignments: walking the
            # whole subtree pulls method locals in as members, which made
            # `Connection.offset` and `Connection.deadline` pass.
            for stmt in node.body:
                if isinstance(stmt, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    found.add(stmt.name)
                    for sub in ast.walk(stmt):
                        if (isinstance(sub, ast.Attribute) and
                                isinstance(sub.value, ast.Name) and
                                sub.value.id == 'self'):
                            found.add(sub.attr)
                elif isinstance(stmt, ast.AnnAssign) and isinstance(
                        stmt.target, ast.Name):
                    found.add(stmt.target.id)
                elif isinstance(stmt, ast.Assign):
                    for tgt in stmt.targets:
                        if isinstance(tgt, ast.Name):
                            found.add(tgt.id)
    # A stale BARE_DEFAULTS target silently disables the check for every bare
    # citation, which is most of them, so it is validated like SOURCES is.
    for bare_name, target in BARE_DEFAULTS.items():
        if target not in by_qualified:
            problems.append(
                f'check_docs: BARE_DEFAULTS maps {bare_name} to {target}, '
                f'which is not a class in SOURCES; bare citations would go '
                f'unchecked')
    # Add bare names only where they are unambiguous.
    bare: dict[str, list[str]] = {}
    for qual in by_qualified:
        bare.setdefault(qual.rsplit('.', 1)[-1], []).append(qual)
    members = dict(by_qualified)
    for name, quals in bare.items():
        if len(quals) == 1:
            members[name] = by_qualified[quals[0]]
        else:
            members[name] = AMBIGUOUS
    return members


def code_spans(text, problems=None, name=''):
    """
    Character ranges covered by fenced code blocks.

    Line-anchored deliberately: an unanchored ``r'```.*?```'`` also pairs
    inline triple backticks, and one of those in prose desynchronises the
    pairing so that most of the document is treated as code and silently
    exempted from the symbol check.
    """
    spans = []
    start = None
    pos = 0
    fences = 0
    for line in text.split('\n'):
        if line.startswith('```'):
            fences += 1
            if start is None:
                start = pos
            else:
                spans.append((start, pos + len(line)))
                start = None
        pos += len(line) + 1
    if start is not None and problems is not None:
        problems.append(f'{name}: unclosed code fence ({fences} fence lines)')
    return spans


def in_code(pos, spans):
    return any(a <= pos < b for a, b in spans)


def check_symbols(path, text, members, problems, tally=None):
    spans = code_spans(text, problems, path.name)
    for m in re.finditer(r'`([A-Za-z_][\w.]*)\.(\w+)`', text):
        if in_code(m.start(), spans):
            continue
        owner, member = m.group(1), m.group(2)
        if FOREIGN.match(owner) or owner.endswith('.py'):
            continue
        # `Channel.Close`, `Basic.Ack`, `Connection.Blocked` and friends are
        # AMQP method frames, not Python members; the protocol namespace
        # collides with our class names. Tested before resolution so a frame
        # name never produces a spurious namespace complaint.
        if tally is not None:
            tally['seen'] += 1
        if member[:1].isupper():
            if tally is not None:
                tally['skipped_constant'] += 1
            continue
        # The stated convention wins over the ambiguity sentinel: a bare
        # `Connection` is the adapter one by definition, not an unresolvable
        # collision.
        if owner in BARE_DEFAULTS:
            key = BARE_DEFAULTS[owner]
        elif owner in members:
            key = owner
        else:
            key = owner.split('.')[-1]
        if key not in members:
            if tally is not None:
                tally['unresolved'] += 1
            continue
        if members[key] is AMBIGUOUS:
            line = text[:m.start()].count('\n') + 1
            problems.append(
                f'{path.name}:{line}: `{owner}.{member}` uses a bare class '
                f'name defined in more than one module; qualify it')
            continue
        # Match PROPOSED on the *resolved* key, not the bare class name:
        # matching bare re-merges the namespaces and exempts every proposed
        # member on the base classes too, which is the case worth catching.
        cls = key.rsplit('.', 1)[-1]
        adapter = key.startswith('pika.adapters.thread_safe_connection.')
        if adapter and f'{cls}.{member}' in PROPOSED:
            continue
        if tally is not None:
            tally['checked'] += 1
        if member not in members[key]:
            line = text[:m.start()].count('\n') + 1
            problems.append(
                f'{path.name}:{line}: `{owner}.{member}` does not exist on '
                f'{key} (members are read from the real source)')


def check_file_lines(path, text, problems):
    # Capture an optional range end: citing `foo.py:176-177` and checking
    # only 176 leaves the load-bearing half unverified.
    for m in re.finditer(r'`?([\w/\\.]+\.py):(\d+)(?:-(\d+))?', text):
        rel = m.group(1)
        lineno = max(int(m.group(2)), int(m.group(3) or 0))
        target = ROOT / rel
        if not target.exists() and '/' not in rel:
            # A bare filename may be a proposed file or a real one cited
            # without its directory; resolve it when exactly one match exists
            # so the citation is checked rather than skipped.
            matches = [q for q in ROOT.rglob(rel) if '.git' not in q.parts]
            if len(matches) == 1:
                target = matches[0]
        if not target.exists():
            if '/' in rel:
                line = text[:m.start()].count('\n') + 1
                problems.append(f'{path.name}:{line}: cited file {rel} '
                                f'does not exist')
            continue
        length = len(target.read_text(encoding='utf-8').splitlines())
        if lineno > length:
            line = text[:m.start()].count('\n') + 1
            problems.append(f'{path.name}:{line}: cited {rel}:{lineno} but '
                            f'that file has {length} lines')


def check_crossrefs(path, text, problems, heads):
    spans = code_spans(text)
    # `re.I` matters: sentence-initial `See "..."` is the dominant form and
    # a case-sensitive pattern missed ten of thirty-eight references here,
    # two of which were genuinely dangling. `\b` stops `within "..."`
    # matching through the `in`.
    # Validate every quoted name in a run, not just the first: pointer lines
    # legitimately list several sections after one `under`, and checking only
    # the first left 27 of 85 real references unverified.
    pattern = (r'\b(?:see|under|per|in|from)\s+'
               r'((?:"[^"]{4,110}"(?:\s*,\s*|\s+and\s+)?)+)')
    for m in re.finditer(pattern, text, re.IGNORECASE):
        if in_code(m.start(), spans):
            continue
        for quoted in re.findall(r'"([^"]{4,110})"', m.group(1)):
            check_one_ref(path, text, m.start(), quoted, heads, problems)


def check_one_ref(path, text, pos, quoted, heads, problems):
    """
    Validate a single quoted section name against the pooled headings.

    A quoted string only counts as a section reference when it reads like a
    title, judged on the first *alphabetic* character rather than the first
    character: two real headings begin with a backticked identifier, and
    testing the raw first character skipped every reference to them.
    """
    raw = quoted.strip().rstrip('.')
    ref = raw.replace('`', '').lower()
    if ref in heads:
        return
    # A leading code span counts as title-like on its own: two real headings
    # start with a lowercase backticked identifier, so neither the raw first
    # character nor the first alphabetic one identifies them as titles.
    if not raw.startswith('`'):
        letters = [c for c in raw if c.isalpha()]
        if not letters or not letters[0].isupper():
            return
    line = text[:pos].count('\n') + 1
    problems.append(f'{path.name}:{line}: cross-reference "'
                    f'{quoted}" matches no heading')


def check_python_blocks(path, text, problems):
    # `[ \t]*` after the language tag: one trailing space silently disabled
    # this whole check. No preamble is injected - it broke any block carrying
    # its own `from __future__` import, which is the import these documents
    # tell the implementer to add, and bought nothing because undefined names
    # are not syntax errors anyway.
    pattern = r'```python[ \t]*\n(.*?)```'
    for i, m in enumerate(re.finditer(pattern, text, re.DOTALL), 1):
        block = m.group(1)
        try:
            compile(block, f'<{path.name} block {i}>', 'exec')
        except SyntaxError as exc:
            fence_line = text[:m.start()].count('\n') + 1
            inner = (exc.lineno or 1)
            problems.append(
                f'{path.name}:{fence_line + inner}: python block {i} does '
                f'not compile: {exc.msg}')


# Sections that are manifests or orderings rather than descriptions. They may
# name symbols, files, tests and section references; they may not describe
# behaviour. Four review passes found the same corrections landed in the prose
# and left stale in these sections, so the separation is enforced rather than
# merely intended. "One canonical location per fact" is the rule; this is the
# only mechanical part of it that can be checked.
MANIFEST_SECTIONS = (
    'Proposed file-by-file changes',
    'Next steps',
)

# Words that only appear when a sentence is explaining how something works.
# Deliberately narrow: a manifest legitimately says "gains", "lands", "new".
BEHAVIOUR_WORDS = re.compile(
    r'\b(?:because|therefore|so that|otherwise|would|must not|cannot|'
    r'instead of|rather than the|which means|the reason)\b', re.IGNORECASE)


def check_manifest_sections(path, text, problems):
    """Fail if a manifest section explains behaviour instead of pointing."""
    section = ''
    fenced = False
    for n, line in enumerate(text.split('\n'), 1):
        if line.startswith('```'):
            fenced = not fenced
            continue
        if fenced:
            continue
        head = re.match(r'^#{2,4}\s+(.*)$', line)
        if head:
            section = head.group(1).strip()
            continue
        if not any(section.startswith(s) for s in MANIFEST_SECTIONS):
            continue
        # A line carrying a section reference is a pointer, which is the
        # whole point of these sections.
        if re.search(r'(?:see|under|defined under|per)\s+"', line,
                     re.IGNORECASE):
            continue
        hit = BEHAVIOUR_WORDS.search(line)
        if hit:
            problems.append(
                f'{path.name}:{n}: "{section}" is a manifest; it should name '
                f'and point, not explain ("{hit.group(0)}")')


NUMBER_WORDS = {
    'one': 1,
    'two': 2,
    'three': 3,
    'four': 4,
    'five': 5,
    'six': 6,
    'seven': 7,
    'eight': 8,
    'nine': 9,
    'ten': 10,
    'eleven': 11,
    'twelve': 12,
}

WORD = re.compile(r'\b(' + '|'.join(NUMBER_WORDS) + r')\b', re.IGNORECASE)
ITEM = re.compile(r'^(?:\d+\.|[-*+])\s')

# A number word only states a count when a plural noun follows it. Without
# this, "a public-contract question rather than a mechanism one:" reads as
# the count 1, which is the pronoun sense.
PLURAL = re.compile(r'^[a-z-]{4,}s$')
NOT_PLURAL = frozenset({
    'across',
    'always',
    'others',
    'takes',
    'these',
    'this',
    'those',
    'thus',
    'unless',
})


def count_items(lines, start):
    """
    Count top-level list items in the block beginning at `start`.

    Returns None when the block is not a list, so the caller can tell "no list here" from "a list of
    zero items".
    """
    n = start
    while n < len(lines) and lines[n].strip() == '':
        n += 1
    if n >= len(lines) or not ITEM.match(lines[n]):
        return None
    items = 0
    while n < len(lines):
        line = lines[n]
        if line.strip() == '' or line.startswith((' ', '\t')):
            n += 1  # blank or continuation of an item
            continue
        if not ITEM.match(line):
            break  # heading, fence, table or prose
        items += 1
        n += 1
    return items


# A conjunction after a full stop is never a sentence start in this prose;
# it is the seam left by inserting text into the middle of a paragraph.
SPLICE = re.compile(
    r'\.\s+(and|or|but|so|which|then|because|therefore|rather|returning)\b')


def inline_spans(text):
    """
    Character ranges covered by single-backtick code spans.

    `code_spans` covers fenced blocks only, and deliberately so: the symbol check reads the
    backticked identifiers this skips.
    """
    return [m.span() for m in re.finditer(r'`[^`\n]+`', text)]


def check_sentence_splices(path, text, problems):
    """Fail on a sentence beginning with a conjunction after a full stop."""
    spans = code_spans(text) + inline_spans(text)
    for m in SPLICE.finditer(text):
        if in_code(m.start(), spans):
            continue
        line = text.count('\n', 0, m.start()) + 1
        problems.append(f'{path.name}:{line}: sentence starts with '
                        f'"{m.group(1)}" after a full stop; a clause was '
                        f'orphaned by an insertion')


def counts_something(line, pos):
    """Say whether a plural noun follows the number word ending at `pos`."""
    for token in re.findall(r'[\w-]+', line[pos:])[:4]:
        token = token.lower()
        if PLURAL.match(token) and token not in NOT_PLURAL:
            return True
    return False


def check_list_counts(path, text, problems):
    """
    Fail when a sentence states a count that its own list contradicts.

    Adding an item to an enumerated list and leaving the count that
    introduces it alone is the exact defect this catches: "Two details
    matter here:" over three numbered items.
    """
    lines = text.split('\n')
    fenced = False
    for n, line in enumerate(lines):
        if line.startswith('```'):
            fenced = not fenced
            continue
        if fenced or not line.rstrip().endswith(':'):
            continue
        words = {
            m.group(1).lower()
            for m in WORD.finditer(line)
            if counts_something(line, m.end())
        }
        if len(words) != 1:
            # No count, or an ambiguous sentence naming several. Counting
            # "two of the three paths" either way would be a guess.
            continue
        stated = NUMBER_WORDS[words.pop()]
        found = count_items(lines, n + 1)
        if found is not None and found != stated:
            problems.append(f'{path.name}:{n + 1}: says {stated} but the list '
                            f'below it has {found} items')


MARKER = re.compile(r'^\s*(?:[-*+]\s|\d+\.\s|\||#{1,6}\s|>)')


def check_markdown(path, text, problems):
    lines = text.split('\n')
    h1 = 0
    fenced = False
    for ln in lines:
        if ln.startswith('```'):
            fenced = not fenced
        elif not fenced and ln.startswith('# '):
            h1 += 1
    if h1 != 1:
        problems.append(f'{path.name}: expected exactly one H1, found {h1}')
    inside = False
    prev_blank = True
    for n, ln in enumerate(lines, 1):
        if ln.startswith('```'):
            inside = not inside
            prev_blank = True
            # Fall through to the ASCII and whitespace checks: a trailing
            # space on a fence line is exactly what disabled the compile
            # check, so it must not be exempt from being reported.
            if not ascii_ok(ln):
                problems.append(f'{path.name}:{n}: non-ASCII character')
            if re.search(r'[ \t]+\r?$', ln):
                problems.append(f'{path.name}:{n}: trailing whitespace')
            continue
        if not ascii_ok(ln):
            problems.append(f'{path.name}:{n}: non-ASCII character')
        if re.search(r'[ \t]+\r?$', ln):
            problems.append(f'{path.name}:{n}: trailing whitespace')
        if inside:
            continue
        if ln.strip() == '':
            prev_blank = True
        else:
            if not prev_blank and not MARKER.match(ln):
                problems.append(f'{path.name}:{n}: hard-wrapped paragraph '
                                f'(join it onto the previous line)')
            prev_blank = False


def ascii_ok(text):
    return all(ord(c) < 128 for c in text)


def main():
    problems: list[str] = []
    tally = {'seen': 0, 'checked': 0, 'unresolved': 0, 'skipped_constant': 0}
    members = load_members(problems)
    docs = sorted(DESIGN.rglob('*.md'))
    if not docs:
        print('check_docs: no documents found')
        return 1
    # Headings are pooled across the tree: referring to a section of a
    # sibling document is normal and must not be flagged.
    all_heads = set()
    texts = {}
    for path in docs:
        try:
            texts[path] = path.read_text(encoding='utf-8')
        except UnicodeDecodeError as exc:
            problems.append(f'{path.name}: not valid UTF-8 ({exc})')
            continue
        all_heads |= {
            m.group(1).strip().replace('`', '').lower()
            for m in re.finditer(r'^#{2,6}\s+(.*)$', texts[path], re.MULTILINE)
        }
    for path in docs:
        text = texts.get(path)
        if text is None:
            continue
        check_symbols(path, text, members, problems, tally)
        check_file_lines(path, text, problems)
        check_crossrefs(path, text, problems, all_heads)
        check_python_blocks(path, text, problems)
        check_markdown(path, text, problems)
        check_manifest_sections(path, text, problems)
        check_list_counts(path, text, problems)
        check_sentence_splices(path, text, problems)
    for p in problems:
        print(p)
    # Report coverage, not just problems. Two review passes over-trusted this
    # tool because "0 problems" reads like "everything verified" when it can
    # also mean "nothing was checked".
    print(f'check_docs: {len(docs)} documents, {len(problems)} problems')
    print(f'check_docs: symbol citations {tally["seen"]} seen, '
          f'{tally["checked"]} verified, {tally["unresolved"]} unresolvable, '
          f'{tally["skipped_constant"]} uppercase-skipped')
    return 1 if problems else 0


if __name__ == '__main__':
    sys.exit(main())
