"""
Check the design documents against the invariants reviews keep catching.

Run from the repository root::

    python3 design/check_docs.py

Exits non-zero and prints one line per problem, then a coverage line. Act
on the exit code: it is the pass/fail. Read the coverage line as well, for
the reason below.

**Read the coverage line.** This tool does not verify most of what the
documents assert, and two review passes over-trusted it because "0
problems" reads like "everything verified". It reports what it actually
checked so that cannot happen again, and the buckets are asserted to sum
to the number seen - they previously did not, which made a breakdown that
could not be read as one. Symbol citations it cannot resolve are the
majority: a receiver like ``self._channel._state`` or ``ch.is_open`` needs
type inference this does not attempt, and members whose names start with a
capital are skipped because the AMQP frame namespace collides with pika's
class names.

What it does check, reliably:

1. Backticked ``Class.member`` citations, with or without a trailing
   ``()``, whose owner resolves to a class in ``SOURCES``, against members
   parsed from the real source and its bases. Bare
   ``Connection``/``Channel`` resolve to the adapter per the glossary; a
   base-qualified citation of an adapter-only member fails, which is the
   base-versus-adapter confusion these documents exist to keep straight,
   and a ``pika.``-rooted owner that does not resolve is reported rather
   than silently retried by its tail.
2. The manifest's *bare* backticked names, against the class its bullet
   names in ``, on `Class`:``. ``check_symbols`` cannot see these, so the
   one section declared authoritative for implementers had no symbol
   checking at all: five typos planted in it produced ``0 problems``.
3. ``file:line`` citations, including both ends of a range, against real
   file lengths. A bare file name matching more than one path is reported,
   not skipped.
4. Every quoted section name after see/under/per/in/from, against headings
   pooled across the whole tree, H1 included and fences excluded.
5. Fenced Python blocks parse. Syntax only - a block may still fail at
   import on undefined names, which ``compile()`` cannot see.
6. Markdown hygiene: ASCII only, no trailing whitespace, exactly one H1,
   no hard-wrapped paragraphs, no unclosed fences.
7. Manifest sections name and point rather than explain, so a correction
   made in the prose cannot go stale in the sections an implementer
   builds from. Judged per clause: a pointer exempts the clause that
   points, not the whole line, or one long bullet buys blanket immunity.
8. A sentence stating a count agrees with the list beneath it. Adding an
   item and leaving the count alone is a defect a review pass found.
9. No sentence begins with a conjunction after a full stop, which is the
   seam left by inserting text into the middle of a paragraph and
   orphaning the clause that followed.
10. Every test the plan numbers is scheduled in some phase of "Next
    steps". A test nothing schedules is a test nothing will build, and
    this has drifted twice.

It also fails when fewer than ``MIN_VERIFIED`` citations get verified,
which is the one mechanical defence against a check going quietly inert:
two have, each printing ``0 problems`` and exiting 0.

Every check has been proven to fail on planted defects, several per check.
That matters more than it sounds, and twice now the proof has been the
thing at fault rather than the check: the cross-reference check was dead
on arrival, and a re-test of the manifest check passed only because the
planted typo landed in the prose instead of the manifest. Plant the defect
where the check is supposed to look, and confirm the message names it.
"""

import ast
import pathlib
import re
import sys
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Set as AbstractSet

# Sentinel for a bare class name that two modules both define, so a
# citation using it cannot be resolved and must be qualified.
# Frozen deliberately. This was weakened to a mutable `set` to clear a mypy
# error, which made a shared sentinel writable by anything holding it; the
# annotation on `members` is the right fix for that error.
AMBIGUOUS = frozenset({'<ambiguous>'})

# The documents' own convention, stated in the glossary: an unqualified
# `Connection` or `Channel` means the adapter class. Encode that rather than
# demanding qualification everywhere, which would fight the prose. Citing a
# base class still requires the module prefix, and a base-qualified citation
# of an adapter-only member therefore fails, which is the case worth catching.
ADAPTER_MODULE = 'pika/adapters/thread_safe_connection.py'
ADAPTER_DOTTED = ADAPTER_MODULE[:-3].replace('/', '.')
BARE_DEFAULTS = {
    'Connection': ADAPTER_DOTTED + '.Connection',
    'Channel': ADAPTER_DOTTED + '.Channel',
}


def is_adapter(key):
    """
    Say whether a resolved key names a class in the adapter module.

    Derived from `ADAPTER_MODULE` rather than spelled out again: a third hand-written copy of that
    path would fail the whole corpus closed after the 2.0 module move `README.md` announces, and
    silently.
    """
    return key.startswith(ADAPTER_DOTTED + '.')


# Reference-client extensions are here too: these documents cite Java, .NET
# and Go source files by name, and a file name is not a symbol citation.
EXTENSIONS = frozenset({
    'py', 'md', 'toml', 'yaml', 'yml', 'cfg', 'ini', 'txt', 'json', 'cs', 'go',
    'java'
})

# A committed floor on how many citations actually get verified. The human
# instruction to "read the coverage line" cannot catch a silent collapse,
# because 92 and 56 are equally plausible readings with nothing to compare
# against - and two checks have already gone inert while printing 0 problems
# and exiting 0. Raise it when coverage grows; lowering it is a deliberate act
# that belongs in the same commit as whatever removed the citations.
MIN_VERIFIED = 92

IGNORED_PARTS = frozenset({
    '.git', '.mypy_cache', '.pytest_cache', '.tox', '.venv', '__pycache__',
    'build', 'dist', 'env', 'node_modules', 'site', 'venv'
})

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

SOURCE_MODULES = frozenset(s[:-3].replace('/', '.') for s in SOURCES)

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
    'Connection._parameters',
    'Connection._connect_timeout',
    'Connection._skip_or_abort',
    'Connection.state',
    'Connection.is_recovering',
    'Connection.add_state_change_listener',
    'Connection.add_on_close_callback',
    'Connection.add_on_open_callback',
    'Connection.add_on_recovery_started_callback',
    'Connection.add_on_recovery_succeeded_callback',
    'Connection.add_on_recovery_failed_callback',
    'Channel._state',
    'Channel._closed',
    'Channel._delivery_tag_offset',
    'Channel._max_seen_delivery_tag',
    'Channel._make_delivery_callback',
    'Channel._register_recovery_close_listener',
    # The channel half of the observability API. The proposal puts these "on
    # both the adapter `Connection` and `Channel`", and listing only the
    # connection half meant a correct citation of the channel half was reported
    # as nonexistent - so the document could not cite its own API in the form
    # its conventions require, and a typo in any of them could never be caught.
    'Channel.state',
    'Channel.is_recovering',
    'Channel.add_state_change_listener',
    'Channel.add_on_recovery_started_callback',
    'Channel.add_on_recovery_succeeded_callback',
    'Channel.add_on_recovery_failed_callback',
}

# Names that look like Class.member but are not ours to check: reference
# clients, other languages, and dotted prose.
FOREIGN = re.compile(
    r'^(?:AutorecoveringChannel|AutorecoveringConnection|RecoveryAware\w*'
    r'|ChannelN|AMQConnection|Utility|RecordedConsumer|QosConfig|Connection\.'
    r'topologyConfiguration|amqp091|struct|copy|enum|dataclasses|typing'
    r'|functools|threading|collections|heapq)$')


def module_of(owner):
    """
    The module part of a dotted owner, dropping class segments.

    Class segments are the ones starting with a capital, so
    `pika.heartbeat.IOLoop` gives `pika.heartbeat` and
    `pika.adapters.utils.connection_workflow.AMQPConnectionWorkflow` gives the
    four-segment module.
    """
    parts = owner.split('.')
    while parts and parts[-1][:1].isupper():
        parts.pop()
    return '.'.join(parts)


def modpath(mod):
    """
    Say whether a dotted module name is on disk.

    This is what separates a typo from a real module outside `SOURCES`.
    Reporting every unresolved `pika.` owner flagged `pika.frame.Method` and
    `connection_workflow.AMQPConnectionWorkflow` - real classes, one of which
    a whole section recommends reusing - so citing them correctly failed the
    tool and the only escape was to de-qualify the citation.
    """
    if not mod:
        return False
    rel = mod.replace('.', '/')
    return (ROOT / f'{rel}.py').exists() or (ROOT / rel /
                                             '__init__.py').exists()


def qualified_problem(owner, member, members):
    """
    Describe why a `pika.`-rooted citation is wrong, or return None.

    Two shapes reach here and they mean different things. In `pika.connection.Connection` the
    *member* is the class and the owner is the module; in `pika.connection.Connection.channel` the
    owner carries the class. Reading only the first shape as the second asked whether
    `pika.connection` contains a class named `connection`.

    Two distinct defects, and an earlier version could see neither: a module not on disk, and a
    class absent from a module that *is* in `SOURCES`. The second is what the tail fallback
    accepted, resolving `pika.heartbeat.IOLoop` through its tail to the adapter's `IOLoop`.
    """
    if member[:1].isupper():
        mod, cls, qual = owner, member, f'{owner}.{member}'
    else:
        mod, cls, qual = module_of(owner), owner.rsplit('.', 1)[-1], owner
    if not modpath(mod):
        return f'names the module {mod}, which does not exist'
    if mod in SOURCE_MODULES and qual not in members:
        return f'names no class {cls} in {mod}'
    return None


def dotted_name(node):
    """Spell an `ast.Name`/`ast.Attribute` back out, dots included."""
    parts = []
    while isinstance(node, ast.Attribute):
        parts.append(node.attr)
        node = node.value
    if isinstance(node, ast.Name):
        parts.append(node.id)
    return '.'.join(reversed(parts))


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
    bases: dict[str, list[str]] = {}
    mod_imports: dict[str, dict[str, str]] = {}
    for rel in SOURCES:
        path = ROOT / rel
        if not path.exists():
            problems.append(f'check_docs: SOURCES entry {rel} does not exist')
            continue
        tree = ast.parse(path.read_text(encoding='utf-8'), filename=str(path))
        # `class SelectConnection(BaseConnection)` names its base bare, having
        # imported it, so resolving a bare base inside the citing module alone
        # finds nothing and loses every inherited member. Resolve through the
        # module's imports instead, which is also what keeps a bare `Connection`
        # from reaching whichever `Connection` happens to sort first.
        imported = {}
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and node.module:
                for alias in node.names:
                    local = alias.asname or alias.name
                    imported[local] = f'{node.module}.{alias.name}'
        mod_imports[rel[:-3].replace('/', '.')] = imported
        for node in ast.walk(tree):
            if not isinstance(node, ast.ClassDef):
                continue
            mod = rel[:-3].replace('/', '.')
            found = by_qualified.setdefault(f'{mod}.{node.name}', set())
            # Keep the base's *dotted* spelling. Reducing `connection.Connection`
            # to the bare tail `Connection` merged the adapter and base
            # namespaces through inheritance, which is the outcome this
            # function's docstring says would defeat the check entirely.
            bases[f'{mod}.{node.name}'] = [
                dotted_name(b)
                for b in node.bases
                if isinstance(b, (ast.Name, ast.Attribute))
            ]
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
    # Fold in inherited members. Without this, a *correct* citation of an
    # inherited member is reported as nonexistent: `SelectConnection` declares
    # four members of its own against 31 on `BaseConnection` and 89 on
    # `pika.connection.Connection`, so `SelectConnection.ioloop` - an obvious
    # citation for documents about the persistent loop - failed. The only
    # escape was adding it to `PROPOSED`, which would record the false claim
    # that it does not exist yet.
    def resolve_base(qual, base):
        """
        Resolve one base's spelling to a qualified key, or None.

        A dotted spelling must match the tail of a qualified name, so `connection.Connection`
        reaches `pika.connection.Connection` and cannot reach the adapter's `Connection`. A bare
        spelling resolves only within the citing class's own module: resolving it across modules is
        what merged the two `Connection` namespaces.
        """
        mod = qual.rsplit('.', 1)[0]
        target = mod_imports.get(mod, {}).get(base, base)
        if '.' in target:
            if target in by_qualified:
                return target  # already fully qualified
            hits = [q for q in by_qualified if q.endswith('.' + target)]
            return hits[0] if len(hits) == 1 else None
        same = mod + '.' + target
        return same if same in by_qualified else None

    def inherited(qual, seen):
        if qual in seen:
            return set()  # cyclic or repeated base; stop
        seen.add(qual)
        out = set(by_qualified[qual])
        for base in bases.get(qual, []):
            cand = resolve_base(qual, base)
            if cand is not None:
                out |= inherited(cand, seen)
        return out

    by_qualified = {q: inherited(q, set()) for q in list(by_qualified)}
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
    members: dict[str, AbstractSet[str]] = dict(by_qualified)
    for name, quals in bare.items():
        if len(quals) == 1:
            members[name] = by_qualified[quals[0]]
        else:
            members[name] = AMBIGUOUS
    return members


def headings(text):
    """Normalised heading text, skipping anything inside a code fence."""
    out = set()
    fenced = False
    for line in text.split('\n'):
        if line.lstrip().startswith('```'):
            fenced = not fenced
            continue
        if fenced:
            continue
        head = re.match(r'^#{1,6}\s+(.*)$', line)
        if head:
            out.add(head.group(1).strip().replace('`', '').lower())
    return out


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
        # `lstrip` matters and the anchoring still does too. An indented
        # closing fence is invisible to `startswith`, so the block stays open
        # until the next block's opening fence closes it - inverting the
        # pairing, so prose between two blocks becomes a code span and is
        # exempted from the symbol, cross-reference and hard-wrap checks while
        # the fence count stays even and the unclosed-fence report never fires.
        if line.lstrip().startswith('```'):
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
    # The trailing `()` is optional: requiring a bare backtick dropped every
    # call-form citation, which is 47 of them here, before `seen` counted
    # them - so the coverage line reported a denominator that silently
    # omitted the citations the check could not see.
    # Arguments allowed inside the parentheses, not just `()`. Requiring empty
    # parens left 14 citations of the form `ch.exchange_declare(exchange=...)`
    # invisible to the check and absent from the `seen` denominator.
    for m in re.finditer(r'`([A-Za-z_][\w.]*)\.(\w+)(?:\([^`]*?\))?`', text):
        if in_code(m.start(), spans):
            continue
        owner, member = m.group(1), m.group(2)
        # A file name is not a symbol citation. The test used to read
        # `owner.endswith('.py')`, which can never fire: in `foo.py` the
        # extension is the *member*, so 24 file names were being counted as
        # symbol citations and reported as unresolvable.
        if member in EXTENSIONS:
            continue
        if FOREIGN.match(owner):
            continue
        # `Channel.Close`, `Basic.Ack`, `Connection.Blocked` and friends are
        # AMQP method frames, not Python members; the protocol namespace
        # collides with our class names. Tested before resolution so a frame
        # name never produces a spurious namespace complaint.
        if tally is not None:
            tally['seen'] += 1
        # All `pika.`-rooted resolution happens here, before the uppercase skip,
        # because in a bare class reference such as `pika.chanel.Chanel` the
        # class name *is* the member - so the skip ran first and swallowed the
        # typo, leaving the branch written to catch a wrong module path dead for
        # exactly the citations it was written for.
        if owner.startswith('pika.'):
            bad = qualified_problem(owner, member, members)
            if bad:
                line = text[:m.start()].count('\n') + 1
                problems.append(f'{path.name}:{line}: `{owner}.{member}` {bad}')
                if tally is not None:
                    tally['unresolved'] += 1
                continue
            if owner not in members:
                # A real module outside `SOURCES`: out of scope, not a defect.
                if tally is not None:
                    tally['unresolved'] += 1
                continue
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
        elif '.' in owner:
            key = owner.split('.')[-1]
        else:
            key = owner
        if key not in members:
            if tally is not None:
                tally['unresolved'] += 1
            continue
        if members[key] is AMBIGUOUS:
            line = text[:m.start()].count('\n') + 1
            problems.append(
                f'{path.name}:{line}: `{owner}.{member}` uses a bare class '
                f'name defined in more than one module; qualify it')
            if tally is not None:
                tally['ambiguous'] += 1
            continue
        # Match PROPOSED on the *resolved* key, not the bare class name:
        # matching bare re-merges the namespaces and exempts every proposed
        # member on the base classes too, which is the case worth catching.
        cls = key.rsplit('.', 1)[-1]
        if is_adapter(key) and f'{cls}.{member}' in PROPOSED:
            if tally is not None:
                tally['proposed'] += 1
            continue
        if tally is not None:
            tally['checked'] += 1
        if member not in members[key]:
            line = text[:m.start()].count('\n') + 1
            problems.append(
                f'{path.name}:{line}: `{owner}.{member}` does not exist on '
                f'{key} (members are read from the real source)')


# `- **`path`**, on `Class`:` - the manifest's own bullet shape, which
# establishes an owner for every bare backticked name after it.
MANIFEST_OWNER = re.compile(r'^-\s+\*\*`[^`]+`\*\*,\s+on\s+`(\w+)`:')


def check_manifest_symbols(path, text, members, problems, tally=None):
    """
    Check the manifest's bare backticked names against the owner it names.

    `check_symbols` only sees `Owner.member`, and the manifest deliberately
    writes `` `_reopen_channel` `` with the owner hoisted into the bullet
    prefix - so the one section declared authoritative for implementers was
    the one section with no symbol checking at all. Five typos planted in it
    produced `0 problems`.
    """
    # No section gate. `MANIFEST_OWNER` already matches only the manifest's own
    # bullet shape, so gating on the heading text added nothing and made the
    # check collapse silently on a heading rename, on one `###` inside the
    # section, and on a `## ` line inside a fenced block. A per-line `fenced`
    # toggle is the whole of what is needed.
    fenced = False
    for n, line in enumerate(text.split('\n'), 1):
        if line.lstrip().startswith('```'):
            fenced = not fenced
            continue
        if fenced:
            continue
        owner_match = MANIFEST_OWNER.match(line)
        if not owner_match:
            continue
        owner = owner_match.group(1)
        key = BARE_DEFAULTS.get(owner, owner)
        if key not in members or members[key] is AMBIGUOUS:
            problems.append(f'{path.name}:{n}: manifest bullet names owner '
                            f'`{owner}`, which does not resolve to one class')
            continue
        cls = key.rsplit('.', 1)[-1]
        for tok in re.finditer(r'`([a-z_]\w*)`', line[owner_match.end():]):
            name = tok.group(1)
            if tally is not None:
                tally['seen'] += 1
            if f'{cls}.{name}' in PROPOSED:
                if tally is not None:
                    tally['proposed'] += 1
                continue
            if tally is not None:
                tally['checked'] += 1
            if name not in members[key]:
                problems.append(
                    f'{path.name}:{n}: manifest names `{name}` on {cls}, '
                    f'which does not exist there and is not in PROPOSED')


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
            # so the citation is checked rather than skipped. Build and
            # environment directories must be excluded, not just `.git`: an
            # in-tree `pip install -e .` or `python -m build` produces a second
            # copy under `build/`, which made `len(matches) != 1` and silently
            # skipped the citation - so the same commit got opposite verdicts
            # depending on the developer's untracked directories.
            # Relative to ROOT, not absolute. Testing the absolute path meant a
            # checkout under a directory named `build`, `env` or `site`
            # discarded its own only match, leaving `matches` empty so the
            # citation fell through unchecked - reintroducing the very
            # "opposite verdicts from untracked directories" failure this
            # filter was added to fix, keyed on an ancestor instead.
            matches = [
                q for q in ROOT.rglob(rel)
                if not IGNORED_PARTS & set(q.relative_to(ROOT).parts)
            ]
            if len(matches) == 1:
                target = matches[0]
            else:
                # Zero is reported as well as many. Zero used to fall through
                # in silence, which matters because both `file:line` citations
                # in this corpus are bare file names, so the 2.0 module move
                # would have stopped verifying both without a word.
                line = text[:m.start()].count('\n') + 1
                problems.append(
                    f'{path.name}:{line}: cited {rel} matches '
                    f'{len(matches)} files; cite it with its directory')
                continue
        if not target.exists():
            if '/' in rel:
                line = text[:m.start()].count('\n') + 1
                problems.append(f'{path.name}:{line}: cited file {rel} '
                                f'does not exist')
            continue
        length = len(target.read_text(encoding='utf-8').splitlines())
        if lineno < 1 or lineno > length:
            line = text[:m.start()].count('\n') + 1
            problems.append(f'{path.name}:{line}: cited {rel}:{lineno} but '
                            f'that file has {length} lines')


def check_crossrefs(path, text, problems, heads):
    spans = code_spans(text)
    # `re.I` matters: sentence-initial `See "..."` is the dominant form and a
    # case-sensitive pattern misses 16 of the 103 references here. `\b` stops
    # `within "..."` matching through the `in`.
    # Validate every quoted name in a run, not just the first: pointer lines
    # legitimately list several sections after one `under`, and checking only
    # the first leaves 24 of those 103 unverified.
    # The separator alternation includes `, and `, which is how four of the
    # runs here are punctuated; without it the run ends at the comma and the
    # name after `and` goes unchecked.
    pattern = (r'\b(?:see|under|per|in|from)\s+'
               r'((?:"[^"]{4,110}"(?:\s*,\s*and\s+|\s*,\s*|\s+and\s+)?)+)')
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

# A pointer phrase and the quoted section it points at. `\b` before the verb
# matters: unanchored, the alternation matched inside ordinary words, so
# `a proper "Guard and exceptions" note, because ...` split at `proper "` and
# exempted the rest of the line - and `wrapper` occurs throughout. The verb
# list is the same one `check_crossrefs` accepts, `in` and `from` included:
# omitting them denied immunity to a legitimate pointer worded `specified in
# "..."`, whose own section title then tripped the behaviour-word check.
# It excises the whole run of quoted names, not just the first: these bullets
# legitimately point at several sections after one `under`, and a section title
# can itself contain a behaviour word - two of this document's do - so leaving
# the later titles in place reported the pointer as an explanation.
POINTER = re.compile(
    r'\b(?:see|under|defined under|per|in|from)\s+'
    r'(?:"[^"]{4,110}"(?:\s*,\s*and\s+|\s*,\s*|\s+and\s+)?)+', re.IGNORECASE)

# Words that only appear when a sentence is explaining how something works.
# Deliberately narrow: a manifest legitimately says "gains", "lands", "new".
BEHAVIOUR_WORDS = re.compile(
    r'\b(?:because|therefore|so that|otherwise|would|must not|cannot|'
    r'instead of|rather than the|which means|'
    # Not a bare "the reason": "the reason for them under X" is a pointer,
    # which these sections are supposed to contain.
    r'the reason (?:is|was|being|that|why))\b',
    re.IGNORECASE)


def check_manifest_sections(path, text, problems):
    """Fail if a manifest section explains behaviour instead of pointing."""
    section = ''
    fenced = False
    for n, line in enumerate(text.split('\n'), 1):
        if line.lstrip().startswith('```'):
            fenced = not fenced
            continue
        if fenced:
            continue
        # H2-H6, and a deeper heading does not leave the section. Matching
        # `#{2,4}` and resetting on any hit meant one `###` inside a manifest
        # disabled the check for the rest of it, and demoting a manifest to H5
        # stopped it being checked at all - both with no signal.
        head = re.match(r'^(#{2,6})\s+(.*)$', line)
        if head:
            if len(head.group(1)) == 2 or not any(
                    section.startswith(s) for s in MANIFEST_SECTIONS):
                section = head.group(2).strip()
            continue
        if not any(section.startswith(s) for s in MANIFEST_SECTIONS):
            continue
        # A pointer earns immunity for the pointing *clause*, not for the whole
        # line. The never-hard-wrap rule makes every manifest bullet one long
        # line, so a line-level escape exempted 14 of 33 lines here - five of
        # which already contained the words this hunts - and granted immunity
        # to exactly the defect class it exists to catch: a bullet that both
        # points and restates mechanism.
        # Excise each pointer phrase and keep everything else. Splitting and
        # keeping only the prefix exempted the whole tail after the first
        # pointer, so on these one-line bullets a bullet that points and *then*
        # explains was fully immune - the blanket immunity the clause-level
        # judgement was introduced to remove, just moved to the right.
        checked = POINTER.sub(' ', line)
        hit = BEHAVIOUR_WORDS.search(checked)
        if hit:
            problems.append(
                f'{path.name}:{n}: "{section}" is a manifest; it should name '
                f'and point, not explain ("{hit.group(0)}")')


NUMBER_WORDS = {
    # Deliberately no 'one'. The defect this catches is adding an item to a
    # list of two or more and leaving the count, and admitting 'one' bought
    # false positives on ordinary English instead: "One class of ordering
    # constraint applies:" and "One thing remains:" both read as the count 1,
    # because `class` and `remains` end in `s` and pass for plural nouns.
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
        if line.lstrip().startswith('```'):
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


def check_tests_are_phased(path, text, problems):
    """
    Fail when a numbered test appears in no phase of "Next steps".

    A test the plan numbers but no phase schedules is a test nothing will build. This has drifted
    twice: two tests were orphaned, one of them added by the same review pass that wrote the section
    it belongs to.
    """
    if '## Next steps' not in text:
        return
    numbered = {
        m.group(1)
        for m in re.finditer(r'^\d+\. `(Test\w+)`', text, re.MULTILINE)
    }
    phases = text[text.index('## Next steps'):]
    scheduled = set(re.findall(r'`(Test\w+)`', phases))
    for name in sorted(numbered - scheduled):
        problems.append(f'{path.name}: `{name}` is numbered in the test plan '
                        f'but scheduled in no phase')


def check_markdown(path, text, problems):
    lines = text.split('\n')
    h1 = 0
    fenced = False
    for ln in lines:
        if ln.lstrip().startswith('```'):
            fenced = not fenced
        elif not fenced and ln.startswith('# '):
            h1 += 1
    if h1 != 1:
        problems.append(f'{path.name}: expected exactly one H1, found {h1}')
    inside = False
    prev_blank = True
    for n, ln in enumerate(lines, 1):
        if ln.lstrip().startswith('```'):
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
    tally = {
        'seen': 0,
        'checked': 0,
        'unresolved': 0,
        'skipped_constant': 0,
        'ambiguous': 0,
        'proposed': 0,
    }
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
        # Fence-aware, and H1 included. This was the only heading scan that
        # toggled on nothing, so a `## ` line inside a Python block became a
        # valid cross-reference target, while a pointer at a real H1 was
        # reported dangling.
        all_heads |= headings(texts[path])
    for path in docs:
        text = texts.get(path)
        if text is None:
            continue
        check_symbols(path, text, members, problems, tally)
        check_manifest_symbols(path, text, members, problems, tally)
        check_file_lines(path, text, problems)
        check_crossrefs(path, text, problems, all_heads)
        check_python_blocks(path, text, problems)
        check_markdown(path, text, problems)
        check_manifest_sections(path, text, problems)
        check_list_counts(path, text, problems)
        check_tests_are_phased(path, text, problems)
        check_sentence_splices(path, text, problems)
    for p in problems:
        print(p)
    # Report coverage, not just problems. Two review passes over-trusted this
    # tool because "0 problems" reads like "everything verified" when it can
    # also mean "nothing was checked".
    print(f'check_docs: {len(docs)} documents, {len(problems)} problems')
    # The buckets must account for every citation seen. They previously summed
    # to 180 of 184, because `PROPOSED` and ambiguity skips were untallied, so
    # the line could not be read as a breakdown even though it looked like one.
    buckets = (tally['checked'] + tally['unresolved'] +
               tally['skipped_constant'] + tally['ambiguous'] +
               tally['proposed'])
    print(f'check_docs: symbol citations {tally["seen"]} seen, '
          f'{tally["checked"]} verified, {tally["unresolved"]} unresolvable, '
          f'{tally["skipped_constant"]} uppercase-skipped, '
          f'{tally["proposed"]} proposed-exempt, '
          f'{tally["ambiguous"]} ambiguous')
    if buckets != tally['seen']:
        print(f'check_docs: BUG: buckets sum to {buckets}, not '
              f'{tally["seen"]}; the coverage line is not a breakdown')
        return 1
    if tally['checked'] < MIN_VERIFIED:
        print(f'check_docs: BUG: verified {tally["checked"]} citations, floor '
              f'is {MIN_VERIFIED}. Either a check has gone inert or citations '
              f'were removed; if the drop is intended, lower MIN_VERIFIED in '
              f'the same commit and say why.')
        return 1
    return 1 if problems else 0


if __name__ == '__main__':
    sys.exit(main())
