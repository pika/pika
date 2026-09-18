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
11. Every file a manifest bullet names exists, or is declared in
    ``PROPOSED_FILES``. The symbol check reads the owner out of those
    bullets and ignores the path, so a typo in one passed unnoticed.
12. A quoted string that is *nearly* a heading, which is what a typo in a
    pointer looks like. The verb-led check in item 4 only reaches a
    quoted name after see/under/per/in/from, and 11 real pointers here
    are introduced some other way.

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
import difflib
import pathlib
import re
import subprocess
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
MIN_VERIFIED = 107

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
    # The document says the five registration methods are provided on both
    # classes and integration test 15 registers these two on a channel, so
    # listing only the connection's half made the channel's uncitable.
    'Channel.add_on_close_callback',
    'Channel.add_on_open_callback',
}

# Bases whose members are genuinely not ours and whose absence from the parsed
# set is expected rather than a gap in coverage.
EXTERNAL_BASES = frozenset({
    'Exception', 'BaseException', 'object', 'ValueError', 'TypeError',
    'IOError', 'OSError', 'RuntimeError', 'AttributeError', 'Enum', 'IntEnum',
    'ABC', 'Generic', 'NamedTuple'
})

# Qualified class keys whose inherited member set could not be fully resolved,
# so a missing member there means "cannot tell", not "does not exist".
INCOMPLETE = set()  # type: ignore[var-annotated]

# Files these documents propose creating. Same declaration as `PROPOSED`: an
# entry says the path does not exist yet, so a typo in any *other* manifest path
# is a failure instead of passing unnoticed.
PROPOSED_FILES = frozenset({
    'pika/recovery.py',
    'examples/thread_safe_recovery_example.py',
    'tests/acceptance/thread_safe_recovery_test.py',
    'tests/unit/recovery_tests.py',
})

# Classes these documents propose adding, as `module.Class`. The same
# declaration `PROPOSED` makes for members: listing one says it does not exist
# yet. Without this, citing the design's own new exception types the way its
# conventions ask for was reported as a defect.
PROPOSED_CLASSES = frozenset({
    'pika.exceptions.ConnectionRecovering',
    'pika.exceptions.ChannelRecovering',
})

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


def tracked_files():
    """
    Paths git knows about, as a set of `ROOT`-relative strings.

    Resolving a bare file name by globbing the tree made verdicts depend on
    whatever untracked directories a developer happened to have: first `build/`
    from `pip install -e .`, then `typings/` from pyright. Patching the ignore
    list each time loses that race, because the list is a guess about other
    people's working copies. Git already knows, so ask it. Falls back to the
    ignore list when git is unavailable, which is the tarball case.
    """
    try:
        out = subprocess.run(
            ['git', '-C', str(ROOT), 'ls-files', '-z'],
            capture_output=True,
            check=True,
            text=True).stdout
    except (OSError, subprocess.CalledProcessError):
        return None
    return {p for p in out.split('\0') if p}


TRACKED = tracked_files()


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


def module_prefix(owner):
    """
    The leading all-lowercase run of a dotted owner: its module part.

    Class segments start with a capital, so `pika.spec.Basic` gives
    `pika.spec` and `pika.connection.Connection` gives `pika.connection`.
    Splitting on the *last* segment instead treated `pika.spec.Basic.Ack` as a
    citation of a module called `pika.spec.Basic`, and reported five correct
    shapes as defects: nested AMQP frame classes, class constants such as
    `Connection.DEFAULT_PORT`, module-level functions, and the design's own
    proposed exception classes.
    """
    keep = []
    for part in owner.split('.'):
        if part[:1].isupper():
            break
        keep.append(part)
    return '.'.join(keep)


def resolve_owner(owner, members):
    """
    Resolve a citation's owner to a qualified class key, or None.

    Suffix matching is what makes a partially-qualified owner work:
    `thread_safe_connection.Connection` reaches the adapter class, while
    `heartbeat.IOLoop` reaches nothing, because no key ends that way. The tail
    fallback it replaces took the last segment alone, so `heartbeat.IOLoop`
    resolved through `IOLoop` to the adapter's and was counted as verified.
    """
    if owner in BARE_DEFAULTS:
        return BARE_DEFAULTS[owner]
    if owner in members:
        return owner
    if '.' not in owner:
        return None
    hits = [q for q in members if q.endswith('.' + owner)]
    return hits[0] if len(hits) == 1 else None


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

    # A base outside `SOURCES` - `SocketConnectionMixin`, or plain `Exception` -
    # contributes members this cannot see, so its subclass's member set is
    # *incomplete* and absence proves nothing there. Recording that is the
    # difference between "this member does not exist" and "I cannot tell":
    # `SelectorIOServicesAdapter.connect_socket` is real and inherited from a
    # mixin in an unlisted module, and was being reported as nonexistent, with
    # `PROPOSED` - which asserts a member does *not* exist yet - the only escape.
    incomplete = set()

    def inherited(qual, seen):
        if qual in seen:
            return set()  # cyclic or repeated base; stop
        seen.add(qual)
        out = set(by_qualified[qual])
        for base in bases.get(qual, []):
            cand = resolve_base(qual, base)
            if cand is None:
                if base not in EXTERNAL_BASES:
                    incomplete.add(qual)
                continue
            out |= inherited(cand, seen)
            if cand in incomplete:
                incomplete.add(qual)
        return out

    by_qualified = {q: inherited(q, set()) for q in list(by_qualified)}
    INCOMPLETE.clear()
    INCOMPLETE.update(incomplete)
    # `PROPOSED` and `PROPOSED_CLASSES` assert that something does *not* exist
    # yet, so an entry that has since landed is a false claim in the one list
    # the proposal tells reviewers to read as its drift signal - and it silently
    # buys a permanent exemption from the membership check. `SOURCES` and
    # `BARE_DEFAULTS` are both validated against reality; these were not.
    for entry in sorted(PROPOSED):
        cls, _, member = entry.partition('.')
        key = BARE_DEFAULTS.get(cls, cls)
        if member in by_qualified.get(key, ()):
            problems.append(
                f'check_docs: PROPOSED lists {entry}, which now exists; '
                f'remove it so the member is checked rather than exempted')
    for entry in sorted(PROPOSED_CLASSES):
        if entry in by_qualified:
            problems.append(
                f'check_docs: PROPOSED_CLASSES lists {entry}, which now '
                f'exists; remove it so the class is checked')
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
        # The incompleteness mark has to reach the bare alias too: citations
        # mostly use bare names, so marking only the qualified key left the
        # false "does not exist" in place for exactly the spelling authors use.
        if len(quals) == 1 and quals[0] in incomplete:
            INCOMPLETE.add(name)
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
        # A misspelled module is caught before the uppercase skip, because in a
        # bare class reference such as `pika.chanel.Chanel` the class name *is*
        # the member, so the skip would swallow the typo. The module part is the
        # leading lowercase run, not everything but the last segment: reading it
        # the other way made `pika.spec.Basic.Ack` a citation of a nonexistent
        # module `pika.spec.Basic`.
        mod = module_prefix(owner)
        if owner.startswith('pika.') and not modpath(mod):
            line = text[:m.start()].count('\n') + 1
            problems.append(f'{path.name}:{line}: `{owner}.{member}` names the '
                            f'module {mod}, which does not exist')
            if tally is not None:
                tally['unresolved'] += 1
            continue
        if member[:1].isupper():
            # A class or constant reference. When the owner is a module this
            # corpus may cite, the class must exist there - that is the
            # base-versus-adapter guard, and dropping it to fix the false
            # positives below would have cost real coverage. Anywhere else,
            # resolving it needs the nested-class and re-export handling the
            # coverage note puts out of scope: `pika.spec.Basic.Ack` is a
            # nested frame class and `Connection.DEFAULT_PORT` is a constant.
            qual = f'{owner}.{member}'
            if owner in SOURCE_MODULES and qual not in PROPOSED_CLASSES:
                if qual in members:
                    if tally is not None:
                        tally['checked'] += 1
                else:
                    line = text[:m.start()].count('\n') + 1
                    problems.append(f'{path.name}:{line}: `{qual}` names no '
                                    f'class {member} in {owner}')
                    if tally is not None:
                        tally['failed'] += 1
                continue
            if tally is not None:
                tally['skipped_constant'] += 1
            continue
        if mod == owner:
            # The owner is entirely a module, so this cites a module-level
            # function such as `pika.callback.sanitize_prefix`. Real, and not a
            # class member; reporting it claimed `pika.callback` had no class
            # named `callback`.
            if tally is not None:
                tally['unresolved'] += 1
            continue
        # The stated convention wins over the ambiguity sentinel: a bare
        # `Connection` is the adapter one by definition, not an unresolvable
        # collision.
        key = resolve_owner(owner, members)
        if key is None:
            # A dotted owner that suffix-matches nothing. Reported when its
            # module is one this corpus is allowed to cite, since then the class
            # genuinely is not there; otherwise it is a receiver expression or a
            # module outside `SOURCES`, both out of scope.
            if mod in SOURCE_MODULES and mod != owner:
                line = text[:m.start()].count('\n') + 1
                problems.append(
                    f'{path.name}:{line}: `{owner}.{member}` names no class '
                    f'{owner[len(mod) + 1:]} in {mod}')
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
        # `checked` counts citations that resolved *and* held. Incrementing it
        # before the membership test counted failures as verified, so the
        # coverage line could report 92 verified while printing a defect, and
        # the `MIN_VERIFIED` floor built on it was measuring the wrong thing.
        if member not in members[key]:
            if key in INCOMPLETE:
                # Its member set is incomplete, so absence proves nothing.
                if tally is not None:
                    tally['unresolved'] += 1
                continue
            line = text[:m.start()].count('\n') + 1
            problems.append(
                f'{path.name}:{line}: `{owner}.{member}` does not exist on '
                f'{key} (members are read from the real source)')
            if tally is not None:
                tally['failed'] += 1
        elif tally is not None:
            tally['checked'] += 1


# `- **`path`**, on `Class`:` - the manifest's own bullet shape, which
# establishes an owner for every bare backticked name after it.
MANIFEST_OWNER = re.compile(r'^-\s+\*\*`[^`]+`\*\*,\s+on\s+`(\w+)`:')

MANIFEST_PATH = re.compile(r'^-\s+\*\*`([\w./-]+\.\w+)`\*\*')


def check_manifest_paths(path, text, problems):
    """
    Check that every file a manifest bullet names exists or is proposed.

    The symbol check reads the owner class out of these bullets and ignores the
    path, and `check_file_lines` only looks at paths followed by `:digits` - so
    `pika/recoverry.py` and `thread_safe_connnection.py` both passed unnoticed
    in the section an implementer builds from.
    """
    fenced = False
    for n, line in enumerate(text.split('\n'), 1):
        if line.lstrip().startswith('```'):
            fenced = not fenced
            continue
        if fenced:
            continue
        m = MANIFEST_PATH.match(line)
        if not m:
            continue
        rel = m.group(1)
        if rel in PROPOSED_FILES or (ROOT / rel).exists():
            continue
        problems.append(f'{path.name}:{n}: manifest names the file {rel}, '
                        f'which does not exist and is not in PROPOSED_FILES')


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
            if name not in members[key]:
                problems.append(
                    f'{path.name}:{n}: manifest names `{name}` on {cls}, '
                    f'which does not exist there and is not in PROPOSED')
                if tally is not None:
                    tally['failed'] += 1
            elif tally is not None:
                tally['checked'] += 1


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
            if TRACKED is not None:
                matches = [
                    ROOT / p
                    for p in TRACKED
                    if p == rel or p.endswith('/' + rel)
                ]
            else:
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


def is_manifest(title):
    """
    Exact match, not `startswith`.

    `startswith` claimed unrelated headings: `### Next steps for the test plan` was judged as a
    manifest and its ordinary prose reported as an explanation.
    """
    return title.strip() in MANIFEST_SECTIONS


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
    manifest_depth = 0
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
        # stopped it being checked at all - both with no signal. A *sibling* H3
        # does leave it, though: pinning `section` for every following sibling
        # made the message name the wrong section.
        head = re.match(r'^(#{2,6})\s+(.*)$', line)
        if head:
            depth, title = len(head.group(1)), head.group(2).strip()
            if depth <= manifest_depth or manifest_depth == 0:
                section = title
                manifest_depth = depth if is_manifest(title) else 0
            continue
        if not is_manifest(section):
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
    # The marker style of the first item decides where the list ends. Skipping
    # blank lines unconditionally merged two adjacent lists, so a correct
    # 3-item numbered list followed by an unrelated 2-item bullet list was
    # reported as "says 3 but the list below it has 5 items" - the check calling
    # correct markdown defective.
    ordered = lines[n][:1].isdigit()
    items = 0
    while n < len(lines):
        line = lines[n]
        if line.startswith((' ', '\t')):
            n += 1  # continuation, or a nested list under the current item
            continue
        if line.strip() == '':
            nxt = n + 1
            while nxt < len(lines) and lines[nxt].strip() == '':
                nxt += 1
            if nxt >= len(lines) or not ITEM.match(lines[nxt]):
                break  # blank line then prose: the list is over
            if lines[nxt][:1].isdigit() != ordered:
                break  # a different marker style is a different list
            n += 1
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


def check_near_miss_refs(path, text, problems, heads):
    """
    Report a quoted string that is nearly, but not exactly, a heading.

    The verb-led check only reaches a quoted name after see/under/per/in/from,
    which leaves 11 real pointers here unvalidated - they follow "Weigh", "the
    premise of", "which is what", or a bold run. Validating every quoted string
    instead would flag ordinary quoted prose, of which these documents have
    plenty. A near-miss is the discriminating signal: close to a heading means
    it was meant to be one, so a typo is caught wherever it sits, while
    unrelated prose is nowhere near any heading and stays quiet.
    """
    spans = code_spans(text) + inline_spans(text)
    for m in re.finditer(r'"([^"]{4,110})"', text):
        if in_code(m.start(), spans):
            continue
        quoted = m.group(1).replace('`', '').strip().lower()
        if quoted in heads:
            continue
        close = difflib.get_close_matches(quoted, heads, n=1, cutoff=0.9)
        if close:
            line = text[:m.start()].count('\n') + 1
            problems.append(f'{path.name}:{line}: "{m.group(1)}" is not a '
                            f'heading but is nearly "{close[0]}"')


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
        'failed': 0,
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
    # A stale `MANIFEST_SECTIONS` entry disables the name-and-point rule in
    # silence on a heading rename, which is why the sibling symbol check dropped
    # its heading gate entirely. This one still needs the names, so validate
    # them instead: every entry must match a real heading somewhere in the tree.
    problems.extend(
        f'check_docs: MANIFEST_SECTIONS names "{name}", which is not a heading '
        f'in any document; the name-and-point rule for it is not running'
        for name in MANIFEST_SECTIONS
        if name.lower() not in all_heads)
    for path in docs:
        text = texts.get(path)
        if text is None:
            continue
        check_symbols(path, text, members, problems, tally)
        check_manifest_symbols(path, text, members, problems, tally)
        check_manifest_paths(path, text, problems)
        check_file_lines(path, text, problems)
        check_crossrefs(path, text, problems, all_heads)
        check_near_miss_refs(path, text, problems, all_heads)
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
               tally['proposed'] + tally['failed'])
    print(f'check_docs: symbol citations {tally["seen"]} seen, '
          f'{tally["checked"]} verified, {tally["unresolved"]} unresolvable, '
          f'{tally["skipped_constant"]} uppercase-skipped, '
          f'{tally["proposed"]} proposed-exempt, '
          f'{tally["ambiguous"]} ambiguous, '
          f'{tally["failed"]} failed')
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
