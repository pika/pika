"""Check the design documents against the invariants reviews keep catching.

Run from the repository root::

    python3 design/check_docs.py

Exits non-zero and prints one line per problem. Every check here exists
because a review found that class of defect in a hand-written design
document at least once; the point is to make the invariant executable
rather than to describe it again in prose.

Checks, in order of how much they have caught:

1. Symbol citations. A backticked ``Class.member`` reference is verified
   against the real class, so a citation of a method that does not exist
   fails here rather than in review.
2. ``file:line`` citations. The file must exist and be at least that long.
3. Cross-references. A quoted section name must match a real heading.
4. Fenced Python blocks must compile.
5. Markdown hygiene: ASCII only, no trailing whitespace, exactly one H1,
   and no hard-wrapped paragraphs.
"""
import ast
import pathlib
import re
import sys

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


def load_members():
    """Map each class name to the attributes and methods defined on it."""
    members: dict[str, set[str]] = {}
    for rel in SOURCES:
        path = ROOT / rel
        if not path.exists():
            print(f'check_docs: missing source {rel}')
            continue
        tree = ast.parse(path.read_text(encoding='utf-8'), filename=str(path))
        for node in ast.walk(tree):
            if not isinstance(node, ast.ClassDef):
                continue
            found = members.setdefault(node.name, set())
            for sub in ast.walk(node):
                if isinstance(sub, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    found.add(sub.name)
                elif isinstance(sub, ast.Attribute) and isinstance(
                        sub.value, ast.Name) and sub.value.id == 'self':
                    found.add(sub.attr)
                elif isinstance(sub, ast.AnnAssign) and isinstance(
                        sub.target, ast.Name):
                    found.add(sub.target.id)
                elif isinstance(sub, ast.Assign):
                    for t in sub.targets:
                        if isinstance(t, ast.Name):
                            found.add(t.id)
    return members


def code_spans(text):
    """Character ranges covered by fenced code blocks, which are not prose."""
    spans = []
    for m in re.finditer(r'```.*?```', text, re.S):
        spans.append((m.start(), m.end()))
    return spans


def in_code(pos, spans):
    return any(a <= pos < b for a, b in spans)


def check_symbols(path, text, members, problems):
    spans = code_spans(text)
    for m in re.finditer(r'`([A-Za-z_][\w.]*)\.(\w+)`', text):
        if in_code(m.start(), spans):
            continue
        owner, member = m.group(1), m.group(2)
        if FOREIGN.match(owner) or owner.endswith('.py'):
            continue
        cls = owner.split('.')[-1]
        if cls not in members:
            continue
        # `Channel.Close`, `Basic.Ack`, `Connection.Blocked` and friends are
        # AMQP method frames, not Python members; the protocol namespace
        # collides with our class names.
        if member[:1].isupper():
            continue
        if f'{cls}.{member}' in PROPOSED:
            continue
        if member not in members[cls]:
            line = text[:m.start()].count('\n') + 1
            problems.append(
                f'{path.name}:{line}: `{cls}.{member}` does not exist on '
                f'{cls} (members are read from the real source)')


def check_file_lines(path, text, problems):
    for m in re.finditer(r'`?([\w/\\.]+\.py):(\d+)', text):
        rel, lineno = m.group(1), int(m.group(2))
        target = ROOT / rel
        if not target.exists():
            # A bare filename such as recovery.py is a proposed file.
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
    for m in re.finditer(r'(?:see|under|per|in) "([^"]{8,110})"', text):
        raw = m.group(1).strip().rstrip('.').replace('`', '')
        ref = raw.lower()
        # Only treat it as a section reference when it reads like a title.
        # Test the case on the raw text: `ref` is already lowercased, so
        # testing it here would make this branch unreachable.
        if not raw[:1].isupper():
            continue
        if ref not in heads:
            line = text[:m.start()].count('\n') + 1
            problems.append(f'{path.name}:{line}: cross-reference "'
                            f'{m.group(1)}" matches no heading')


def check_python_blocks(path, text, problems):
    preamble = ('from __future__ import annotations\n'
                'import enum\n'
                'from dataclasses import dataclass, field\n'
                'from typing import Any, Callable\n')
    for i, m in enumerate(re.finditer(r'```python\n(.*?)```', text, re.S), 1):
        try:
            compile(preamble + m.group(1), f'<{path.name} block {i}>', 'exec')
        except SyntaxError as exc:
            line = text[:m.start()].count('\n') + 1
            problems.append(f'{path.name}:{line}: python block {i} does not '
                            f'compile: {exc.msg}')


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
    members = load_members()
    problems: list[str] = []
    docs = sorted(DESIGN.rglob('*.md'))
    if not docs:
        print('check_docs: no documents found')
        return 1
    # Headings are pooled across the tree: referring to a section of a
    # sibling document is normal and must not be flagged.
    all_heads = set()
    for path in docs:
        all_heads |= {
            m.group(1).strip().replace('`', '').lower() for m in re.finditer(
                r'^#{2,6}\s+(.*)$', path.read_text(encoding='utf-8'), re.M)
        }
    for path in docs:
        text = path.read_text(encoding='utf-8')
        check_symbols(path, text, members, problems)
        check_file_lines(path, text, problems)
        check_crossrefs(path, text, problems, all_heads)
        check_python_blocks(path, text, problems)
        check_markdown(path, text, problems)
    for p in problems:
        print(p)
    print(f'check_docs: {len(docs)} documents, {len(problems)} problems')
    return 1 if problems else 0


if __name__ == '__main__':
    sys.exit(main())
