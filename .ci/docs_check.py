"""
Post-build checks on the generated documentation site.

`mkdocs build --strict` and `mkdocs-autorefs` between them refuse a cross-reference whose target
does not exist, so a wrong path fails the build. Neither notices a reference that never parsed as
one: `docformatter` reflows a docstring summary and can wrap inside the target path, leaving
``[`name`][pika.module.Cla`` on one line and ``ss.member]`` on the next. The reference is then plain
text, autorefs has nothing to validate, and the page ships with markup in it.

Sphinx inline roles have the same shape. Griffe's sphinx parser reads field lists and passes
everything else through, so `:meth:`x`` reaches the page verbatim. See #1750.

Both checks therefore read the built site rather than the source. Source blocks are excluded:
`docs/modules/` sets `show_source: true`, so every docstring appears twice on its page and the
second copy legitimately contains whatever the first one renders.
"""

from __future__ import annotations

import html
import pathlib
import re
import sys

#: Sphinx inline roles, with or without the `py:` domain prefix. The role's backticks do not
#: survive rendering, since mkdocs turns them into a `code` element, so this must not require one.
ROLE = re.compile(r':(?:py:)?(?:meth|class|attr|data|func|exc|mod):')

#: A markdown cross-reference that reached the page as text instead of a link.
LITERAL_REF = re.compile(r'\]\[pika\.')

#: mkdocstrings wraps published source in a `details` element.
SOURCE_BLOCK = re.compile(r'<details[^>]*>.*?</details>', re.DOTALL)

TAGS = re.compile(r'<[^>]+>')


def rendered_text(page: str) -> str:
    """
    Return a page's prose with markup and published source removed.

    :param page: The page's HTML.
    :returns: Text as a reader sees it, source listings excluded.
    """
    return html.unescape(TAGS.sub(' ', SOURCE_BLOCK.sub(' ', page)))


def findings(site: pathlib.Path) -> list[str]:
    """
    Report every leaked role and unparsed cross-reference under *site*.

    :param site: The built site directory.
    :returns: One line per affected page, empty when the site is clean.
    """
    out = []
    for page in sorted(site.rglob('index.html')):
        text = rendered_text(page.read_text(encoding='utf-8'))
        roles = len(ROLE.findall(text))
        refs = len(LITERAL_REF.findall(text))
        if roles or refs:
            out.append(f'{page.relative_to(site)}: {roles} sphinx role(s), '
                       f'{refs} unparsed cross-reference(s)')
    return out


def main(argv: list[str]) -> int:
    """
    Check a built site and report what it found.

    :param argv: Command-line arguments; the first is the site directory.
    :returns: Process exit status.
    """
    site = pathlib.Path(argv[0] if argv else 'site')
    if not site.is_dir():
        print(f'{site} is not a directory; build the docs first')
        return 2
    found = findings(site)
    for line in found:
        print(line)
    if found:
        print(f'{len(found)} page(s) carry unrendered documentation markup')
        return 1
    print(f'{site}: no leaked roles or unparsed cross-references')
    return 0


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
