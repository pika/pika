"""
The release, as a command per step.

`release.py <operation>` is the only entry point for cutting a release, so there is one place to look
up what the release can do. The decisions live in modules beside it, each with unit tests, because a
version becomes a PyPI release and a documentation directory that nobody can take back:
`release_version.py` owns the version arithmetic and what a pushed tag means, and `tag_release.py`
owns the preconditions for tagging.

The flow, which is also the order these operations run in:

    release.py bump --bump minor --prerelease-tag a1   # branch, write, commit
    release.py changelog                              # the HISTORY.md entry
    release.py pr                                     # push, open the PR
    #                                                 # ... review and merge ...
    release.py check                                  # changes nothing
    release.py tag --push                             # this publishes

`bump` comes first because `changelog` commits, and it cannot commit to `main`.

Each step stops rather than chaining into the next. Merging is deliberately not automated: the whole
point of releasing from a tag is that the tagged commit is one a human approved and `tests-passed`
has seen, and a script that merged its own pull request would hand that back.

`--` forwards the rest of the arguments to the tool an operation drives, so `pr` can take any flag
`gh pr create` accepts without this file growing a passthrough for each one:

    release.py pr -- --draft --reviewer michaelklishin

Every operation that changes something takes `--dry-run`, which prints the git, `gh` and file
changes it would make and makes none of them. `compute`, `classify` and `check` have no such flag
because they only read.
"""

from __future__ import annotations

import argparse
import os
import pathlib
import re
import subprocess
import sys
import tempfile

_HERE = pathlib.Path(__file__).resolve().parent
_ROOT = _HERE.parent

if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))

import changelog  # noqa: E402
import release_version  # noqa: E402
import tag_release  # noqa: E402

CheckFailed = tag_release.CheckFailed

#: Release branches have been named this since 1.3.0: `pika-1.4.2`, `pika-1.4.1`.
#: Reused rather than invented, so the history reads consistently.
BRANCH = 'pika-{version}'

#: And bump commits have been subjects of this form over the same span.
COMMIT_SUBJECT = 'pika {version}'

#: Version entries in `HISTORY.md`. Matched as a literal rather than by parsing
#: headings, because the upgrade notes contain fenced code blocks whose contents
#: include lines starting with `#`.
HISTORY_ENTRY = '## ['

#: Everything below this heading is hand-written history for 1.3.0 and earlier
#: that the generator does not reproduce. `changelog` must never reach it.
HISTORY_FLOOR = '## Version History'

#: Applied by `pr` unless `--label` or `--assignee` names others. A version bump
#: is packaging work, and AGENTS.md asks for a label and an assignee.
DEFAULT_LABELS = ('A-packaging',)
DEFAULT_ASSIGNEES = ('lukebakken',)

#: The two files that declare the version, and how to find it in each.
VERSION_FILES = (
    (pathlib.Path('pyproject.toml'), 'PYPROJECT_VERSION'),
    (pathlib.Path('pika/__init__.py'), 'INIT_VERSION'),
)

#: The heading the upgrade notes live under, in both files that carry them.
#: The comparison anchors here rather than on the first `### ` so the paragraph
#: introducing the sections is compared too.
UPGRADE_HEADING = '## Upgrading'

#: Prepended to the generated release notes. Paired with the `## Upgrading`
#: section in `HISTORY.md`; `check` verifies the two still agree.
PREAMBLE = pathlib.Path('.github/release-notes-preamble.md')


def run(command: tuple[str, ...], dry_run: bool = False) -> str:
    """
    Run *command* in the repository root, or print it under `--dry-run`.

    :param command: The argument vector.
    :param dry_run: Print rather than run.
    :returns: Captured stdout, or the empty string under `--dry-run`.
    :raises CheckFailed: if the command fails.
    """
    if dry_run:
        print('  would run: ' + ' '.join(command))
        return ''
    result = subprocess.run(command,
                            cwd=_ROOT,
                            capture_output=True,
                            text=True,
                            check=False)
    if result.returncode != 0:
        raise CheckFailed(f'{" ".join(command[:3])} failed: '
                          f'{result.stderr.strip() or result.stdout.strip()}')
    return result.stdout.strip()


def resolve_version(args: argparse.Namespace) -> str:
    """
    Return the version an operation should act on.

    Either stated outright with `--version`, or computed from a bump the way the
    old workflow did it. Computing is the default path for `bump`, because
    picking the next version in a pre-release cycle is the step that used to be
    got wrong: every value except `none` increments the base version.

    :param args: Parsed arguments, with `version`, `bump` and `prerelease_tag`.
    :returns: The version to act on.
    :raises CheckFailed: if the arguments do not name one.
    """
    if args.version and args.bump:
        raise CheckFailed('--version and --bump are alternatives; pass one')
    if args.version:
        return str(args.version)
    if not args.bump:
        raise CheckFailed('pass --version, or --bump to compute one')

    current = tag_release.version_in(pathlib.Path('pyproject.toml'),
                                     tag_release.PYPROJECT_VERSION, _ROOT)
    mode = 'prerelease' if args.prerelease_tag else 'release'
    try:
        return release_version.compute(current,
                                       args.bump,
                                       mode,
                                       prerelease_tag=args.prerelease_tag)
    except ValueError as exc:
        raise CheckFailed(str(exc)) from exc


def write_version(version: str, dry_run: bool = False) -> None:
    """
    Write *version* into both files that declare it.

    :param version: The version to write.
    :param dry_run: Report rather than write.
    """
    edits = (
        (pathlib.Path('pyproject.toml'), tag_release.PYPROJECT_VERSION,
         f'version = "{version}"'),
        (pathlib.Path('pika/__init__.py'), tag_release.INIT_VERSION,
         f"__version__ = '{version}'"),
    )
    for path, pattern, replacement in edits:
        target = _ROOT / path
        with target.open(encoding='utf-8', newline='') as handle:
            body = handle.read()
        updated, count = pattern.subn(replacement, body, count=1)
        if count != 1:
            raise CheckFailed(f'{path} does not declare a version to replace')
        if dry_run:
            print(f'  would write {path}: {replacement}')
            continue
        with target.open('w', encoding='utf-8', newline='') as handle:
            handle.write(updated)
        print(f'  {path}: {replacement}')


def current_branch() -> str:
    """
    Return the branch currently checked out.

    :returns: The branch name.
    """
    return tag_release.git('rev-parse', '--abbrev-ref', 'HEAD')


def require_clean_main() -> None:
    """
    Refuse unless `main` is checked out, clean, and level with the remote.

    :raises CheckFailed: on the first condition that does not hold.
    """
    branch = current_branch()
    if branch != tag_release.RELEASE_BRANCH:
        raise CheckFailed(f'on branch {branch!r}, not '
                          f'{tag_release.RELEASE_BRANCH!r}')
    dirty = tag_release.git('status', '--porcelain')
    if dirty:
        raise CheckFailed('working tree is not clean:\n' + dirty)
    tag_release.git(
        'fetch', '--quiet', 'origin',
        f'+refs/heads/{tag_release.RELEASE_BRANCH}:'
        f'refs/remotes/origin/{tag_release.RELEASE_BRANCH}')
    head = tag_release.git('rev-parse', 'HEAD')
    remote = tag_release.git('rev-parse',
                             f'origin/{tag_release.RELEASE_BRANCH}')
    if head != remote:
        raise CheckFailed(
            f'HEAD is {head[:12]} but origin/{tag_release.RELEASE_BRANCH} is '
            f'{remote[:12]}; pull first')


def notes_agree() -> str:
    """
    Report whether the upgrade notes still match between their two homes.

    The two are a pair: `HISTORY.md` renders on the documentation site and the preamble is prepended
    to the GitHub release notes. They drifted by hand twice while being written, which is why this is
    checked rather than trusted.

    Agreement means the preamble *starts with* what `HISTORY.md` says, not that the two are equal.
    The preamble is allowed to add release-notes-only material after the shared sections, and does:
    it ends by pointing at the changelog, which would be self-referential in the changelog itself.

    An absent or empty preamble agrees vacuously, because `release.yaml` treats both as "generated
    notes only" and RELEASE.md tells you to delete the file when a release needs no guidance.

    :returns: The empty string when they agree, else what differs.
    """
    preamble_path = _ROOT / PREAMBLE
    if not preamble_path.is_file():
        return ''
    published_text = preamble_path.read_text(encoding='utf-8')
    if not published_text.strip():
        return ''
    history_text = (_ROOT / 'HISTORY.md').read_text(encoding='utf-8')

    def region(text: str) -> list[str] | None:
        """
        Return the upgrade notes in *text* as lines, or None if absent.

        Anchored on the `## Upgrading` heading rather than the first `### ` so the introductory
        paragraph under it is compared too; anchoring deeper let a reworded intro pass. `find`
        returning -1 is checked rather than used as a slice bound, which silently produced an empty
        region and an unconditional pass.
        """
        start = text.find(UPGRADE_HEADING)
        if start < 0:
            return None
        # Ends at the next `## ` heading of any kind, not at the next `## [`
        # version entry. RELEASE.md keeps the previous release's `## Upgrading`
        # section, so anchoring on the version entry swallowed it into the
        # region and reported every release after the first as drifted.
        rest = text[start + len(UPGRADE_HEADING):]
        offset, fence = None, False
        for match in re.finditer(r'^(```|~~~|## )', rest, re.MULTILINE):
            if match.group(1) in ('```', '~~~'):
                fence = not fence
                continue
            if not fence:
                offset = match.start()
                break
        end = (len(text) if offset is None else start + len(UPGRADE_HEADING) +
               offset)
        return text[start:end].strip().splitlines()

    shared = region(history_text)
    published = region(published_text)
    if shared is None:
        return f'HISTORY.md has no {UPGRADE_HEADING!r} section to compare'
    if published is None:
        return f'{PREAMBLE} has no {UPGRADE_HEADING!r} section to compare'

    # Compared as lines, not with `startswith` on the raw text: `splitlines`
    # collapses CRLF where `startswith` does not, which reported a drift that was
    # only a line ending and printed a negative count for it.
    if published[:len(shared)] == shared:
        return ''
    for number, (one, two) in enumerate(zip(shared, published), start=1):
        if one != two:
            # Windowed on the first differing character rather than truncated at
            # a fixed width, or a difference past the cut prints two lines that
            # look identical.
            column = next((at for at, (left, right) in enumerate(zip(one, two))
                           if left != right), min(len(one), len(two)))
            start = max(column - 24, 0)
            return (f'they diverge at line {number}, column {column + 1} of '
                    f'the notes:\n'
                    f'    HISTORY.md: ...{one[start:column + 48]}\n'
                    f'    {PREAMBLE}: ...{two[start:column + 48]}')
    return (f'{PREAMBLE} stops after {len(published)} lines, {len(shared)} '
            f'expected; the notes are truncated rather than reworded')


def refuse_backwards(version: str) -> None:
    """
    Refuse a version that does not move past what has already been published.

    `compute` only refuses a version equal to the current one, which is not the same thing. On
    `main` at 1.4.0 the documented `--bump none --prerelease-tag b1` yields `1.4.0b1`, which sorts
    below both 1.4.0 and the published 1.4.4, and every other guard passes it: `classify` calls it a
    pre-release, and PyPI has no such version to collide with. It would publish a pre-release of a
    version that shipped a year ago.

    :param version: The version about to be tagged or published.
    :raises CheckFailed: if *version* does not sort above the newest release.
    """
    newest = previous_release()
    if release_version.Version(version) <= release_version.Version(newest):
        raise CheckFailed(
            f'{version} does not move past {newest}, the newest released '
            f'version. A release has to go forwards: pick a bump that advances '
            f'past it')


def reachable_release() -> str:
    """
    Return the newest released tag reachable from HEAD.

    The range a changelog covers is "what is new on this branch", and reachability is exactly that
    question, so `git describe` is right here. It is wrong for "what is the newest released
    version", which `previous_release` answers by PEP 440 order, because 1.4.1 through 1.4.4 were
    cut from `1.4.x` and are not ancestors of `main`. Confusing the two produces either a changelog
    that re-lists three released versions or a release that goes backwards.

    :returns: The newest tag reachable from HEAD.
    :raises CheckFailed: if there is none.
    """
    described = tag_release.git('describe', '--tags', '--abbrev=0', check=False)
    if not described:
        raise CheckFailed('no tag is reachable from HEAD to generate a '
                          'changelog since; pass --since-tag')
    return described


def previous_release() -> str:
    """
    Return the newest released version, by version order rather than reachability.

    `git describe --tags --abbrev=0` is the obvious answer and the wrong one. It
    reports the newest tag *reachable from HEAD*, and pika's patch releases are
    cut from maintenance branches: 1.4.1 through 1.4.4 came off `1.4.x`, so none
    of them is an ancestor of `main` and `describe` answers 1.4.0 there.
    Generating a changelog since 1.4.0 would re-list everything already written
    up for 1.4.1, 1.4.2 and 1.4.3.

    The same confusion, in the other direction, made `git tag --contains` claim
    a change was unreleased when 1.4.4 had shipped it.

    :returns: The newest tag this scheme would publish, by PEP 440 order.
    :raises CheckFailed: if the repository has no such tag.
    """
    # Tags are fetched explicitly: both fetch sites name a `main` refspec, which
    # does not bring tags, so a fresh or `--no-tags` clone would compare against
    # whatever happened to be local.
    tag_release.git('fetch', '--quiet', '--tags', 'origin', check=False)
    released = []
    for line in tag_release.git('tag', '--list').splitlines():
        name = line.strip()
        if not name:
            continue
        try:
            release_version.classify(name)
        except ValueError:
            # Tags from before this scheme, such as `v0.9.5`.
            continue
        released.append(name)
    if not released:
        raise CheckFailed('no released tag to generate a changelog since; '
                          'pass --since-tag')
    return max(released, key=release_version.Version)


def cmd_compute(args: argparse.Namespace) -> int:
    """
    Print the version a bump would produce.

    Serves two callers, which is why it takes more than `bump` needs. A human runs it to pick the
    next version and lets `--current` and `--mode` default. `release.yaml` runs it on the dispatch
    path and passes all three, because `test` keys a throwaway `.devN` on the workflow run number.

    :param args: Parsed arguments.
    :returns: Process exit status.
    """
    if args.version:
        raise CheckFailed(
            'compute derives a version; pass --current to say what to derive '
            'it from, not --version')
    current = args.current or tag_release.version_in(
        pathlib.Path('pyproject.toml'), tag_release.PYPROJECT_VERSION, _ROOT)
    mode = args.mode or ('prerelease' if args.prerelease_tag else 'release')
    if not args.bump:
        raise CheckFailed('compute needs --bump')
    try:
        print(
            release_version.compute(current,
                                    args.bump,
                                    mode,
                                    prerelease_tag=args.prerelease_tag,
                                    run_number=args.run_number))
    except ValueError as exc:
        raise CheckFailed(str(exc)) from exc
    return 0


def cmd_classify(args: argparse.Namespace) -> int:
    """
    Print what a pushed tag implies, as `$GITHUB_OUTPUT` lines.

    This is the one operation a machine runs: `release.yaml` appends its output
    directly, so every line is a bare `key=value` and a failure prints nothing
    to stdout.

    :param args: Parsed arguments.
    :returns: Process exit status.
    """
    try:
        implied = release_version.classify(args.tag)
    except ValueError as exc:
        raise CheckFailed(str(exc)) from exc
    for key, value in implied.items():
        print(f'{key}={value}')
    return 0


def cmd_bump(args: argparse.Namespace) -> int:
    """
    Branch, write the version into both files, and commit.

    Local and reversible, which is why it is a separate operation from `pr`: nothing leaves the
    machine until you ask it to.

    :param args: Parsed arguments.
    :returns: Process exit status.
    """
    version = resolve_version(args)
    tag_release.check_releasable(version)
    refuse_backwards(version)
    require_clean_main()

    # Before the branch exists, so a file that does not declare a version fails
    # with nothing created. `write_version` would otherwise raise after
    # `checkout -b`, leaving a branch behind for the next run to refuse.
    for path, attribute in VERSION_FILES:
        tag_release.version_in(path, getattr(tag_release, attribute), _ROOT)

    branch = args.branch or BRANCH.format(version=version)
    if tag_release.git('branch', '--list', branch):
        raise CheckFailed(f'branch {branch} already exists; delete it with '
                          f'`git branch -D {branch}` if it is stale')
    if tag_release.git('tag', '--list', version):
        raise CheckFailed(f'tag {version} already exists, so {version} has '
                          f'been cut before')

    print(f'bumping to {version} on {branch}')
    run(('git', 'checkout', '-b', branch), args.dry_run)
    # Unwound on failure. `git commit` can still fail after the branch exists -
    # no identity under `user.useConfigOnly`, a hook, a full disk - and this
    # operation's docstring calls it reversible, so it has to be. Leaving the
    # branch checked out also makes the tool's own advice, `git branch -D`, fail
    # with "used by worktree".
    try:
        write_version(version, args.dry_run)
        run(('git', 'add', 'pyproject.toml', 'pika/__init__.py'), args.dry_run)
        run(('git', 'commit', '--message',
             COMMIT_SUBJECT.format(version=version)), args.dry_run)
    except (CheckFailed, OSError):
        if not args.dry_run:
            run(('git', 'checkout', '--force', tag_release.RELEASE_BRANCH))
            run(('git', 'branch', '--delete', '--force', branch))
        raise
    if not args.dry_run:
        print('committed. `release.py pr` opens the pull request')
    return 0


def cmd_pr(args: argparse.Namespace) -> int:
    """
    Push the release branch and open its pull request.

    :param args: Parsed arguments.
    :returns: Process exit status.
    """
    branch = current_branch()
    if branch == tag_release.RELEASE_BRANCH:
        raise CheckFailed(f'on {tag_release.RELEASE_BRANCH}; run '
                          f'`release.py bump` first')
    # `rev-parse --abbrev-ref HEAD` answers the literal string `HEAD` when
    # detached, which is not `main` and so passed the test above. Pushing it
    # creates a branch named `HEAD`, or fails inside git with a message about
    # refnames that says nothing about the real problem.
    if branch == 'HEAD':
        raise CheckFailed('HEAD is detached; check out the release branch')
    dirty = tag_release.git('status', '--porcelain')
    if dirty:
        raise CheckFailed('working tree is not clean:\n' + dirty)

    version = tag_release.version_in(pathlib.Path('pyproject.toml'),
                                     tag_release.PYPROJECT_VERSION, _ROOT)
    # A pre-release belongs to the milestone of the version it leads to, so
    # 1.5.0a1 is milestone 1.5.0. `--milestone ''` opts out, for a release whose
    # milestone does not exist yet.
    major, minor, patch = release_version.base_of(version)
    milestone = (f'{major}.{minor}.{patch}'
                 if args.milestone is None else args.milestone)

    existing = ('' if args.dry_run else run(
        ('gh', 'pr', 'list', '--head', branch, '--state', 'open', '--json',
         'number', '--jq', '.[].number')))
    if existing:
        raise CheckFailed(f'pull request #{existing} is already open for '
                          f'{branch}')

    body = (f'Bumps the version to `{version}` so the release can be tagged.\n'
            f'\n'
            f'Merging this does not publish anything. Pushing the `{version}` '
            f'tag afterwards is what triggers `release.yaml`, which builds, '
            f'publishes to PyPI, creates the GitHub Release and deploys the '
            f'documentation. See RELEASE.md.\n'
            f'\n' + (f'Milestone: {milestone}\n' if milestone else ''))
    # A temporary file, not `_ROOT / '.git'`: `.git` is a regular file in a
    # linked worktree or a submodule, so writing under it raises
    # `NotADirectoryError`. Nothing cleaned the old path up either.
    print(f'opening the pull request for {version}')
    descriptor, name = tempfile.mkstemp(suffix='.md', prefix='pika-release-pr-')
    with os.fdopen(descriptor, 'w', encoding='utf-8') as handle:
        handle.write(body)
    body_file = pathlib.Path(name)

    run(('git', 'push', '--set-upstream', 'origin', branch), args.dry_run)
    create = [
        'gh',
        'pr',
        'create',
        '--base',
        tag_release.RELEASE_BRANCH,
        '--head',
        branch,
        '--title',
        COMMIT_SUBJECT.format(version=version),
        '--body-file',
        str(body_file),
    ]
    for label in (args.label if args.label is not None else DEFAULT_LABELS):
        create += ['--label', label]
    if milestone:
        create += ['--milestone', milestone]
    for assignee in (args.assignee
                     if args.assignee is not None else DEFAULT_ASSIGNEES):
        create += ['--assignee', assignee]
    # Passthrough first: both `gh` and the changelog generator take the last
    # occurrence of a repeated flag, so appending it would let a caller silently
    # retarget `--base` or rewrite `--title`.
    try:
        url = run(
            tuple(create[:3]) + tuple(args.passthrough) + tuple(create[3:]),
            args.dry_run)
    finally:
        # Not `unlink(missing_ok=True)`: that keyword is 3.8+, and
        # `requires-python` is 3.7, which the test matrix still covers.
        try:
            body_file.unlink()
        except OSError:
            pass
    if url:
        print(f'  {url}')
        print('  review and merge it, then `release.py tag --push`')
    return 0


def cmd_tag(args: argparse.Namespace) -> int:
    """
    Create the signed release tag, and optionally push it.

    :param args: Parsed arguments.
    :returns: Process exit status.
    """
    version = tag_release.version_in(pathlib.Path('pyproject.toml'),
                                     tag_release.PYPROJECT_VERSION, _ROOT)
    if args.version and args.version != version:
        raise CheckFailed(f'--version is {args.version} but pyproject.toml '
                          f'says {version}')

    implied = tag_release.check(version,
                                verify_pypi=not args.no_verify_pypi,
                                root=_ROOT)
    # `tag_release.check` covers the repository and PyPI; these cover the
    # documents. Without them `check` could exit 1 for a missing changelog entry
    # and `tag --push` succeed on the very next line.
    # `verify_pypi=False` rather than filtering the message: `tag_release.check`
    # has already asked, and a `startswith` on an error string is a contract
    # nobody can see. It also meant `--no-verify-pypi` still reached PyPI, so a
    # PyPI outage blocked tagging entirely and the error named the flag that was
    # already passed.
    problems = readiness(version, verify_pypi=False)
    if problems and not args.skip_readiness:
        raise CheckFailed('not ready to release:\n' +
                          '\n'.join(f'  {problem}' for problem in problems) +
                          '\n  pass --skip-readiness to tag anyway')
    key = tag_release.signing_key(args.sign_key)
    command = tag_release.tag_command(version, key)

    print(f'pika {version} is a {implied["mode"]}')
    print(f'  docs version {implied["docs_version"]!r}, '
          f'aliases {implied["docs_aliases"]!r}, '
          f'site default {implied["docs_set_default"]}')
    print(f'  commit {tag_release.git("rev-parse", "--short", "HEAD")} on '
          f'{tag_release.RELEASE_BRANCH}')

    run(command, args.dry_run)
    if args.dry_run:
        if not args.push:
            print('  would stop here; pass --push to publish')
        else:
            print(f'  would run: git push origin {version}')
        return 0

    # Verified here rather than suggested, because an unsigned or lightweight tag
    # is only discoverable after the fact and the tag is the record of what the
    # published bytes were built from.
    # Removed again if it cannot be verified. A tag has to exist before it can be
    # checked, so the only way to keep the operation atomic is to undo it.
    # `cmd_bump` carries the same shape for the same reason, and leaving the tag
    # behind means the next run refuses with `already exists` instead.
    try:
        if tag_release.git('cat-file', '-t', version) != 'tag':
            raise CheckFailed(f'{version} is not an annotated tag')
        run(('git', 'tag', '--verify', version))
    except CheckFailed:
        run(('git', 'tag', '--delete', version))
        raise
    print(f'created {version}, annotated and signature verified')
    if not args.push:
        print(f'not pushed. `git push origin {version}` publishes it')
        return 0
    # `--no-follow-tags` and an explicit refspec, because a bare
    # `git push origin <tag>` honours `push.followTags`. The refspec alone is
    # not enough: `--follow-tags` pushes every annotated tag reachable from the
    # ref-tips being pushed, and a tag is a ref-tip, so the sibling tags went
    # too. GitHub creates no event at all for more than three tags in one push,
    # so the release would silently not happen while this printed success.
    run(('git', 'push', '--no-follow-tags', 'origin',
         f'refs/tags/{version}:refs/tags/{version}'))
    print(f'pushed {version}; release.yaml is now running')
    return 0


def cmd_changelog(args: argparse.Namespace) -> int:
    """
    Generate the `HISTORY.md` entry for a version and prepend it.

    Only the new entry is generated, with `--since-tag`, and it is inserted above the newest
    existing one. Regenerating the whole file would reach the hand-written history below `## Version
    History`, which the generator does not reproduce.

    :param args: Parsed arguments.
    :returns: Process exit status.
    """
    # Read, not computed. `bump` has already written the release version into
    # the files, so a `--bump` here would compound off it and generate an entry
    # for a version this branch is not releasing.
    version = tag_release.version_in(pathlib.Path('pyproject.toml'),
                                     tag_release.PYPROJECT_VERSION, _ROOT)
    if args.version and args.version != version:
        raise CheckFailed(f'--version is {args.version} but pyproject.toml '
                          f'says {version}; run `release.py bump` first')
    tag_release.check_releasable(version)

    branch = current_branch()
    if branch == tag_release.RELEASE_BRANCH:
        raise CheckFailed(
            f'on {tag_release.RELEASE_BRANCH}, which this would have to commit '
            f'to. Run `release.py bump` first and generate the entry on the '
            f'release branch, so it lands in the same pull request')
    # The same two guards `cmd_pr` carries, for the same reasons: a commit on a
    # detached HEAD is orphaned by the next checkout, and anything already
    # modified would be swept into the changelog commit.
    if branch == 'HEAD':
        raise CheckFailed('HEAD is detached; check out the release branch')
    dirty = tag_release.git('status', '--porcelain')
    if dirty:
        raise CheckFailed('working tree is not clean:\n' + dirty)

    history = _ROOT / 'HISTORY.md'
    with history.open(encoding='utf-8', newline='') as handle:
        body = handle.read()

    anchor = body.find(HISTORY_ENTRY)
    if anchor < 0:
        raise CheckFailed(f'HISTORY.md has no {HISTORY_ENTRY!r} entry to '
                          f'insert above')
    if anchor > body.find(HISTORY_FLOOR) >= 0:
        raise CheckFailed(f'the newest entry is below {HISTORY_FLOOR!r}, '
                          f'which this will not edit')
    if f'{HISTORY_ENTRY}{version}]' in body:
        raise CheckFailed(f'HISTORY.md already has an entry for {version}')

    since = args.since_tag or reachable_release()
    # The milestone is the version's base, so 1.5.0a1 draws on milestone 1.5.0.
    major, minor, patch = release_version.base_of(version)
    milestone = f'{major}.{minor}.{patch}'
    print(f'generating the {version} entry from milestone {milestone}, '
          f'since {since}')
    # Both runners are injected rather than left to default. `changelog`'s own
    # helpers use the process's working directory, which is not necessarily the
    # repository root - invoking from a subdirectory would read the wrong repo -
    # and routing `gh` through `run` means one place talks to the network.
    entry = changelog.generate(milestone,
                               version,
                               since,
                               until='HEAD',
                               git=tag_release.git,
                               gh=lambda *args: run(('gh', *args)))

    if args.dry_run:
        print(entry)
        print(f'  dry run: {len(entry.splitlines())} lines, not written')
        return 0

    # `newline=''` on both halves of the round trip, via `open` rather than
    # `read_text`/`write_text`, whose `newline` parameters are 3.13 and 3.10
    # while the floor is 3.7. Without it the default translates on read and
    # again on write, so editing one line rewrote the whole file as CRLF on
    # Windows.
    with history.open('w', encoding='utf-8', newline='') as handle:
        handle.write(body[:anchor] + entry + '\n\n' + body[anchor:])
    print(f'  prepended {len(entry.splitlines())} lines to HISTORY.md')
    run(('git', 'add', 'HISTORY.md'))
    run(('git', 'commit', '--message', f'Add the {version} changelog entry'))
    print('  committed. Read it before opening the pull request; the generator '
          'groups by label')
    return 0


def readiness(version: str, verify_pypi: bool = True) -> list[str]:
    """
    Return every reason *version* is not ready to release.

    Shared with `tag`, which used to check none of this: a release could be tagged and published
    with no changelog entry and a stale preamble, because those two checks lived only in `check`.

    :param version: The version about to be released.
    :returns: Problems, empty when there are none.
    """
    problems = []

    try:
        release_version.classify(version)
    except ValueError as exc:
        problems.append(str(exc))

    try:
        refuse_backwards(version)
    except (CheckFailed, ValueError) as exc:
        # `ValueError` too: `packaging` raises `InvalidVersion` for an
        # unparseable version, which escaped and threw away the `classify`
        # problem collected just above it.
        problems.append(str(exc))

    # Both files, not just one: `tag_release.check` loops over the pair and the
    # friendlier copy here dropped half of it, so a wrong `pyproject.toml` was
    # reported only as a disagreement in `pika/__init__.py`.
    for path, attribute in VERSION_FILES:
        written = tag_release.version_in(path, getattr(tag_release, attribute),
                                         _ROOT)
        if written != version:
            problems.append(f'{path} says {written}, not {version}')

    if f'{HISTORY_ENTRY}{version}]' not in (_ROOT / 'HISTORY.md').read_text(
            encoding='utf-8'):
        problems.append(f'HISTORY.md has no entry for {version}; run '
                        f'`release.py changelog`')

    divergence = notes_agree()
    if divergence:
        problems.append(f'HISTORY.md and {PREAMBLE} are a pair, but '
                        f'{divergence}')

    if verify_pypi and tag_release.on_pypi(version):
        problems.append(f'PyPI already has {version}')

    return problems


def cmd_check(args: argparse.Namespace) -> int:
    """
    Report on everything a release needs, changing nothing.

    :param args: Parsed arguments.
    :returns: Process exit status, nonzero if anything is wrong.
    """
    version = (args.version or tag_release.version_in(
        pathlib.Path('pyproject.toml'), tag_release.PYPROJECT_VERSION, _ROOT))
    problems = readiness(version, verify_pypi=not args.no_verify_pypi)
    if not problems:
        print(f'ready to release {version}')
        return 0
    for problem in problems:
        print(f'problem: {problem}')
    return 1


def build_parser() -> argparse.ArgumentParser:
    """
    Return the argument parser for every operation.

    :returns: The parser.
    """
    parser = argparse.ArgumentParser(
        prog='release.py',
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest='operation', required=True)

    def add(name, handler, help_text, version=True, dry_run=True):
        each = sub.add_parser(name, help=help_text)
        each.set_defaults(func=handler, dry_run=False, passthrough=())
        if version:
            each.add_argument('--version', default='')
            each.add_argument('--bump',
                              default='',
                              choices=('', *release_version.BUMPS))
            each.add_argument('--prerelease-tag',
                              default='',
                              dest='prerelease_tag')
        if dry_run:
            each.add_argument('--dry-run', action='store_true')
        return each

    compute = add('compute',
                  cmd_compute,
                  'print the version a bump would produce',
                  dry_run=False)
    compute.add_argument('--current', default='')
    compute.add_argument('--mode', default='')
    compute.add_argument('--run-number', default='', dest='run_number')

    classify = sub.add_parser('classify',
                              help='print what a pushed tag implies')
    classify.add_argument('--tag', required=True)
    classify.set_defaults(func=cmd_classify, dry_run=False, passthrough=())

    bump = add('bump', cmd_bump, 'branch, write the version, and commit')
    bump.add_argument('--branch', default='')

    pr = add('pr',
             cmd_pr,
             'push the branch and open the pull request',
             version=False)
    # `default=None` rather than the list itself: `action='append'` appends to a
    # default instead of replacing it, so a non-empty default would make
    # `--label C-bug` mean `A-packaging` and `C-bug`, with no way to drop the
    # first. The defaults are applied in `cmd_pr` instead.
    pr.add_argument('--label', action='append', default=None)
    pr.add_argument('--assignee', action='append', default=None)
    pr.add_argument('--milestone', default=None)

    # `version=False`: `tag` and `check` read the version from the files, so
    # accepting `--bump` and `--prerelease-tag` only let a maintainer paste the
    # flags from the `bump` step and have them silently ignored.
    tag = add('tag',
              cmd_tag,
              'create the signed tag, and maybe push it',
              version=False)
    tag.add_argument('--version', default='')
    tag.add_argument('--skip-readiness',
                     action='store_true',
                     dest='skip_readiness')
    tag.add_argument('--push', action='store_true')
    tag.add_argument('--sign-key', default='', dest='sign_key')
    tag.add_argument('--no-verify-pypi',
                     action='store_true',
                     dest='no_verify_pypi')

    changelog = add('changelog',
                    cmd_changelog,
                    'generate the HISTORY.md entry and prepend it',
                    version=False)
    changelog.add_argument('--version', default='')
    changelog.add_argument('--since-tag', default='', dest='since_tag')

    check = add('check',
                cmd_check,
                'report on release readiness, change nothing',
                version=False,
                dry_run=False)
    check.add_argument('--version', default='')
    check.add_argument('--no-verify-pypi',
                       action='store_true',
                       dest='no_verify_pypi')

    return parser


def main(argv: list[str] | None = None) -> int:
    """
    Run one operation.

    :param argv: Argument list, defaulting to `sys.argv[1:]`.
    :returns: Process exit status.
    """
    argv = list(sys.argv[1:] if argv is None else argv)
    passthrough: list[str] = []
    if '--' in argv:
        split = argv.index('--')
        argv, passthrough = argv[:split], argv[split + 1:]

    args = build_parser().parse_args(argv)
    args.passthrough = tuple(passthrough)
    try:
        return int(args.func(args))
    except (CheckFailed, OSError, TypeError, ValueError) as exc:
        # `ValueError` because the helpers raise it for a malformed version and
        # only some call sites wrap it, and `OSError` because a missing
        # `HISTORY.md` or an unreachable `gh` are both ordinary operator
        # errors. A
        # traceback where every sibling prints one clean line is worse than a
        # slightly broad except here.
        #
        # `::error::` so a failure is an annotation on the workflow run, which
        # matters because `classify` runs there and the caller reads stdout for
        # the version. Locally the prefix would be noise, so it is conditional.
        prefix = '::error::' if os.environ.get('GITHUB_ACTIONS') else 'error: '
        print(f'{prefix}{exc}', file=sys.stderr)
        return 1


if __name__ == '__main__':
    sys.exit(main())
