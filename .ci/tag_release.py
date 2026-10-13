"""
Create the signed tag that publishes a release.

Pushing a tag is the whole trigger for `release.yaml`, which makes `git push origin <tag>` the
single irreversible act in the release: a tag can be deleted and re-pushed, but the PyPI version it
causes cannot. So the checks the workflow runs are worth running first, locally, where failing costs
nothing. Every check here has a counterpart in the `release` job, deliberately: the point is to fail
on this machine rather than half-way through a publish.

It also spells out the tag command, which was previously a line pasted from shell history.
`--annotate` because `--follow-tags` and `git describe` both ignore lightweight tags, `--sign`
because the published tags are signed, and the message format `pika X.Y.Z` because that is what the
existing tags carry.

Creating the tag and pushing it are separate: the default stops after creating it locally, so there
is a moment to run `git show` before anything leaves the machine.
"""

from __future__ import annotations

import pathlib
import re
import subprocess
import sys
import urllib.error
import urllib.request

_HERE = pathlib.Path(__file__).resolve().parent
_ROOT = _HERE.parent

# `.ci` is not a package, and running this file puts its directory on `sys.path`
# anyway; the insert is for importing it from a test, which loads it by path.
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))

import release_version  # noqa: E402

#: The branch a release may be tagged on. `release.yaml` refuses a tag whose
#: commit is not an ancestor of `origin/main`, because `main` is the only branch
#: whose content passed `tests-passed`.
RELEASE_BRANCH = 'main'

#: Where the version is written. Both files, because `pyproject.toml` decides
#: what PyPI indexes and `pika/__init__.py` decides what `pika.__version__`
#: reports, and nothing else checks the two agree.
PYPROJECT_VERSION = re.compile(r'^version = "([^"]+)"', re.MULTILINE)
INIT_VERSION = re.compile(r'^__version__ = [\'"]([^\'"]+)[\'"]', re.MULTILINE)

#: The armour a signed tag carries. All three forms are accepted because
#: `git tag --sign` honours `gpg.format`, so a correctly signed tag may be
#: OpenPGP, SSH or x509; a PGP-only check once rejected an SSH-signed tag after
#: it had already been pushed. Presence is what is checked, not validity:
#: verifying the signature would need the signer's public key on the runner.
SIGNATURE_ARMOUR = re.compile(
    r'^-----BEGIN (?:PGP|SSH) SIGNATURE-----|^-----BEGIN SIGNED MESSAGE-----',
    re.MULTILINE)

PYPI_URL = 'https://pypi.org/pypi/pika/{version}/json'

#: urllib's default announces Python and gets refused by some indexes.
USER_AGENT = 'pika-release-tagger (+https://github.com/pika/pika)'


class CheckFailed(Exception):
    """A precondition that makes tagging unsafe."""


def git(*args: str, check: bool = True) -> str:
    """
    Run git in the repository root and return its stdout, stripped.

    :param args: Arguments after `git`.
    :param check: Raise on a non-zero exit.
    :returns: Captured stdout with surrounding whitespace removed.
    """
    result = subprocess.run(('git', *args),
                            cwd=_ROOT,
                            capture_output=True,
                            text=True,
                            check=False)
    if check and result.returncode != 0:
        raise CheckFailed(f'git {" ".join(args)} failed: '
                          f'{result.stderr.strip() or result.stdout.strip()}')
    return result.stdout.strip()


def version_in(path: pathlib.Path,
               pattern: re.Pattern[str],
               root: pathlib.Path | None = None) -> str:
    """
    Return the version *pattern* finds in *path*.

    *root* is explicit because this module and `release.py` each have their own, and they are the same
    directory in production but not under test: reading through the wrong one made `bump`'s
    pre-validation check a different file from the one `write_version` then edited, so the check
    passed and the write failed after the branch had been created.

    :param path: File to read, relative to *root*.
    :param pattern: Expression whose first group is the version.
    :param root: Directory to resolve *path* against, defaulting to this module's.
    :returns: The version as written.
    :raises CheckFailed: if the file holds no version.
    """
    base = _ROOT if root is None else root
    match = pattern.search((base / path).read_text(encoding='utf-8'))
    if match is None:
        raise CheckFailed(f'{path} does not declare a version')
    return match.group(1)


def on_pypi(version: str) -> bool:
    """
    Return whether PyPI already holds *version*.

    :param version: The version to look for.
    :returns: True if PyPI answers for it.
    :raises CheckFailed: if PyPI cannot be reached, since a silent skip would turn the one
        unrecoverable mistake into an unchecked one.
    """
    request = urllib.request.Request(PYPI_URL.format(version=version),
                                     headers={'User-Agent': USER_AGENT})
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            return bool(response.status == 200)
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            return False
        raise CheckFailed(f'PyPI answered {exc.code} for {version}') from exc
    except OSError as exc:
        raise CheckFailed(
            f'could not reach PyPI to check {version}: {exc}. Pass '
            f'--no-verify-pypi to skip, but a version already on PyPI cannot '
            f'be republished') from exc


def newest_released(root: pathlib.Path | None = None, exclude: str = '') -> str:
    """
    Return the newest tag this scheme publishes, or the empty string if there is none.

    *exclude* drops one tag from the comparison. `verify_tag` needs it because it runs after the
    push, so the tag being released is already in `git tag --list` and would otherwise be its own
    predecessor.

    :param root: Directory to run git in.
    :param exclude: A tag to leave out, or '' to consider all of them.
    :returns: The newest released version by PEP 440 order, or ''.
    """
    base = _ROOT if root is None else root
    listed = subprocess.run(('git', 'tag', '--list'),
                            cwd=base,
                            capture_output=True,
                            text=True,
                            check=False).stdout.split()
    released = []
    for name in listed:
        if exclude and name == exclude:
            continue
        try:
            release_version.classify(name)
        except ValueError:
            continue
        released.append(name)
    if not released:
        return ''
    return max(released, key=release_version.Version)


def check_releasable(version: str) -> dict[str, str]:
    """
    Return what *version* implies, refusing anything unpublishable.

    Separate from `check` because it needs no repository state: `bump` calls it before creating a
    branch, so a version that can never be released fails before anything is written.

    :param version: The version to classify.
    :returns: The release parameters, from `release_version.classify`.
    :raises CheckFailed: if this scheme does not publish *version*.
    """
    try:
        return release_version.classify(version)
    except ValueError as exc:
        raise CheckFailed(str(exc)) from exc


def check(version: str,
          verify_pypi: bool = True,
          root: pathlib.Path | None = None) -> dict[str, str]:
    """
    Refuse every reason this tag should not be created.

    *root* is threaded through for the reason `version_in` documents. This was the one call site
    that took the default, so it read a different tree from its caller, which made `cmd_tag`
    impossible to drive from a fixture. That is why `cmd_tag` had no tests, and why three defects in
    it shipped.

    :param version: The version to tag, which must equal both files.
    :param verify_pypi: Ask PyPI whether the version is already published.
    :param root: Directory to resolve the version files against.
    :returns: The release parameters the tag implies, from `release_version.classify`.
    :raises CheckFailed: on the first precondition that does not hold.
    """
    # First, because it is the only check that needs no repository state and
    # because a tag this scheme will not publish makes the rest moot.
    implied = check_releasable(version)

    branch = git('rev-parse', '--abbrev-ref', 'HEAD')
    if branch != RELEASE_BRANCH:
        raise CheckFailed(f'on branch {branch!r}, not {RELEASE_BRANCH!r}. '
                          f'`release.yaml` refuses a tag that is not on '
                          f'{RELEASE_BRANCH}')

    # A dirty tree means the tag names a commit whose content is not what is on
    # screen, and the workflow builds from the commit rather than the tree.
    dirty = git('status', '--porcelain')
    if dirty:
        raise CheckFailed('working tree is not clean:\n' + dirty)

    # The refspec is spelled out, as it is at the two sibling call sites: a bare
    # `git fetch origin main` writes `refs/remotes/origin/main` as a convenience
    # rather than because the refspec asked for it, and in a clone whose
    # `remote.origin.fetch` is narrowed it writes only FETCH_HEAD. The next line
    # would then fail opaquely, or compare against a stale tracking ref, in the
    # one copy of this check that stands on the irreversible path.
    git('fetch', '--quiet', 'origin',
        f'+refs/heads/{RELEASE_BRANCH}:refs/remotes/origin/{RELEASE_BRANCH}')
    head = git('rev-parse', 'HEAD')
    remote = git('rev-parse', f'origin/{RELEASE_BRANCH}')
    if head != remote:
        raise CheckFailed(
            f'HEAD is {head[:12]} but origin/{RELEASE_BRANCH} is '
            f'{remote[:12]}. Merge the version bump and pull before tagging')

    for path, pattern in ((pathlib.Path('pyproject.toml'), PYPROJECT_VERSION),
                          (pathlib.Path('pika/__init__.py'), INIT_VERSION)):
        written = version_in(path, pattern, root)
        if written != version:
            raise CheckFailed(f'{path} says {written!r}, not {version!r}. Bump '
                              f'the version and merge it before tagging')

    if git('tag', '--list', version):
        raise CheckFailed(f'tag {version} already exists locally. Delete it '
                          f'with `git tag -d {version}` if it is wrong')
    if git('ls-remote', '--tags', 'origin', f'refs/tags/{version}'):
        raise CheckFailed(f'tag {version} already exists on origin')

    # Here rather than only in `release.py readiness`, because `--skip-readiness`
    # must not be able to turn it off: the message refusing a backwards version
    # names that flag, so advertising it as the escape hatch for a different
    # check made it the escape hatch for this one too.
    newest = newest_released(root)
    if newest and release_version.Version(version) <= release_version.Version(
            newest):
        raise CheckFailed(f'{version} does not move past {newest}, the newest '
                          f'released version. A release has to go forwards')

    if verify_pypi and on_pypi(version):
        raise CheckFailed(
            f'pika {version} is already on PyPI. A version is immutable and '
            f'cannot be re-uploaded even after deletion; pick the next one')

    return implied


def verify_tag(version: str,
               sha: str,
               verify_pypi: bool = True,
               root: pathlib.Path | None = None) -> dict[str, str]:
    """
    Refuse every reason a tag that has already been pushed should not publish.

    The mirror of `check`, on the other side of the push. `check` runs before the tag exists and
    refuses if it does; this runs in `release.yaml` after someone has pushed one, so the tag must
    exist, and what it can establish is about the tag object and the commit it names rather than
    about a working tree.

    The overlap with `check` is the point rather than an oversight. A tag carries no review, and
    nothing obliges anyone to create it with `release.py tag`: a hand-edited version bump plus
    `git tag -s` and `git push` skips every local check. So each guard on the irreversible path has
    to hold here too. The version-ordering guard did not, which is how a tag naming a version older
    than the newest release would have published `main` under that older name. 1.4.3 was the live
    example: never published, lower than 1.4.4, and accepted by every check the workflow ran.

    :param version: The pushed tag, which must equal both version files.
    :param sha: The commit the tag names, which must be an ancestor of `main`.
    :param verify_pypi: Ask PyPI whether the version is already published.
    :param root: Directory to resolve the version files against.
    :returns: The release parameters the tag implies, from `release_version.classify`.
    :raises CheckFailed: on the first precondition that does not hold.
    """
    # First, because a tag this scheme will not publish makes the rest moot.
    implied = check_releasable(version)

    kind = git('cat-file', '-t', version)
    if kind != 'tag':
        raise CheckFailed(
            f'{version} is a {kind}, not an annotated tag. Delete it and '
            f're-create it with `release.py tag`, which annotates and signs')
    if not SIGNATURE_ARMOUR.search(git('cat-file', 'tag', version)):
        raise CheckFailed(f'{version} carries no signature. Delete it and '
                          f're-create it with `release.py tag`')

    # The artifact is built from the files at this commit, not from the tag text,
    # so a tag disagreeing with either file would publish one version under
    # another's name. The realistic mistake is tagging before the bump landed.
    for path, pattern in ((pathlib.Path('pyproject.toml'), PYPROJECT_VERSION),
                          (pathlib.Path('pika/__init__.py'), INIT_VERSION)):
        written = version_in(path, pattern, root)
        if written != version:
            raise CheckFailed(f'{path} says {written!r}, not {version!r}. Tag '
                              f'the commit that bumps the version')

    # `main` is the only branch whose content passed `tests-passed`, so a tag
    # anywhere else would publish unreviewed code. The refspec is spelled out for
    # the reason `check` documents.
    git('fetch', '--quiet', 'origin',
        f'+refs/heads/{RELEASE_BRANCH}:refs/remotes/origin/{RELEASE_BRANCH}')
    try:
        git('merge-base', '--is-ancestor', sha, f'origin/{RELEASE_BRANCH}')
    except CheckFailed as exc:
        raise CheckFailed(
            f'{sha[:12]} is not an ancestor of origin/{RELEASE_BRANCH}. Land '
            f'the version bump on {RELEASE_BRANCH} before tagging it') from exc

    # Excluding the tag leaves re-tagging an already-published version to the
    # PyPI check below, which is the authority on what shipped.
    newest = newest_released(root, exclude=version)
    if newest and release_version.Version(version) <= release_version.Version(
            newest):
        raise CheckFailed(f'{version} does not move past {newest}, the newest '
                          f'released version. A release has to go forwards')

    if verify_pypi and on_pypi(version):
        raise CheckFailed(
            f'pika {version} is already on PyPI. A version is immutable and '
            f'cannot be re-uploaded even after deletion')

    return implied


def tag_command(version: str, signing_key: str) -> tuple[str, ...]:
    """
    Return the git invocation that creates the release tag.

    :param version: The version to tag.
    :param signing_key: The key to sign with, as `--local-user`.
    :returns: The argument vector, for printing or running.
    """
    return (
        'git',
        'tag',
        '--annotate',
        '--sign',
        f'--local-user={signing_key}',
        f'--message=pika {version}',
        version,
    )


def signing_key(override: str) -> str:
    """
    Return the key to sign with.

    :param override:`--sign-key`, or empty to read git's configuration.
    :returns: The signing key.
    :raises CheckFailed: if neither supplies one.
    """
    if override:
        return override
    configured = git('config', '--get', 'user.signingkey', check=False)
    if not configured:
        raise CheckFailed(
            'no signing key: set `user.signingkey` in git config or pass '
            '--sign-key. The published tags are signed')
    return configured
