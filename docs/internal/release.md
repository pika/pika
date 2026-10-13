# Releasing

The long-form record for the release machinery. `RELEASE.md` is the procedure; this is why it has the shape it does.

## How it works

`.ci/release.py` is the only command. `bump` writes the version to `pyproject.toml` and `pika/__init__.py` on a branch, `changelog` writes the `HISTORY.md` entry, `pr` opens the pull request, `check` reports readiness, and `tag` creates the signed tag. `.ci/release_version.py`, `.ci/changelog.py` and `.ci/tag_release.py` are libraries with no command line of their own, so there is one place that talks to git, `gh` and PyPI.

Pushing the tag is the whole trigger. `.github/workflows/release.yaml` runs on `push: tags`, verifies the tag, builds once, and publishes the artifact it built.

The workflow never writes to `main` because it cannot. `main` requires a pull request and restricts pushes to the Maintainers team, and classic branch protection has no way to grant `GITHUB_TOKEN` a bypass. So the version bump lands as an ordinary pull request and the tag is pushed afterwards, which also means the tagged commit is one `tests-passed` has already seen.

## Invariants

**Every guard on the irreversible path holds on both sides of the push.** Nothing obliges a maintainer to create the tag with `release.py tag`: a hand-edited version bump plus `git tag -s` and `git push` skips every local check, and then the workflow is the only guard. `tag_release.check` and `tag_release.verify_tag` therefore overlap.

**The two differ only in what they can know.** `check` runs before the tag exists and refuses if it does. `verify_tag` runs after, so it reasons about the tag object and the commit it names rather than about a working tree. A check moved between them has to have its preconditions re-derived rather than copied: `verify_tag` once used `newest_released()` unchanged from `check`, and because the tag exists by then it was its own predecessor, so `1.5.0 <= 1.5.0` refused every release. `newest_released` takes an `exclude` argument for this reason, and only `verify_tag` passes it.

**Re-tagging an already-published version is PyPI's to refuse,** not the ordering guard's. The exclusion above means the ordering check no longer sees such a tag as backwards; `on_pypi` catches it and is the authority on what shipped.

**`on_pypi` treats only a 404 as absent.** Testing for 200 made every other status a green light, so a 429, a 503 or a redirect would wave an already-published version through to `twine`.

**A tag has to exist before it can be verified,** so `cmd_tag` removes the tag again when verification fails. Leaving it behind makes the next run refuse with `already exists` instead of the real reason. `cmd_bump` unwinds its branch for the same reason.

## Designs that were tried and rejected

**Shell in the workflow, for anything that decides.** The documentation alias comparison used `sort -V`, which ranks `1.5.0rc1` above `1.5`. The version arithmetic incremented on every bump, so a published pre-release could not be promoted to its final version. Both were untestable where they lived.

**A bare `git push origin <tag>`.** It honours `push.followTags`, and an explicit refspec alone does not stop that: `--follow-tags` pushes every annotated tag reachable from the ref-tips being pushed, and a tag is itself a ref-tip, so sibling tags went too. GitHub creates no event at all for a push of more than three tags, so the release would silently not happen while the command reported success. `--no-follow-tags` plus the explicit refspec is the form that works.

**Testing the tag signature for PGP armour only.** `git tag --sign` honours `gpg.format`, so a correctly signed tag may carry `BEGIN PGP SIGNATURE`, `BEGIN SSH SIGNATURE` or `BEGIN SIGNED MESSAGE`. A PGP-only test rejected an SSH-signed tag after it had already been pushed and advised re-creating it, which reproduces a byte-identical tag.

**`git describe --tags --abbrev=0` for the previous release.** It answers the nearest tag by commit distance rather than the newest by version, and `--tags` re-enables lightweight ones, so with pika's `wip-1617-core-2p0` and nine legacy `v*` tags it will name a branch marker as the previous release. Reachability and version order are different questions: `reachable_release` answers the first for a changelog range, `previous_release` the second for refusing a backwards version.

**`--output -` for the changelog generator.** Its help text advertises `-` for stdout and the flag writes a file named `-` instead.

## Known gaps

- Releases cannot be cut from a maintenance branch: #1736.
- Nothing enforces that `docs/internal/` stays unpublished: #1738.
