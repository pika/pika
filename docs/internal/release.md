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

## The changelog entry

`.ci/changelog.py` builds the `HISTORY.md` entry from the GitHub milestone, cross-checked against the commit range. The milestone decides membership because that is where the editorial decision about a release is recorded; the range only reports the three ways the two can disagree, and acts on none of them. An earlier version let the range decide membership instead, which papered over milestone data errors and dropped 20 of the 62 numbers in the committed 1.4.0 entry, because pika closes plenty of issues with no linking pull request.

It replaced `github_changelog_generator`, which crawls every tag and every closed issue by date. pika has tags back to `v0.9a` and a 1,380-line changelog, the gem needs a token nothing else in the release path uses, and it writes the heading link from the *previous* tag, which is why the committed 1.4.3 entry points at `tree/1.4.2`.

`gh` rather than a GitHub client library, because there is no official Python one: GitHub maintains Octokit for JavaScript, Ruby, .NET and Terraform only. `gh` is already used by `release.py` and `release.yaml` and already authenticated on a maintainer's machine and through `GH_TOKEN` in Actions.

### Measurements behind the constants

- `LIMIT = 500`: `gh` applies `--limit` and exits 0 with no warning, so a saturated answer loses entries silently and `_rows` refuses one instead. Both queries are date-bounded to the release window, and the widest window to date holds 104 pull requests and 55 issues.
- `MERGE_SUBJECT` and `SQUASH_SUBJECT`: `1.4.0..HEAD` holds 96 merge-style subjects and 2 squash-style. In `MERGE_SUBJECT` the leading `^` and the `.match` call anchor the same thing twice; measured, either alone still anchors, so dropping one is an equivalent mutation, and only dropping both lets `Revert "Merge pull request #5 from a"` read as a merge of #5.
- `MARKDOWN`: the committed region of `HISTORY.md` escapes `#` 106 times, `_` 26, `(` and `)` 14 each, `[` and `]` twice and `<` once, and leaves backticks alone.
- `CODE_SPAN`: `HISTORY.md` holds 166 lines carrying a code span and not one escaped character inside any of them. Escaping blindly turned `` `exchange_exchange_bindings` `` into `` `exchange\_exchange\_bindings` ``.
- `tag_date` reads `%ct` and converts in UTC, not `%cs`, which renders in the offset the commit recorded. A commit stamped `2026-05-07T08:00+09:00` is `2026-05-06` in UTC, so `%cs` would open the cross-check window a day late and drop anything merged in between. GitHub's `merged:>=` compares UTC dates.
- `milestone_issues` separates issues closed as `NOT_PLANNED` or `DUPLICATE` rather than listing them as shipped, and tests by exclusion rather than `stateReason == 'COMPLETED'` so an issue closed before GitHub recorded a reason stays in the entry. pika has three `NOT_PLANNED` among its sixty most recent closed issues.

### Where a changelog starts

`reachable_release` keeps final releases only. A pre-release is part of the release being prepared rather than a boundary before it, so counting 1.5.0a1 would start the 1.5.0 entry at a1 and then report every milestone item closed before a1 as mis-milestoned.

`readiness` asks for a `HISTORY.md` entry only for a final release, because `changelog` refuses to write one for a pre-release without `--force`. Requiring one regardless made the two halves contradict, so no pre-release could be tagged without a flag that advertises itself as something else.

## Known gaps

- Releases cannot be cut from a maintenance branch: #1736.
- Nothing enforces that `docs/internal/` stays unpublished: #1738.
