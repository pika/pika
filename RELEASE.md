# Release process

## Before the release: update `HISTORY.md`

`HISTORY.md` is the changelog, and the release workflow does not touch it. `gh release create --generate-notes` writes the GitHub release body rather than this file, so without this step the documentation site's Changelog page stops at the previous release while PyPI carries the new one.

```bash
hatch run docs:python .ci/release.py changelog --version 1.5.0
```

The rest of this document shortens that to `release`, using the alias defined under [The steps](#the-steps).

That builds the entry from the version's **milestone**, in `.ci/changelog.py`: the closed issues and merged pull requests it carries, plus a sweep of `git log` for merged pull requests that escaped it. It needs no token beyond the `gh` authentication the rest of the release already uses.

Only the new entry is generated, inserted above the newest existing one. It will not regenerate the whole file, because everything from `## Version History` down is hand-written history for 1.3.0 and earlier; it refuses rather than reaching past that heading.

`--dry-run` prints the entry instead of writing it, which is the way to read it before it lands. Without it, the entry is written and committed on the current branch, which must not be `main`.

**Unmilestoned pull requests are excluded, and reported.** The milestone decides membership, so a pull request carrying none is not in the entry. They are mostly dependabot, and the numbers are printed so you can milestone them and re-run rather than discover the omission after publishing. Read that list before committing the entry: at least one substantive fix has turned up in it, #1694, which was milestoned once the report surfaced it.

**Read the result before committing.** Grouping follows labels: `C-enhancement` to "Implemented enhancements", `C-bug` to "Fixed bugs", `A-documentation` to "Documentation", and anything else to "Closed issues" if it is an issue or "Merged pull requests" if it is a pull request. An unlabelled item lands in one of those two, so this is the one operation whose output wants an editorial pass. `.github/release.yml` maps the same three labels for the GitHub release notes, and a test checks the two agree, because it previously named six labels pika does not have and every pull request fell through its catch-all.

Keep the file to a single `# ` heading. It is included verbatim into `docs/changelog.md`, so a second top-level heading becomes a second H1 on that page.

Land it before you tag, in the same pull request as the version bump or an earlier one, so the commit you tag carries the changelog for the version it names. `release check` fails if the version about to ship has no entry.

## Before the release: upgrade notes

Guidance a reader needs *before* installing goes in two places, and they are a pair rather than two independent documents:

- `HISTORY.md`, under an `## Upgrading to X.Y.Z` section above the version entries, which is what the documentation site's Changelog page renders.
- `.github/release-notes-preamble.md`, which `release.yaml` prepends to the generated GitHub release notes with `gh release create --notes`. `--notes` prepends rather than replacing, so the per-pull-request list still follows.

Keep the two in sync, or delete the preamble file when a release needs no guidance: the workflow treats an absent or empty file as "generated notes only" and says which it did in a notice annotation.

## Releasing

**A release is a pushed tag.** The workflow never writes to `main`; the version bump is an ordinary pull request, and pushing the tag afterwards is what publishes. Nothing is dispatched.

### The steps

`.ci/release.py` is the only entry point, one operation per step. Shorthand for the rest of this section:

```bash
alias release='hatch run docs:python .ci/release.py'
```

```bash
# 1. Branch, write the version into both files, commit `pika 1.5.0b1`.
release bump --bump none --prerelease-tag b1      # or --version 1.5.0b1

# 2. Generate the changelog entry and commit it, on that same branch, so it
#    lands in the same pull request as the bump.
release changelog --version 1.5.0b1 --dry-run     # prints the entry
release changelog --version 1.5.0b1

# 3. Push the branch and open the pull request.
release pr

#    Review and merge it. `tests-passed` has to pass, which is the point: the
#    commit that gets tagged is one CI has already seen.

# 4. Tag the merged commit and push the tag. This publishes.
git checkout main && git pull --ff-only
release check                 # everything a release needs, changes nothing
release tag --push
```

**`bump` comes before `changelog`.** `changelog` edits `HISTORY.md` and commits it, which it cannot do on `main`, and `bump` is what creates the release branch. Run them the other way round and `changelog` refuses outright, naming `bump`; it does not modify anything first.

**`check` says nothing useful before step 1.** On `main` it reads the version that is already published, so it reports that the version does not move forwards and that PyPI holds it, both correctly. It becomes a readiness check only once `bump` has written the new version, which is why step 4 runs it after the bump has merged. Running `release bump` locally and then `check` is the way to rehearse the whole sequence: `bump` only creates a branch, so `git checkout main && git branch -D pika-X.Y.Z` undoes it.

Step 4 is the only irreversible one. Everything before it is a normal pull request that can be amended or abandoned.

Every operation that changes something takes `--dry-run`, which prints the git, `gh` and file changes it would make and makes none of them: `bump`, `changelog`, `pr` and `tag`. The read-only operations have no such flag, which is `compute`, `classify`, `verify` and `check`.

`--` forwards the rest of the arguments to the tool an operation drives, so `release pr -- --draft --reviewer michaelklishin` adds flags to `gh pr create` without this script growing one for each. **Under `hatch run` you need two**, because `hatch` consumes the first: `release pr -- -- --draft`. Passthrough is placed *before* the flags the operation owns, since both `gh` and the changelog generator take the last occurrence of a repeated flag, so a passthrough `--base` cannot silently retarget the pull request.

`pr` labels the pull request `A-packaging`, assigns @lukebakken and sets the milestone to the version's base, so `1.5.0a1` goes to milestone `1.5.0`. `--label`, `--assignee` and `--milestone` replace those rather than adding to them, and `--milestone ''` opts out when the milestone does not exist yet.

### The operations

| Operation | Does |
|---|---|
| `compute` | print the version a bump would produce, and nothing else |
| `classify` | print what a pushed tag implies. The one operation a machine runs: `release.yaml` appends it to `$GITHUB_OUTPUT` |
| `bump` | create `pika-X.Y.Z`, write both version files, commit `pika X.Y.Z` |
| `changelog` | generate the `HISTORY.md` entry and commit it, on the release branch |
| `pr` | push the branch and open its pull request |
| `tag` | create the signed tag, after the local checks, and optionally push it |
| `check` | report on release readiness, changing nothing |

`bump` and `pr` are separate because `bump` is local and reversible while `pr` is outward-facing, the same split already drawn between creating a tag and pushing it. Nothing chains into the next step, and merging is not automated at all: releasing from a tag is worth doing *because* a human approved the tagged commit, and a script that merged its own pull request would hand that back.

The branch name `pika-X.Y.Z` and the commit subject `pika X.Y.Z` are not new. Both have been the convention since 1.3.0, so `bump` reuses them rather than introducing a third spelling.

### What `tag` checks, and the tag it makes

`tag` runs the same checks the workflow will, before the tag exists, so a wrong version fails on your machine rather than part-way through a publish. It refuses a tag that is not on `main`, a dirty tree, a `HEAD` that does not match `origin/main`, a version the two files disagree with, a tag that already exists locally or on `origin`, a version PyPI already holds, and a version that does not sort above the newest released one.

It also runs everything `check` runs, bar the PyPI probe, so a release cannot be tagged with no changelog entry or a stale preamble. Those checks used to live only in `check`, which meant `check` could exit 1 and `tag --push` succeed on the next line. `--skip-readiness` overrides that when you know better.

Then it creates the tag the way the published tags are made:

```bash
git tag --annotate --sign --local-user=$(git config user.signingkey) \
  --message="pika 1.5.0b1" 1.5.0b1
```

The key comes from `user.signingkey`; `tag` refuses rather than quietly making an unsigned tag, and afterwards confirms the tag is annotated and its signature verifies. The workflow checks the same two properties on the pushed tag, because a lightweight or unsigned tag created by hand would otherwise pass every guard, and the tag is the only record of what the published bytes were built from.

Creating and pushing are separate on purpose. Without `--push` it stops after creating the tag locally, leaving a moment to run `git show 1.5.0b1` or `git tag --verify 1.5.0b1`; `git push origin 1.5.0b1` then publishes, whether you let the script do it or do it yourself.

### What `check` is for

It answers "is this ready" in one command, and `tag` runs the same set:

- Both version files declare the version, not just one
- The version is one this scheme can publish, and **sorts above the newest released version**. Equality with the current version is not enough: on `main` at 1.4.0 the arithmetic happily produces `1.4.0b1`, which sorts below the published 1.4.4 and would publish a pre-release of a version that already shipped
- `HISTORY.md` has an entry for the version
- `HISTORY.md` and `.github/release-notes-preamble.md` still agree on the upgrade notes. They are a pair, and they drifted twice by hand while being written. Agreement means the preamble *starts with* what `HISTORY.md` says, compared from the `## Upgrading` heading so the introductory paragraph counts; the preamble may add release-notes-only material at the end, and does, since it closes by pointing at the changelog. An absent or empty preamble agrees vacuously, because the workflow treats both as "generated notes only"
- PyPI does not already hold the version

### What makes the tag trigger fire

Three conditions, and all three fail silently: no run starts, and nothing reports an error.

1. **A person pushes the tag, not a workflow.** Events triggered by `GITHUB_TOKEN` do not create workflow runs, with only `workflow_dispatch` and `repository_dispatch` exempt. This is GitHub's recursion guard, and it is the reason #1614 could not let a tag trigger the docs deploy: its commit message says "tag push cannot trigger it", which was true of the tag `release.yaml` itself pushed with `GITHUB_TOKEN` and is not true of one pushed from a terminal. That commit kept the tag trigger "for manually pushed tags", which is precisely the case this design uses. If the release is ever automated again behind a PAT or a GitHub App token, that token's pushes *do* create runs, and the guard stops applying.
2. **One tag per push.** GitHub creates no events at all when more than three tags are pushed at once, so `release.py tag --push` pushes a single tag by name. Do not reach for `git push --tags`, and note the old flow's `git push --follow-tags` would have been exposed to this too.
3. **The tagged commit contains the workflow, with the tag trigger.** A `push` event reads the workflow file from the pushed ref rather than from the default branch, which is also why this works at all for a ref that is not a branch. Tagging a commit from before this change landed would do nothing.

Nothing in the repository blocks the push itself: there are no tag protection rules and no rulesets. Worth knowing that **no workflow run in pika's history has ever been triggered by a tag push**, so the first real tag is the first time this fires here. It is documented GitHub behaviour, not a local convention, but there is no local precedent to point at.

### Why a tag and not a dispatch

The workflow cannot write to `main`, so it does not try: the bump is a pull request and the tag is pushed afterwards. `docs/internal/release.md` records why every credential route was closed, and what tagging buys besides. See #1728.

### Picking the version

`release.py compute` is a local helper rather than something the workflow runs for a release. It applies `bump` to the version in `pyproject.toml` with any pre-release suffix stripped first. Every value except `none` increments, so once `main` carries `1.5.0a1`, `minor` would give `1.6.0`. `none` holds the stripped base version, which is what both promoting and progressing a pre-release need.

It owns the input rules, so they are stated once: a pre-release needs a canonical `a`/`b`/`rc` segment with no leading zero, a pre-release tag is rejected in any other mode, and a computation that does not move the version is refused.

A full 1.5.0 cycle, from `main` at 1.4.x. Each line is the version to write into the two files and then tag:

```bash
# 1. alpha: bump the base version and attach the suffix
--current 1.4.0   --bump minor --mode prerelease --prerelease-tag a1   # -> 1.5.0a1

# 2. beta, then rc: hold the base version, change the suffix
--current 1.5.0a1 --bump none  --mode prerelease --prerelease-tag b1   # -> 1.5.0b1
--current 1.5.0b1 --bump none  --mode prerelease --prerelease-tag rc1  # -> 1.5.0rc1

# 3. the release: hold the base version, drop the suffix
--current 1.5.0rc1 --bump none --mode release                          # -> 1.5.0

# 4. the next cycle bumps normally again
--current 1.5.0   --bump patch --mode release                          # -> 1.5.1
```

Only step 1 uses a bump that moves the version; everything inside the cycle uses `none`. Using `minor` at step 2 or 3 would publish 1.6.0 instead.

The helper and the workflow are independent here: nothing forces the tag to be the version the helper suggests. What the workflow does enforce is that the tag matches the files, so a wrong version is caught as a disagreement rather than published.

**Build it first if you want certainty.** `-f mode=dry-run` builds without publishing anything and prints what it would use. Unlike the old flow, a dry run can no longer rehearse the release path, because that path is now a tag push.

### Modes

`mode` is no longer an input for a real release. A tag push decides it, and the two dispatch modes exist only to rehearse the build:

| `mode`       | Triggered by          | Version          | Publishes to | GitHub Release |
|--------------|-----------------------|------------------|--------------|----------------|
| `release`    | pushing `X.Y.Z`       | the tag          | PyPI         | yes            |
| `prerelease` | pushing `X.Y.Z<seg>`  | the tag          | PyPI         | yes (marked)   |
| `test`       | dispatch              | `X.Y.Z.dev<run>` | TestPyPI     | no             |
| `dry-run`    | dispatch              | `X.Y.Z`          | nothing      | no             |

`release` and `prerelease` are absent from the dispatch form, so there is no way to publish to PyPI without pushing a tag.

A pre-release goes to real PyPI: per PEP 440, `pip install pika` skips it unless the user passes `--pre` or pins the exact version. A test release uses a `.devN` version keyed on the workflow run number, so it is unique per run and never collides with TestPyPI's immutable-version rule. `.devN` is also refused as a tag, so a test version can never reach PyPI by being tagged.

### What happens

One `release.yaml` workflow, five jobs in order:

1. `release` verifies the tag, works out the mode, and builds the distribution once
2. `publish-pypi` or `publish-test-pypi` downloads that artifact and publishes it, so the bytes tested are the bytes shipped
3. `smoke-test` installs the published wheel and runs `.ci/smoke_test.py` against a live broker
4. `github-release` creates the Release on the tag, with the preamble prepended to generated notes
5. `deploy-docs` publishes the site, last, so a release that failed earlier publishes no documentation

A `dry-run` dispatch stops after the build.

### Guards on the tag

A tag carries no review, so these are what stand between a mistyped `git push origin <tag>` and a published release. `release.py tag` runs the same checks locally, before the tag exists, which is the cheaper place to fail.

Both sides share one implementation. `release.py tag` calls `tag_release.check` and the workflow calls `release.py verify`, which calls `tag_release.verify_tag`; the two differ only in what they can know, since one runs before the tag exists and the other after. The workflow used to carry its own shell copies instead, and they had drifted: nothing in them asked whether the version moved forwards, even though guard 4 below has always said it did. A hand-pushed `1.4.3` would have passed every check and published `main` under a version lower than `1.4.4`.

1. **The tag is one this scheme publishes.** `X.Y.Z` with an optional canonical `a`/`b`/`rc` segment. `v1.5.0`, `1.5`, `1.5.0.dev3`, `1.5.0.post1`, `1.5.0+local` and `1.05.0` are all refused, the last because `packaging` would normalise it to a version the tag does not spell. The `on.push.tags` glob is *broader* than that rule, matching anything starting with a digit or `v`, so a refused tag produces a named failing step rather than no workflow run at all. A narrow glob made those tags a silent no-op while this document called them refused
2. **The tag matches both version files.** The artifact is built from the files at that commit, not from the tag text, so a disagreement would publish one version under another's name. This catches the realistic mistake: tagging before the bump commit landed, or tagging the wrong commit
3. **The tagged commit is an ancestor of `origin/main`.** `main` is the only branch whose content passed `tests-passed`
4. **PyPI does not already hold the version, and the version sorts above the newest released one.** A tag can be deleted and re-pushed; a PyPI version cannot be re-uploaded even after deletion. Only a 404 from PyPI counts as "not published": any other status fails the step, because treating, say, a 503 as a green light would wave an already-published version through to `twine`
5. **The tag is annotated and signed.** Checked on the pushed tag, since a tag created by hand can be neither

Failing any of them aborts before anything is published, because they all run in the `release` job ahead of the publish jobs.

### When a release run fails

Where it failed decides what to do, and the dividing line is whether PyPI accepted the upload.

**Before `publish-pypi` succeeded**, nothing is published and the tag is the only artifact. Delete it and start over:

```bash
git push origin :refs/tags/1.5.0b1   # delete on origin
git tag -d 1.5.0b1                   # and locally
```

Fix the cause, then tag again. Re-pushing the same tag is fine here precisely because no version was published under it.

**After `publish-pypi` succeeded**, the version is permanent: PyPI does not allow re-uploading a version even after deletion. Do not delete the tag, because it is now the only record of what those bytes were built from. Finish the remaining steps by hand instead. The GitHub Release is `gh release create`, and the documentation is a `deploy-docs.yaml` dispatch as described below. If the wheel itself is broken, the only route is a new version.

This is the ordering the workflow is built around: every guard, and the build, run ahead of the first publish, so the common failures all land in the recoverable half.

### Label categories

Both the `HISTORY.md` entry and the GitHub release notes group by label, from the same three:

| Label | Section |
|---|---|
| `C-enhancement` | Implemented enhancements |
| `C-bug` | Fixed bugs |
| `A-documentation` | Documentation |
| everything else | `HISTORY.md`: Closed issues, or Merged pull requests. Release notes: Other changes |

`.ci/changelog.py` applies this to the changelog and `.github/release.yml` to the release notes, and a test asserts the two agree on the *mapping*, not merely on the set of labels. `C-refactor`, `C-performance`, `dependencies` and `github_actions` fall to the catch-all.

### Setup: publishing credentials

The publish jobs authenticate with API tokens stored as repository secrets:

- `PYPI_API_TOKEN` - an upload token for the pika project on pypi.org
- `TEST_PYPI_API_TOKEN` - an upload token for the pika project on test.pypi.org

Create each token under the account's settings on the respective index (scoped to the pika project), then add it under repo Settings → Secrets and variables → Actions.

The publish jobs also reference the `pypi` and `testpypi` GitHub environments (repo Settings → Environments). They can be empty; add required reviewers there if you want to gate publishing behind an approval. That is worth considering now that a tag push publishes: a tag carries no review of its own, so a required reviewer on the `pypi` environment is the only thing that would put a human between the push and the upload.

### Setup: signing

Release tags are signed. `release.py tag` reads `user.signingkey` from git configuration and refuses to tag without a key, rather than quietly producing an unsigned tag. Verify one afterwards with `git tag --verify X.Y.Z`.

### Setup: branch protection

Nothing to do, which is the point. The workflow never writes to `main`, so no rule has to be relaxed and no identity needs a bypass. `main` keeps its pull-request requirement and its push restriction, and the release path holds no credential beyond the two PyPI tokens.

## Documentation site

The site is published to the `gh-pages` branch with [`mike`](https://github.com/jimporter/mike), which keeps every version in its own subdirectory. `latest` is an alias, and the `index.html` at the site root redirects to whichever version holds it.

The work lives in the reusable `_deploy-docs.yaml`, which nothing triggers directly. Three callers decide what to publish:

| caller | when | publishes |
|--------|------|-----------|
| `main.yaml`, job `deploy-dev-docs` | push to `main`, after `tests-passed` | `dev` |
| `release.yaml`, job `deploy-docs` | end of a release or pre-release | `MAJOR.MINOR` for a release, the full version for a pre-release |
| `deploy-docs.yaml` | manual dispatch only | whatever you ask for |

The `dev` publish lives in `main.yaml` rather than on its own `push` trigger so that it can depend on `tests-passed`; an independent workflow would race the test matrix and publish documentation from a commit whose tests then failed. `main.yaml` also runs `validate-docs-deploy` on every pull request, which performs a full deploy with `push: false`. That rehearsal asks for a release version and an alias on purpose: with `dev` and no alias, the alias decision, `set-default` and `.ci/docs_site.py` all short-circuit, and the job would prove nothing about the path a release takes. `mike` is unpinned, so this is what catches a `mike` release that changes alias handling.

`release.yaml` calls the deploy with `uses:` rather than dispatching it with `gh workflow run`, so the deploy's conclusion is the release's conclusion. A dispatch returns as soon as the API accepts it, which would report a release as successful whether or not its documentation ever published. It runs last, after the PyPI publish, the smoke test and the GitHub Release, so a release that fails partway through never publishes docs for a version nobody can install. That does make the docs wait on `smoke-test`, the least deterministic job in the release: a broker flake there leaves the release published with its documentation unpublished, recoverable by hand as described below.

The version name and alias are inputs rather than something the deploy infers from a tag, because `release.yaml` knows its own mode and tag text does not distinguish a pre-release from a stable release reliably.

### Guards

Three things are checked because getting them wrong is silent rather than loud.

**Only a stable release may hold `latest`.** It backs the site root and every `latest/` URL compiled into a released wheel, so `.ci/docs_site.py` decides eligibility by parsing the version with `packaging`, not by matching a pattern. A pre-release, a post-release, a non-canonical spelling such as `1.05`, and a typo such as `1.5.0rcl` are all refused. `dev` is the one exception, and only until a release exists.

**An alias is never moved backwards.** Before moving `latest`, the deploy compares the version it is publishing against the version currently holding the alias and declines the move if the holder is newer. Without this, a deploy on an older tag would take `latest` and the site-root redirect with it, rolling the whole site back for every reader. A stable release can still reclaim the alias from its own release candidate, which is a forward move.

**The deploy is verified against the remote.** `mike` places its push inside the block that downgrades an empty commit to a warning, so a deploy producing no change skips the push and still exits 0. Checking that a version is merely *listed* is not enough either: a patch release publishes into the same `MAJOR.MINOR` directory, so a 1.5.1 whose push was lost would still find `1.5` carrying `latest` from 1.5.0. The workflow therefore records the `gh-pages` tip before deploying and requires it to have moved, then reads `versions.json` back and checks the site-root redirect points at the alias it was asked to set.

### Configuration

`mkdocs.yml` carries four settings that exist only because the site is versioned: `strict: true`, an explicit `mike` plugin declaration, `canonical_version: latest` and `extra.version.default: latest`. Each is commented where it is set, and `docs/internal/release.md` records what breaks without it.

### Permissions

`_deploy-docs.yaml` declares no `permissions` at all, and each caller grants what it needs: the three that push declare `contents: write`, and `validate-docs-deploy` inherits `contents: read`. `docs/internal/release.md` records why the called workflow must not declare them.

### Recovering a skipped or failed deploy

Dispatch `deploy-docs.yaml` with the parameters the automated job would have passed. They differ by mode, and using the release parameters for a pre-release would move `latest` onto a release candidate. Rather than working them out by hand, ask for the same answer the workflow got:

```bash
hatch run docs:python .ci/release.py classify --tag 1.6.0rc1
# mode=prerelease
# version=1.6.0rc1
# docs_version=1.6.0rc1
# docs_aliases=
# docs_set_default=false
```

```bash
# after a failed stable release
gh workflow run deploy-docs.yaml -f ref=1.6.0 -f version=1.6 -f aliases=latest -f set-default=true

# after a failed pre-release: no alias, no site-root change. Both are explicit
# because an omitted input takes the form's default.
gh workflow run deploy-docs.yaml -f ref=1.6.0rc1 -f version=1.6.0rc1 -f aliases= -f set-default=false
```

### Site maintenance

Standing the site up, the ReadTheDocs redirects, rebuilding `gh-pages` after damage and pruning pre-release versions are in `docs/internal/docs-site.md`. None of them is part of cutting a release.

### Links into the site

Because every page lives under a version directory there is no unversioned `/modules/...` path, so a link that must land on a specific page needs a version in it. The adapter deprecation warnings use `latest/` for that reason. Links that only need the docs home point at the bare site root and let its redirect follow the alias, which does not go stale if the alias scheme changes: that is why the README badge, the README documentation link and the `information` client property pika sends to every broker all use the root.

`README.md` is snippet-included into `docs/index.md`, so its absolute `latest/contributing/` link appears on every version's home page and always resolves to the current guide. That is deliberate: the README is also rendered on GitHub and on the PyPI project page, where a repository-relative link would be broken, and contributing instructions should reflect current practice rather than the release a reader happens to be viewing.

## Post-release verification

The `smoke-test` job in `release.yaml` runs automatically after `publish-pypi` for every release: it starts RabbitMQ, waits for the just-released version to become installable from PyPI, installs it into a clean virtualenv, and runs `.ci/smoke_test.py` (connect, declare, publish, get). A failing smoke test fails the release run, so a broken wheel is caught loudly rather than shipped silently.

To reproduce the smoke test locally:

```bash
docker run --pull always --detach --rm --publish 5672:5672 --publish 15672:15672 rabbitmq:4-management-alpine
python -m venv venv && source ./venv/bin/activate
pip install pika==<version>
python .ci/smoke_test.py
```
