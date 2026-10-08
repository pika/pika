# Release process

## Before the release: update `HISTORY.md`

`HISTORY.md` is the changelog, and the release workflow does not touch it. `gh release create --generate-notes` writes the GitHub release body rather than this file, so without this step the documentation site's Changelog page stops at the previous release while PyPI carries the new one.

```bash
release changelog --version 1.5.0
```

That builds the entry from the version's **milestone**, in `.ci/changelog.py`: the closed issues and merged pull requests it carries, plus a sweep of `git log` for merged pull requests that escaped it. It needs no token beyond the `gh` authentication the rest of the release already uses.

It replaced `github_changelog_generator`, which fits pika badly. That tool crawled every tag and every closed issue by date, and pika has tags back to `v0.9a` and a 1,380-line changelog. It also wrote the heading link from the *previous* tag, which is why the committed 1.4.3 entry points at `tree/1.4.2`. Above all it ignored milestones, and pika curates those: 1.5.0 carries 38 closed issues and 84 merged pull requests. See #1731.

Only the new entry is generated, inserted above the newest existing one. It will not regenerate the whole file, because everything from `## Version History` down is hand-written history for 1.3.0 and earlier; it refuses rather than reaching past that heading.

`--dry-run` prints the entry instead of writing it, which is the way to read it before it lands. Without it, the entry is written and committed on the current branch, which must not be `main`.

`--since-tag` defaults to the newest tag **reachable from HEAD**, which `git describe` answers. The range is "what is new on this branch", and reachability is exactly that question. That is the opposite of how "has this version already shipped" is decided, which is **PEP 440 order**, because 1.4.1 through 1.4.4 were cut from `1.4.x` and are not ancestors of `main`. Reasoning from one rule to the other gives either a changelog that re-lists three released versions or a release that goes backwards; both rules are commented where they live.

**Unmilestoned pull requests are included, and reported.** There are 14 in `1.4.0..main`, mostly dependabot, and the list is printed so the milestone can be corrected afterwards. One of them, #1694, is a substantive fix that never got milestoned.

**Read the result before committing.** Grouping follows labels: `C-enhancement` to "Implemented enhancements", `C-bug` to "Fixed bugs", `A-documentation` to "Documentation", everything else to "Closed issues". An unlabelled issue lands in the catch-all, so this is the one operation whose output wants an editorial pass. `.github/release.yml` maps the same three labels for the GitHub release notes, and a test checks the two agree, because it previously named six labels pika does not have and every pull request fell through its catch-all.

Keep the file to a single `# ` heading. It is included verbatim into `docs/changelog.md`, so a second top-level heading becomes a second H1 on that page.

Land it before you tag, in the same pull request as the version bump or an earlier one, so the commit you tag carries the changelog for the version it names. `release check` fails if the version about to ship has no entry.

## Before the release: upgrade notes

Guidance a reader needs *before* installing goes in two places, and they are a pair rather than two independent documents:

- `HISTORY.md`, under an `## Upgrading to X.Y.Z` section above the version entries, which is what the documentation site's Changelog page renders.
- `.github/release-notes-preamble.md`, which `release.yaml` prepends to the generated GitHub release notes with `gh release create --notes`. `--notes` prepends rather than replacing, so the per-pull-request list still follows.

Keep the two in sync, or delete the preamble file when a release needs no guidance: the workflow treats an absent or empty file as "generated notes only" and says which it did in a notice annotation.

The preamble is committed rather than typed into the GitHub UI at release time for two reasons. It is reviewed alongside the code it describes, and a draft release created by hand does not become the release: `release.yaml` runs `gh release create` for the tag itself, so a hand-made draft would linger untagged beside the real one.

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

Step 4 is the only irreversible one. Everything before it is a normal pull request that can be amended or abandoned.

Every operation that changes something takes `--dry-run`, which prints the git, `gh` and file changes it would make and makes none of them. `compute`, `classify` and `check` have no such flag because they only read.

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

The workflow used to compute the bump itself, commit it, and push the commit and tag to `main` with `GITHUB_TOKEN`. That cannot work here, and had never been tried: every dispatch to date was `test` or `dry-run`.

`main` requires a pull request with one approving review *and* restricts pushes to the Maintainers team. `enforce_admins` is off, so the Maintainers bypass both, which is why a human can push `main` directly; `github-actions[bot]` is neither a Maintainer nor an allowed app. Classic branch protection offers no way to grant `GITHUB_TOKEN` a bypass, because adding an app to the push allowlist does not exempt it from the review requirement.

The credential routes are all closed too. A fine-grained PAT for an org-owned repository needs the org to opt in, which is an org-owner setting. Installing a GitHub App wants an owner as well, and would additionally require migrating `main` from classic protection to a ruleset before an app could be a bypass actor. A classic PAT would work mechanically but is whole-account in scope.

Tagging sidesteps all of it, needs no credential, and gains two things worth having on their own: the released commit is one that passed `tests-passed`, where the bot's commit was created after the build and pushed untested, and the release path holds one fewer secret rather than one more. See #1728.

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

The entire flow lives in a single `release.yaml` workflow:

1. The `release` job works out what to do. On a tag push it asks `release.py classify` for the mode and the documentation parameters, then runs the four guards below. On a dispatch it computes a throwaway version and edits the two version files, which a real release never does because the files already carry the version. Either way it builds the distribution once (`twine check` included) and uploads it as an artifact
2. The `publish-pypi` / `publish-test-pypi` job downloads that artifact and publishes it, authenticating with the `PYPI_API_TOKEN` / `TEST_PYPI_API_TOKEN` repository secrets. Downstream jobs never rebuild, so the bytes tested are the bytes shipped
3. The `smoke-test` job installs the just-published wheel from the matching index and runs `.ci/smoke_test.py` against a live broker (see Post-release verification)
4. The `github-release` job creates a GitHub Release on the pushed tag, with notes auto-generated from merged PRs (`--generate-notes`, grouped per `.github/release.yml`) and the upgrade preamble prepended
5. The `deploy-docs` job runs last and calls the reusable deploy on the tag, so the documentation site is published only after the wheel is on PyPI, the smoke test has passed, and the Release exists

A `dry-run` dispatch builds the artifact and stops. Nothing is published.

> Building in the `release` job and publishing from separate jobs is the pattern the `gh-action-pypi-publish` maintainers recommend: the build runs without access to the publishing secret, which is exposed only to the `publish-pypi` / `publish-test-pypi` jobs.

### Guards on the tag

A tag carries no review, so these are what stand between a mistyped `git push origin <tag>` and a published release. `release.py tag` runs the same checks locally, before the tag exists, which is the cheaper place to fail.

1. **The tag is one this scheme publishes.** `X.Y.Z` with an optional canonical `a`/`b`/`rc` segment. `v1.5.0`, `1.5`, `1.5.0.dev3`, `1.5.0.post1`, `1.5.0+local` and `1.05.0` are all refused, the last because `packaging` would normalise it to a version the tag does not spell. The `on.push.tags` glob is deliberately *broader* than that rule, matching anything starting with a digit or `v`, so a refused tag produces a named failing step rather than no workflow run at all. A narrow glob made those tags a silent no-op while this document called them refused
2. **The tag matches both version files.** The artifact is built from the files at that commit, not from the tag text, so a disagreement would publish one version under another's name. This catches the realistic mistake: tagging before the bump commit landed, or tagging the wrong commit
3. **The tagged commit is an ancestor of `origin/main`.** `main` is the only branch whose content passed `tests-passed`
4. **PyPI does not already hold the version**, and the version sorts above the newest released one. A tag can be deleted and re-pushed; a PyPI version cannot be re-uploaded even after deletion. Only a 404 from PyPI counts as "not published": any other status fails the step, because treating, say, a 503 as a green light would wave an already-published version through to `twine`
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

### PR label categories

Release notes are grouped by PR labels (configured in `.github/release.yml`):

| Label                    | Section                  |
|--------------------------|--------------------------|
| `enhancement`, `feature` | Implemented enhancements |
| `bug`, `fix`             | Fixed bugs               |
| `documentation`, `docs`  | Documentation            |
| everything else          | Other changes            |

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

This section used to say to let `github-actions[bot]` bypass the relevant rules. That is not possible under classic branch protection, which is what #1728 was about: adding an app to the push allowlist does not exempt it from the review requirement, and `GITHUB_TOKEN` cannot be given a bypass at all.

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

`mkdocs.yml` carries four settings that exist only because the site is versioned:

- `strict: true` makes `mike`'s own `mkdocs build` strict. Without it a warning that fails the pull-request docs job would pass during a deploy. `hatch run docs:serve` passes `--no-strict` so authoring a page before wiring up `nav` still starts a server.
- The `mike` plugin is declared explicitly. `mike` injects it when deploying, but only with defaults, and declaring it is what makes `canonical_version` reachable.
- `canonical_version: latest` stops every published version from declaring itself canonical, which would let an unreleased `dev` page outrank the released one. See #1712 for the effect this has on per-version sitemaps.
- `extra.version.default: latest` names the versions that are current, so they do not show the outdated-version banner. That banner comes from `overrides/main.html`: mkdocs-material renders it only from an `outdated` block that is empty in the stock theme, so without the override a reader landing on an old version from a search result gets no signal, and the version-comparison JavaScript is never loaded. Its link is relative rather than pointing at `latest` directly, so a page published years from now still resolves through the site root instead of hard-coding today's alias.

### Permissions

A called workflow can only maintain or reduce the caller's token, never elevate it, and this applies to the *demand* as well as the grant: `_deploy-docs.yaml` therefore declares no `permissions` at all. Declaring `contents: write` there made GitHub reject any caller that granted less, which showed up as a startup failure that ran no jobs and reported no checks rather than as a red job.

Each caller grants what it needs instead. The three that push declare `contents: write`; `validate-docs-deploy` declares nothing and inherits `contents: read`, which is correct because it pushes nothing and keeps a write-capable token away from a job that runs `mike` and `hatch` over configuration a pull request supplied.

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

### Links into the site

Because every page lives under a version directory there is no unversioned `/modules/...` path, so a link that must land on a specific page needs a version in it. The adapter deprecation warnings use `latest/` for that reason. Links that only need the docs home point at the bare site root and let its redirect follow the alias, which does not go stale if the alias scheme changes: that is why the README badge, the README documentation link and the `information` client property pika sends to every broker all use the root.

`README.md` is snippet-included into `docs/index.md`, so its absolute `latest/contributing/` link appears on every version's home page and always resolves to the current guide. That is deliberate: the README is also rendered on GitHub and on the PyPI project page, where a repository-relative link would be broken, and contributing instructions should reflect current practice rather than the release a reader happens to be viewing.

### Setup: bootstrap

The `latest` alias and the `index.html` at the site root that redirects to it would otherwise exist only once a stable release had been deployed, leaving `https://pika.github.io/pika/` and every `latest/` URL returning 404 in the meantime, including the ones compiled into the adapter deprecation warnings of any wheel released before then.

The `main` deploy handles this itself: it asks for `latest` on every push, so the first deploy takes the alias and writes the root redirect, and after a release has taken `latest` the deploy's alias guard declines to move it back. No manual command is needed, and re-running it is safe.

What does need doing by hand, once, is the repository configuration:

1. Confirm no branch-protection rule on `gh-pages` blocks the `github-actions[bot]` push. If the first deploy fails at the push, this is why.
2. Set Settings -> Pages -> Source to `Deploy from a branch`, branch `gh-pages`, folder `/ (root)`. The branch has to exist first, so do this after the first deploy.

### ReadTheDocs redirects

The Sphinx docs on `pika.readthedocs.io` are retired, and years of indexed links and third-party references point at them. RTD redirects forward those to the MkDocs site. This is a one-time setup, recorded here because the next person to touch it will not be the one who did it.

`utils/rtd_redirects.json` holds the mapping and `utils/push_rtd_redirects.py` pushes it. There is no maintained Python client for the RTD API - the `readthedocs` and `readthedocs-client` names on PyPI are reservations with no uploaded files - so the script talks to API v3 over stdlib `urllib`.

```bash
# No token needed: prints every request it would send.
python3 utils/push_rtd_redirects.py

# What RTD holds now, and the diff against the mapping.
python3 utils/push_rtd_redirects.py --list
python3 utils/push_rtd_redirects.py            # with a token: shows the plan

# Send it, then confirm the old URLs land on the new site.
python3 utils/push_rtd_redirects.py --apply
python3 utils/push_rtd_redirects.py --verify
```

The token comes from an RTD account's API tokens page. The script reads `~/.config/rtd-token` or `RTD_TOKEN`; prefer the file, so the value stays out of shell history and process listings. Nothing is committed and nothing authenticated happens without `--apply`.

Five things about the mapping are deliberate and easy to get wrong:

- **The rules are `page` redirects and `from_url` carries no version prefix.** RTD applies a page redirect across every version, so `/intro.html` covers `/en/stable/intro.html`, `/en/latest/intro.html` and `/en/1.3.2/intro.html` in one rule. Writing `/en/stable/intro.html` instead would need an `exact` rule per version and would miss the older versions search engines still hold.
- **`to_url` carries `latest/`.** The new site is versioned by `mike` and has no unversioned page paths: `/pika/intro/` is a 404 and `/pika/latest/intro/` is not. The site root is the one exception, because `/pika/` serves a redirect that follows the `latest` alias and so survives an alias change.
- **`force: true`**, so a rule fires even where the old page still builds, and **`http_status: 301`**, so search engines move their index rather than treating it as temporary.
- **Verify against the deployed site, never `mkdocs serve`.** A local server has no version directories, so every one of these paths resolves locally whether or not it is right in production.
- **Directory roots need rules of their own.** Sphinx writes `index.html` but serves `/en/stable/` and `/en/stable/modules/` as directory indexes, and a rule on `/index.html` does not match `/`. Three such paths existed, and they were still serving the retired docs at 200 after every explicit `.html` path already redirected. That is the shape that hides this: the links a mapping is derived from all work, while the URL a person actually types does not, and `/en/stable/` is where the bare `readthedocs.io` domain sends its visitors.

Re-running is safe: matching rules are left alone, differing ones are updated in place, and rules on RTD that the mapping does not describe are reported but never deleted, so anything set by hand in the dashboard survives.

`--verify` probes a wildcard rule with a deliberately unmapped path rather than with its own `from_url`. Substituting `/en/*` builds `https://pika.readthedocs.io/en/stable/en/*`, which the catch-all then answers, so the check would report success having tested nothing.

Two things confirmed against the live API in October 2026, so they need not be re-derived:

- A `page` redirect accepts an absolute off-site `to_url`. That is the whole basis for one version-agnostic rule per page rather than an `exact` rule per version, and RTD's published documentation does not say so either way. Proven by creating a single rule, confirming the 301, and checking it also covered `/en/latest/` and `/en/1.3.2/` without per-version rules.
- RTD normalises `from_url` by stripping a trailing slash: `/modules/` is stored as `/modules`, while `/` is kept. The script compares normalised paths for that reason. Without it an existing rule reads as missing and `--apply` creates a duplicate on every run, which a dry run shows as "2 to create, 2 on RTD not in the mapping".
- The valid `type` values are `page`, `exact`, `clean_url_to_html` and `html_to_clean_url`, established by posting each candidate and keeping the ones that returned 201. The API does not list the choices when it rejects one, but its rejection messages are informative: `sphinx_html` and `sphinx_htmldir` name their replacements, and `prefix` says "Prefix redirects have been removed. Please use an exact redirect `/prefix/*` instead", which is where the wildcard syntax comes from.
- **The catch-all is the last rule and is an `exact` redirect on `/en/*`.** Its target is fixed rather than a `:splat` passthrough, because the two sites do not share a URL shape: the old paths end in `.html` and the new ones are directories, so passing the tail through would 404 on every page. The API accepts `:splat`, `$rest` and a fixed target alike, so acceptance proves nothing here and only behaviour does.
- **RTD appends a new rule to the end of the list, and lower positions win.** Posting the catch-all with `position: 100` stored it as `position: 21`, after the 21 specific rules, which is what makes `faq.html` still reach `latest/faq/` while an unmapped path reaches the docs home. Verified both ways round.

### Rebuilding `gh-pages` from scratch

`gh-pages` holds built output only, so it can be deleted and regenerated. The history is not worth preserving, but the layout is: `mike` records versions in a `versions.json` that only the deploys themselves write, so a rebuild has to replay each version rather than restore a snapshot.

**Delete the local branch as well as the remote one.** `mike` syncs from the remote only when the local branch is behind it or has diverged; when the local branch is merely *ahead*, which is what a freshly deleted remote makes it, `mike` uses the local branch as-is and republishes the entire pre-delete version index without a warning. Deleting only the remote therefore looks like it worked and undoes the rebuild:

```bash
git push origin --delete gh-pages
git branch -D gh-pages          # the step that is easy to miss
```

Then, for every version to republish, check out the **newest** tag in that series and deploy under the name the original deploy used:

```bash
git checkout <newest tag in the series, e.g. 1.5.3>
hatch run docs:mike deploy --push 1.5    # MAJOR.MINOR, not the full tag
```

The newest tag matters because a stable release publishes `MAJOR.MINOR` and each patch overwrites that same directory. Rebuilding `1.5` from tag `1.5.0` when the series ended at `1.5.3` silently reverts the published `1.5` docs by three patch releases, and nothing records which patch a directory holds.

Then redeploy `dev` from `main` and re-establish the alias and the root redirect, which no `deploy` recreates:

```bash
git checkout main
hatch run docs:mike deploy --push dev
hatch run docs:mike alias --push --update-aliases <newest MAJOR.MINOR> latest
hatch run docs:mike set-default --push latest
```

Deploy order does not matter: `mike` sorts the version selector itself, newest first, with non-version names such as `dev` at the top.

Only versions whose tag carries an `mkdocs.yml` can be replayed, which today means none of them: every released tag up to and including the 1.4 series predates the MkDocs migration. Until 1.5.0 ships there is nothing to replay, and a rebuild reduces to redeploying `dev` from `main` and pointing `latest` at it.

### Pruning pre-release versions

A pre-release publishes a full copy of the site under its own version, and no alias points at it. Nothing removes those, so the version selector accumulates every `b1` and `rc1` indefinitely. Delete them once the stable release they led to has shipped:

```bash
hatch run docs:mike list                        # see what is published
hatch run docs:mike delete --push 1.5.0rc1
```

## Post-release verification

The `smoke-test` job in `release.yaml` runs automatically after `publish-pypi` for every release: it starts RabbitMQ, waits for the just-released version to become installable from PyPI, installs it into a clean virtualenv, and runs `.ci/smoke_test.py` (connect, declare, publish, get). A failing smoke test fails the release run, so a broken wheel is caught loudly rather than shipped silently.

To reproduce the smoke test locally:

```bash
docker run --pull always --detach --rm --publish 5672:5672 --publish 15672:15672 rabbitmq:4-management-alpine
python -m venv venv && source ./venv/bin/activate
pip install pika==<version>
python .ci/smoke_test.py
```
