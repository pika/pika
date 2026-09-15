# Release process

## Automated release

Trigger the release workflow from GitHub Actions UI or CLI:

The workflow takes a `mode` (default `release`) and a `bump`:

```bash
# Patch release (1.4.0 -> 1.4.1)
gh workflow run release.yaml -f bump=patch

# Minor release (1.4.0 -> 1.5.0)
gh workflow run release.yaml -f bump=minor

# Major release (1.4.0 -> 2.0.0)
gh workflow run release.yaml -f bump=major

# Pre-release to PyPI (1.4.0 -> 1.5.0b1); pip skips it unless --pre or pinned
gh workflow run release.yaml -f mode=prerelease -f bump=minor -f prerelease_tag=b1

# Test release to TestPyPI (1.4.0 -> 1.5.0.devN); throwaway, repeatable
gh workflow run release.yaml -f mode=test -f bump=minor

# Dry run (compute the version and build, but publish nothing)
gh workflow run release.yaml -f mode=dry-run -f bump=patch
```

### Modes

| `mode`       | Version         | Commit + tag | Publishes to | GitHub Release |
|--------------|-----------------|--------------|--------------|----------------|
| `release`    | `X.Y.Z`         | yes          | PyPI         | yes            |
| `prerelease` | `X.Y.Z<tag>`    | yes          | PyPI         | yes (marked)   |
| `test`       | `X.Y.Z.dev<run>`| no           | TestPyPI     | no             |
| `dry-run`    | `X.Y.Z`         | no           | nothing      | no             |

A pre-release goes to real PyPI: per PEP 440, `pip install pika` skips it
unless the user passes `--pre` or pins the exact version. A test release
uses a `.devN` version keyed on the workflow run number, so it is unique per
run and never collides with TestPyPI's immutable-version rule.

### What happens

The entire flow lives in a single `release.yaml` workflow:

1. The `release` job bumps the version in `pyproject.toml` and `pika/__init__.py`, builds the distribution once (`twine check` included), and uploads it as an artifact. For `release`/`prerelease` it also commits, tags, and pushes. The build runs before any commit, so a broken build aborts the release before `main` is mutated
2. The `publish-pypi` / `publish-test-pypi` job downloads that artifact and publishes it, authenticating with the `PYPI_API_TOKEN` / `TEST_PYPI_API_TOKEN` repository secrets. Downstream jobs never rebuild, so the bytes tested are the bytes shipped
3. The `smoke-test` job installs the just-published wheel from the matching index and runs `.ci/smoke_test.py` against a live broker (see Post-release verification)
4. The `github-release` job creates a GitHub Release with notes auto-generated from merged PRs (`--generate-notes`, grouped per `.github/release.yml`)
5. The `deploy-docs` job runs last and calls the reusable deploy on the new tag, so the documentation site is published only after the wheel is on PyPI, the smoke test has passed, and the Release exists

A `dry-run` dispatch computes the version and builds the artifact, then stops - nothing is committed, tagged, or published.

> Building in the `release` job and publishing from separate jobs is the pattern the `gh-action-pypi-publish` maintainers recommend: the build runs without access to the publishing secret, which is exposed only to the `publish-pypi` / `publish-test-pypi` jobs.

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

Create each token under the account's settings on the respective index
(scoped to the pika project), then add it under repo Settings → Secrets and
variables → Actions.

The publish jobs also reference the `pypi` and `testpypi` GitHub environments
(repo Settings → Environments). They can be empty; add required reviewers
there if you want to gate publishing behind an approval.

### Setup: branch protection

The `release` job pushes the version-bump commit and tag directly to `main`
using `GITHUB_TOKEN`. If `main` is protected, allow `github-actions[bot]` to
bypass the relevant rules (or the push step will fail).

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

Dispatch `deploy-docs.yaml` with the parameters the automated job would have passed. They differ by mode, and using the release parameters for a pre-release would move `latest` onto a release candidate:

```bash
# after a failed stable release
gh workflow run deploy-docs.yaml -f ref=1.6.0 -f version=1.6 -f aliases=latest -f set-default=true

# after a failed pre-release: no alias, no site-root change
gh workflow run deploy-docs.yaml -f ref=1.6.0rc1 -f version=1.6.0rc1
```

### Links into the site

Because every page lives under a version directory there is no unversioned `/modules/...` path, so a link that must land on a specific page needs a version in it. The adapter deprecation warnings use `latest/` for that reason. Links that only need the docs home point at the bare site root and let its redirect follow the alias, which does not go stale if the alias scheme changes: that is why the README badge, the README documentation link and the `information` client property pika sends to every broker all use the root.

`README.md` is snippet-included into `docs/index.md`, so its absolute `latest/contributing/` link appears on every version's home page and always resolves to the current guide. That is deliberate: the README is also rendered on GitHub and on the PyPI project page, where a repository-relative link would be broken, and contributing instructions should reflect current practice rather than the release a reader happens to be viewing.

### Setup: bootstrap

The `latest` alias and the `index.html` at the site root that redirects to it
would otherwise exist only once a stable release had been deployed, leaving
`https://pika.github.io/pika/` and every `latest/` URL returning 404 in the
meantime, including the ones compiled into the adapter deprecation warnings of
any wheel released before then.

The `main` deploy handles this itself: it asks for `latest` on every push, so the
first deploy takes the alias and writes the root redirect, and after a release
has taken `latest` the deploy's alias guard declines to move it back. No manual
command is needed, and re-running it is safe.

What does need doing by hand, once, is the repository configuration:

1. Confirm no branch-protection rule on `gh-pages` blocks the
   `github-actions[bot]` push. If the first deploy fails at the push, this is
   why.
2. Set Settings -> Pages -> Source to `Deploy from a branch`, branch `gh-pages`,
   folder `/ (root)`. The branch has to exist first, so do this after the first
   deploy.

### Rebuilding `gh-pages` from scratch

`gh-pages` holds built output only, so it can be deleted and regenerated. The
history is not worth preserving, but the layout is: `mike` records versions in a
`versions.json` that only the deploys themselves write, so a rebuild has to
replay each version rather than restore a snapshot.

**Delete the local branch as well as the remote one.** `mike` syncs from the
remote only when the local branch is behind it or has diverged; when the local
branch is merely *ahead*, which is what a freshly deleted remote makes it, `mike`
uses the local branch as-is and republishes the entire pre-delete version index
without a warning. Deleting only the remote therefore looks like it worked and
undoes the rebuild:

```bash
git push origin --delete gh-pages
git branch -D gh-pages          # the step that is easy to miss
```

Then, for every version to republish, check out the **newest** tag in that
series and deploy under the name the original deploy used:

```bash
git checkout <newest tag in the series, e.g. 1.5.3>
hatch run docs:mike deploy --push 1.5    # MAJOR.MINOR, not the full tag
```

The newest tag matters because a stable release publishes `MAJOR.MINOR` and each
patch overwrites that same directory. Rebuilding `1.5` from tag `1.5.0` when the
series ended at `1.5.3` silently reverts the published `1.5` docs by three patch
releases, and nothing records which patch a directory holds.

Then redeploy `dev` from `main` and re-establish the alias and the root redirect,
which no `deploy` recreates:

```bash
git checkout main
hatch run docs:mike deploy --push dev
hatch run docs:mike alias --push --update-aliases <newest MAJOR.MINOR> latest
hatch run docs:mike set-default --push latest
```

Deploy order does not matter: `mike` sorts the version selector itself, newest
first, with non-version names such as `dev` at the top.

Only versions whose tag carries an `mkdocs.yml` can be replayed, which today
means none of them: every released tag up to and including the 1.4 series
predates the MkDocs migration. Until 1.5.0 ships there is nothing to replay, and
a rebuild reduces to redeploying `dev` from `main` and pointing `latest` at it.

### Pruning pre-release versions

A pre-release publishes a full copy of the site under its own version, and no
alias points at it. Nothing removes those, so the version selector accumulates
every `b1` and `rc1` indefinitely. Delete them once the stable release they led
to has shipped:

```bash
hatch run docs:mike list                        # see what is published
hatch run docs:mike delete --push 1.5.0rc1
```

## Post-release verification

The `smoke-test` job in `release.yaml` runs automatically after
`publish-pypi` for every release: it starts RabbitMQ, waits for
the just-released version to become installable from PyPI, installs it into
a clean virtualenv, and runs `.ci/smoke_test.py` (connect, declare, publish,
get). A failing smoke test fails the release run, so a broken wheel is caught
loudly rather than shipped silently.

To reproduce the smoke test locally:

```bash
docker run --pull always --detach --rm --publish 5672:5672 --publish 15672:15672 rabbitmq:4-management-alpine
python -m venv venv && source ./venv/bin/activate
pip install pika==<version>
python .ci/smoke_test.py
```
