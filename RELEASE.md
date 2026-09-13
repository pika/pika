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
5. The `deploy-docs` job runs last and dispatches `deploy-docs.yaml` on the new tag, so the documentation site is published only after the wheel is on PyPI, the smoke test has passed, and the Release exists

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

`deploy-docs.yaml` publishes the MkDocs site to the `gh-pages` branch with
[`mike`](https://github.com/jimporter/mike), which keeps every version in its
own subdirectory. A push to `main` publishes `dev`; a stable release tag
publishes `MAJOR.MINOR` and moves the `latest` alias to it.

The work lives in the reusable `_deploy-docs.yaml`. Two workflows call it:

- `deploy-docs.yaml` on a push to `main`, publishing `dev`, and on manual
  dispatch for anything else.
- `release.yaml`, as its terminal `deploy-docs` job, which runs after the PyPI
  publish, the smoke test, and the GitHub Release. A release that fails partway
  through therefore never publishes its docs.

`release.yaml` calls it with `uses:` rather than dispatching it with
`gh workflow run`, so the deploy's conclusion is the release's conclusion. A
dispatch returns as soon as the API accepts it, which would report the release
as successful whether or not the docs ever published.

The caller passes the version name and aliases explicitly. A stable release
publishes `MAJOR.MINOR` and takes `latest`; a pre-release publishes under its
full version and takes nothing. That decision is made in `release.yaml`, where
the mode is known, rather than inferred from the tag text.

Two guards worth knowing about:

- **An alias is never moved backwards.** Before moving `latest`, the deploy
  compares the version it is publishing against whichever version currently
  holds the alias, and declines the move if the current holder is newer. Without
  this, a deploy on an older tag would take `latest` and the site-root redirect
  with it, rolling the whole site back for every reader and every `latest/` URL
  compiled into a shipped wheel. `dev` is treated as superseded by any real
  version, since it holds the alias only as the pre-release bootstrap below.
- **The deploy is verified against the remote.** `mike` places its push inside
  the same block that downgrades an empty commit to a warning, so a deploy whose
  built output matches what is already published skips the push and still exits
  0. The workflow reads `versions.json` back from `origin/gh-pages` afterwards
  and fails if the version, or an alias it was supposed to move, is not there.

If an upstream job fails and the docs job is skipped, deploy by hand with the
same parameters that job would have passed:

```bash
gh workflow run deploy-docs.yaml -f ref=<tag> -f version=<MAJOR.MINOR> -f aliases=latest -f set-default=true
```

Because every page lives under a version directory, there is no unversioned
`/modules/...` path. A link that must land on a specific page therefore needs a
version in it, and the adapter deprecation warnings use `latest/` so the alias
resolves them to the newest stable release. Links that only need the docs home,
such as the README badge, point at the bare site root instead and let its
redirect follow `latest` on their behalf, which does not go stale if the alias
scheme ever changes.

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
