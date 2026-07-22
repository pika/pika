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
