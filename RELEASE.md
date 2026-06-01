# Release process

## Automated release

Trigger the release workflow from GitHub Actions UI or CLI:

```bash
# Patch release (1.4.0 -> 1.4.1)
gh workflow run release.yaml -f bump=patch

# Minor release (1.4.0 -> 1.5.0)
gh workflow run release.yaml -f bump=minor

# Major release (1.4.0 -> 2.0.0)
gh workflow run release.yaml -f bump=major

# Pre-release to TestPyPI (1.4.0 -> 1.5.0b1)
gh workflow run release.yaml -f bump=minor -f prerelease=true -f prerelease_tag=b1

# Dry run (compute and bump the version, but do not commit, tag, or publish)
gh workflow run release.yaml -f bump=patch -f dry_run=true
```

### What happens

The entire flow lives in a single `release.yaml` workflow:

1. The `release` job bumps the version in `pyproject.toml` and `pika/__init__.py`, then commits, tags, and pushes
2. The `publish-pypi` / `publish-test-pypi` job builds the distribution and publishes via PyPI trusted publishing (OIDC, no tokens). Regular versions go to PyPI; pre-releases (`prerelease=true`) go to TestPyPI
3. The `github-release` job creates a GitHub Release with notes auto-generated from merged PRs (`--generate-notes`, grouped per `.github/release.yml`)

A `dry_run=true` dispatch stops after computing the version and prints the diff — nothing is committed, tagged, or published.

> The publish jobs deliberately live in this top-level workflow rather than a reusable one: PyPI trusted publishing matches the OIDC `job_workflow_ref` claim against the configured workflow filename and rejects tokens minted inside a reusable workflow.

### PR label categories

Release notes are grouped by PR labels (configured in `.github/release.yml`):

| Label                    | Section                  |
|--------------------------|--------------------------|
| `enhancement`, `feature` | Implemented enhancements |
| `bug`, `fix`             | Fixed bugs               |
| `documentation`, `docs`  | Documentation            |
| everything else          | Other changes            |

### Setup: trusted publishing

PyPI trusted publishing must be configured for this to work. On PyPI:

1. Go to the pika project settings on pypi.org
2. Add a new "trusted publisher" for GitHub Actions
3. Set repository to `pika/pika`, workflow to `release.yaml`, environment to `pypi`
4. Repeat on test.pypi.org with environment `testpypi`

Also create the `pypi` and `testpypi` GitHub environments (repo Settings →
Environments) so the publish jobs can request the OIDC token.

### Setup: branch protection

The `release` job pushes the version-bump commit and tag directly to `main`
using `GITHUB_TOKEN`. If `main` is protected, allow `github-actions[bot]` to
bypass the relevant rules (or the push step will fail).

## Post-release verification

After the release is published:

1. Check PyPI: `https://pypi.org/project/pika/<version>/`
2. Test the release:

    ```bash
    docker run --pull always --detach --rm --publish 5672:5672 --publish 15672:15672 rabbitmq:4-management-alpine
    cd examples
    python -m venv venv && source ./venv/bin/activate
    pip install pika==<version>
    python ./asynchronous_publisher_example.py
    ```
