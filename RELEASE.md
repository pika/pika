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
```

### What happens

1. `release.yaml` bumps version in `pyproject.toml` and `pika/__init__.py`
2. `CHANGELOG.md` is auto-updated with changes since the last release (via `requarks/changelog-action`)
3. Version bump and changelog are committed, tagged, and pushed
4. Tag push triggers `publish.yaml` which runs CI and publishes to PyPI (via trusted publishing)
5. A GitHub Release is created with auto-generated notes from merged PRs
6. Pre-release tags (containing `a`, `alpha`, `b`, `beta`, `rc`, or `dev`) publish to TestPyPI instead

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
3. Set repository to `pika/pika`, workflow to `publish.yaml`, environment to `pypi`
4. Repeat on test.pypi.org with environment `testpypi`

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
