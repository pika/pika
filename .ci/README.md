# CI Infrastructure

This directory holds two unrelated things: the scripts and configuration that start RabbitMQ for pika's acceptance tests on GitHub Actions, which is what the rest of this file is about, and the Python helpers that own decisions too consequential to express in a workflow.

## Python helpers

| File | Owns |
|---|---|
| `release.py` | the release command line, one operation per step. See `RELEASE.md` |
| `release_version.py` | version arithmetic, and what a pushed tag implies |
| `changelog.py` | the `HISTORY.md` entry, from the milestone plus `git log` |
| `tag_release.py` | the preconditions for tagging, before and after the push, and the tag command itself |
| `docs_site.py` | the documentation alias policy and deploy verification |
| `smoke_test.py` | connect, declare, publish and get against a live broker |
| `run_examples.py` | which examples serve until interrupted, and whether each example still runs cleanly against a live broker |

All but `smoke_test.py` have unit tests in `tests/unit/`, which is the deal for anything that decides something irreversible: a version becomes a git tag, a PyPI release and a documentation directory that nobody can take back. `smoke_test.py` is exempt because it has no decisions to test; it runs against a live broker and either connects or does not.

`fmt`, `lint`, `docfmt` and `typecheck` all cover this directory as a directory rather than naming files, so a new helper is gated the moment it lands. `release.py` is the one to reach for by hand. `release_version.py` and `tag_release.py` are libraries with no command line at all. `docs_site.py` is called by the docs-deploy workflow, and `smoke_test.py` is called by the release workflow but is also documented in `RELEASE.md` for reproducing a failed smoke test locally against your own broker. `run_examples.py` is called by the `examples` job in `main.yaml`, and locally by `hatch run examples:check`.

## How the test matrix works

`.github/workflows/main.yaml` triggers on push and PR. It calls the reusable workflow `.github/workflows/_test.yaml` twice. Each call matrices over `python-version` and `test-tls: [true, false]` on both the Linux and Windows runners:

- `test-modern` - Python 3.10-3.15, ubuntu-latest + windows-latest
- `test-legacy` - Python 3.7-3.9, ubuntu-22.04 + windows-latest

macOS runs separately as the standalone `test-macos` job in `main.yaml` (not through the reusable workflow): only the latest supported CPython on macos-latest, both TLS modes. It is slow and occasionally flaky, so it is kept minimal and non-blocking, but still exercises the BSD-only KQueuePoller path and feeds its coverage to Codecov.

Job names look like `build/test on ubuntu-latest py3.12 tls=true`.

The `--use-tls` pytest flag is passed when `test-tls` is true, directing tests to connect on port 5671 instead of 5672.

## Directory layout

```
.ci/
  README.md              this file
  release.py             the release CLI
  release_version.py     version arithmetic and tag interpretation
  changelog.py           the HISTORY.md entry, from the milestone
  tag_release.py         tagging preconditions
  docs_site.py           docs-site alias policy
  smoke_test.py          post-publish broker check
  run_examples.py        runs every example against a live broker
  rabbitmq.conf.in       shared RabbitMQ config template (macOS, Windows)
  linux/
    gha-setup.sh         starts RabbitMQ via Docker
    rabbitmq.conf        static config for the Docker container
    examples.conf        what the examples job adds to it
  macos/
    gha-setup.sh         installs Erlang + RabbitMQ natively via brew/tarball
  windows/
    gha-setup.ps1        installs Erlang + RabbitMQ natively via exe installers
```

## RabbitMQ configuration

### Shared template: `rabbitmq.conf.in`

Used by macOS and Windows. Contains the placeholder `PIKA_DIR` which each platform's setup script replaces with the absolute path to the repository checkout (e.g. `sed "s|PIKA_DIR|$pika_dir|g"`).

If you add a new setting here, the macOS and Windows test legs pick it up automatically. Linux uses its own static config (see below).

### Linux: `linux/rabbitmq.conf`

A static, ready-to-use config file - no sed substitution. It differs from the shared template in three ways:

| Setting | Template value | Linux value | Why |
|---------|---------------|-------------|-----|
| `ssl_options.*` paths | `PIKA_DIR/tests/certs/...` | `/etc/rabbitmq/certs/...` | Certs are bind-mounted at this fixed path in the container |
| `loopback_users.guest` | `true` | `false` | Traffic from the runner arrives via the Docker bridge IP, which RabbitMQ does not classify as loopback |
| `log.console` | `false` | `true` | `docker logs rabbitmq` is the only log surface in CI |

If you add a new setting to `rabbitmq.conf.in`, you must also add it to `linux/rabbitmq.conf`. A missing setting will cause test failures on Linux, making it easy to catch in CI.

### Examples job: `linux/examples.conf`

Mounted into `/etc/rabbitmq/conf.d/`, which the broker reads after `rabbitmq.conf`, only when `PIKA_CI_EXAMPLES=true`, so the test legs are unchanged. It adds SASL `EXTERNAL` for `tls_mutual_authentication_twisted.py`. Listing any mechanism replaces the default list, so the file repeats the defaults; without them every other example is refused.

## Platform-specific setup scripts

### Linux (`linux/gha-setup.sh`)

Runs RabbitMQ in a Docker container rather than using GitHub Actions `services:` because the service container starts before checkout (so it cannot mount repo certs) and does not allow custom config.

Key details:
- Mounts only `tests/certs/` at `/etc/rabbitmq/certs:ro` (not the full repo, which triggers intermittent `.erlang.cookie: eacces` crashes)
- Uses `--hostname pika-rabbitmq` for a stable Erlang node name
- Runs all `docker exec` CLI commands as `--user rabbitmq` (running as root can corrupt cookie file ownership)
- Polls `rabbitmqctl await_startup` for up to 60 seconds
- With `PIKA_CI_EXAMPLES=true`, also publishes 15672, mounts `linux/examples.conf`, enables `rabbitmq_management` and `rabbitmq_auth_mechanism_ssl`, and creates the user the client certificate logs in as
- On success: dumps `rabbitmq-diagnostics listeners` and `status` to confirm both ports (5672, 5671) are bound
- On failure: dumps `docker ps`, `docker logs`, `docker inspect`, and `/var/lib/rabbitmq` listing for diagnostics

### macOS (`macos/gha-setup.sh`)

Installs RabbitMQ natively:
1. Resolves the latest RabbitMQ version from GitHub Releases (or uses `RABBITMQ_VERSION` env var if set)
2. `brew install erlang@27`
3. Downloads the generic-unix tarball from GitHub Releases
4. Writes `rabbitmq.conf` from the shared template via sed
5. Starts the server detached, waits for epmd + `rabbitmqctl await_startup`
6. Dumps `rabbitmq-diagnostics listeners` and `status` for TLS verification

The `RABBITMQ_CONFIG_FILE` environment variable takes the config path with or without the `.conf` extension. The broker uses the path as given when it names a file that exists, and otherwise appends `.conf` or `.config` and retries, so this script's extensionless form works and so would the other.

### Windows (`windows/gha-setup.ps1`)

Installs RabbitMQ natively via the `.exe` installer:
1. Resolves latest Erlang 27 and RabbitMQ versions from GitHub
2. Caches installers in `~/installers` (keyed by version)
3. Writes `rabbitmq.conf` from the shared template (replaces `PIKA_DIR` and converts backslashes to forward slashes for Erlang path compatibility)
4. Creates matching `.erlang.cookie` files for user and system profiles
5. Waits for epmd to report the rabbit node, then `rabbitmqctl wait`

## Common pitfalls

- **Adding settings to the template**: remember to update `linux/rabbitmq.conf` too. The Linux leg will fail if the setting is missing, which is the desired failure mode (loud, not silent).
- **Cert paths**: the template uses `PIKA_DIR/tests/certs/...` as a placeholder. macOS and Windows substitute the real repo path. Linux uses `/etc/rabbitmq/certs/...` because certs are bind-mounted there.
- **Docker bridge networking**: the runner is not "localhost" from the container's perspective. The `guest` user needs `loopback_users.guest = false` to authenticate over the bridge.
- **Cookie file ownership**: never run `docker exec` as root against the rabbitmq container. The rabbit process runs as the `rabbitmq` user; root CLI calls can change cookie ownership and crash the server on next read.
