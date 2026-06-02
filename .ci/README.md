# CI Infrastructure

This directory contains the scripts and configuration that start RabbitMQ for
pika's acceptance tests on GitHub Actions.

## How the test matrix works

`.github/workflows/main.yaml` triggers on push and PR. It calls the reusable
workflow `.github/workflows/_test.yaml` twice:

- `test-modern` - Python 3.10-3.14, ubuntu-latest, macos-latest
- `test-legacy` - Python 3.7-3.9, ubuntu-22.04, macos-15-intel

Each invocation matrices over `python-version` and `test-tls: [true, false]`,
producing job names like `build/test on ubuntu-latest py3.12 tls=true`.

The `--use-tls` pytest flag is passed when `test-tls` is true, directing tests
to connect on port 5671 instead of 5672.

## Directory layout

```
.ci/
  README.md              this file
  rabbitmq.conf.in       shared RabbitMQ config template (macOS, Windows)
  linux/
    gha-setup.sh         starts RabbitMQ via Docker
    rabbitmq.conf        static config for the Docker container
  macos/
    gha-setup.sh         installs Erlang + RabbitMQ natively via brew/tarball
  windows/
    gha-setup.ps1        installs Erlang + RabbitMQ natively via exe installers
```

## RabbitMQ configuration

### Shared template: `rabbitmq.conf.in`

Used by macOS and Windows. Contains the placeholder `PIKA_DIR` which each
platform's setup script replaces with the absolute path to the repository
checkout (e.g. `sed "s|PIKA_DIR|$pika_dir|g"`).

If you add a new setting here, the macOS and Windows test legs pick it up
automatically. Linux uses its own static config (see below).

### Linux: `linux/rabbitmq.conf`

A static, ready-to-use config file - no sed substitution. It differs from the
shared template in three ways:

| Setting | Template value | Linux value | Why |
|---------|---------------|-------------|-----|
| `ssl_options.*` paths | `PIKA_DIR/tests/certs/...` | `/etc/rabbitmq/certs/...` | Certs are bind-mounted at this fixed path in the container |
| `loopback_users.guest` | `true` | `false` | Traffic from the runner arrives via the Docker bridge IP, which RabbitMQ does not classify as loopback |
| `log.console` | `false` | `true` | `docker logs rabbitmq` is the only log surface in CI |

If you add a new setting to `rabbitmq.conf.in`, you must also add it to
`linux/rabbitmq.conf`. A missing setting will cause test failures on Linux,
making it easy to catch in CI.

## Platform-specific setup scripts

### Linux (`linux/gha-setup.sh`)

Runs RabbitMQ in a Docker container rather than using GitHub Actions `services:`
because the service container starts before checkout (so it cannot mount repo
certs) and does not allow custom config.

Key details:
- Mounts only `tests/certs/` at `/etc/rabbitmq/certs:ro` (not the full repo,
  which triggers intermittent `.erlang.cookie: eacces` crashes)
- Uses `--hostname pika-rabbitmq` for a stable Erlang node name
- Runs all `docker exec` CLI commands as `--user rabbitmq` (running as root can
  corrupt cookie file ownership)
- Polls `rabbitmqctl await_startup` for up to 60 seconds
- On failure: dumps `docker ps`, `docker logs`, `docker inspect`, and
  `/var/lib/rabbitmq` listing for diagnostics

### macOS (`macos/gha-setup.sh`)

Installs RabbitMQ natively:
1. `brew install erlang@27`
2. Downloads the generic-unix tarball from GitHub Releases
3. Writes `rabbitmq.conf` from the shared template via sed
4. Starts the server detached, waits for epmd + `rabbitmqctl await_startup`

The `RABBITMQ_CONFIG_FILE` environment variable must point to the config path
without the `.conf` extension (an Erlang/RabbitMQ convention).

### Windows (`windows/gha-setup.ps1`)

Installs RabbitMQ natively via the `.exe` installer:
1. Resolves latest Erlang 27 and RabbitMQ versions from GitHub
2. Caches installers in `~/installers` (keyed by version)
3. Writes `rabbitmq.conf` from the shared template (replaces `PIKA_DIR` and
   converts backslashes to forward slashes for Erlang path compatibility)
4. Creates matching `.erlang.cookie` files for user and system profiles
5. Waits for epmd to report the rabbit node, then `rabbitmqctl wait`

## Common pitfalls

- **Adding settings to the template**: remember to update `linux/rabbitmq.conf`
  too. The Linux leg will fail if the setting is missing, which is the desired
  failure mode (loud, not silent).
- **Cert paths**: the template uses `PIKA_DIR/tests/certs/...` as a placeholder.
  macOS and Windows substitute the real repo path. Linux uses
  `/etc/rabbitmq/certs/...` because certs are bind-mounted there.
- **Docker bridge networking**: the runner is not "localhost" from the
  container's perspective. The `guest` user needs `loopback_users.guest = false`
  to authenticate over the bridge.
- **Cookie file ownership**: never run `docker exec` as root against the
  rabbitmq container. The rabbit process runs as the `rabbitmq` user; root CLI
  calls can change cookie ownership and crash the server on next read.
