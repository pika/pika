# AGENTS.md

Guidelines for AI agents working on the pika codebase.

## Project overview

pika is a pure-Python implementation of the AMQP 0-9-1 protocol. It targets
Python 3 with `requires-python >= 3.7`. CI tests Python 3.10 through 3.14 on
the modern matrix and 3.7 through 3.9 on the legacy matrix, both defined in
`.github/workflows/main.yaml`.

## Repository layout

```
pika/                   # library source
pika/adapters/          # connection adapters (asyncio, blocking, gevent,
                        #   select, tornado, twisted)
pika/adapters/utils/    # adapter internals (io_services_utils, nbio_interface,
                        #   connection_workflow, selector_ioloop_adapter)
pika/spec.py            # auto-generated AMQP spec (see below)
tests/unit/             # unit tests
tests/acceptance/       # acceptance tests (require a running RabbitMQ)
tests/typing/           # type-checker fixtures (checked by hatch run typecheck)
utils/codegen.py        # code generator for pika/spec.py
utils/regen_spec.py     # regenerate/verify pika/spec.py (hatch run spec-regen)
.ci/release.py          # the release CLI: one operation per step (see RELEASE.md)
.ci/release_version.py  # library: version arithmetic; what a pushed tag means
.ci/tag_release.py      # library: preconditions for tagging, and the tag itself
.ci/docs_site.py        # docs-site deploy helper (alias policy, verification)
examples/               # usage examples
```

## Code style

- **Formatter:** [yapf](https://pypi.org/project/yapf/) with Google style. Run
  `hatch run fmt-check` to check, `hatch run fmt` to format in place.
- **Docstring formatter:**
  [docformatter](https://github.com/PyCQA/docformatter) enforces [PEP
  257](https://peps.python.org/pep-0257/) style. Configuration is in
  `pyproject.toml` under `[tool.docformatter]`. Run `hatch run docfmt-check`.
- **The style gates run on Python 3.10 in CI, and your hatch environment is
  newer.** `yapf.yaml`, `lint.yaml` and `mypy.yaml` all pin `3.10`, so a gate
  that passes locally can still fail there when the construct it formats is
  version-sensitive. This is not hypothetical: docformatter 1.7.8 rewrites a
  backslash line continuation differently on 3.10 than on 3.14, stripping the
  space before the backslash, which produced a clean `hatch run docfmt-check`
  and a red CI job for the same commit. **Avoid backslash continuations.**
  Parenthesised `with (a, b):` is the obvious alternative but needs Python 3.9,
  and `requires-python` is 3.7, so reach for `contextlib.ExitStack` instead;
  `tests/unit/tag_release_tests.py` does. A green local style gate is evidence,
  not proof.
- **The test matrix floor is Python 3.7, and it covers `.ci/` too.** The
  helpers there run on 3.14 in CI and on your machine, but their unit tests run
  on every version in the matrix, so a 3.8+ API in `.ci/` fails the legacy leg;
  `pathlib.Path.unlink(missing_ok=True)` is the one that caught us. Windows is
  in the matrix as well: do not compare a rendered path against a hardcoded
  `/`, and remember `write_text` translates newlines, so a test that means
  exact bytes should use `write_bytes`. The same lesson as the bullet above,
  one layer down: passing locally on Linux and 3.14 is two platforms short of
  what CI runs.
- **Linter:** [ruff](https://docs.astral.sh/ruff/). Configuration is in
  `pyproject.toml` under `[tool.ruff]`. `hatch run lint` applies fixes (`ruff
  check --fix`); `hatch run lint-check` only reports (this is what CI runs).
  Verify with `lint-check`, not `lint`, so you read the same result CI will.
- **Do not pin the ruff version.** Ruff is intentionally left unpinned so CI
  always installs the latest release. When ruff stabilizes a new rule, CI
  surfaces the violations immediately and we fix them, keeping the codebase
  current with ruff's evolving idioms. A CI lint failure after a ruff release
  is this mechanism working as intended, not a regression to suppress. Fix the
  flagged code; never add a version pin to make the failure go away.
- **Type checking:** [mypy](https://mypy-lang.org/). Configuration is in
  `mypy.ini`. Run `hatch run typecheck`, which covers `pika/`, the
  downstream-consumer fixtures in `tests/typing/`, and `.ci/`. The fixtures
  exist because `mypy.ini` sets `packages = pika`, so a run without them never
  observes code that consumes pika from the outside.
- **The gates name `.ci/` as a directory, not the files in it.** `fmt`, `lint`,
  `docfmt` and `typecheck` all cover `.ci/`, because a helper nothing gates
  rots and naming files individually leaves the next helper ungated until
  somebody notices. `.ci/release_version.py` was added with an unused import
  and bad spacing and all three gates passed, which is how this was found.
- Logic that decides something irreversible belongs in `.ci/` with unit tests,
  not in a workflow. `.ci/docs_site.py` owns the documentation alias policy and
  `.ci/release_version.py` owns the release version arithmetic and what a
  pushed tag means, and `.ci/tag_release.py` owns the preconditions for
  creating the tag, each with tests in `tests/unit/`. Those two are libraries
  with no command line of their own: `.ci/release.py` is the only command, so
  there is one place to look up what the release can do and one place that
  talks to git, `gh` and PyPI. Both were shell first and both were wrong: the
  alias comparison used `sort -V`, which ranks `1.5.0rc1` above `1.5`, and the
  version arithmetic incremented on every bump, so a published pre-release
  could not be promoted to its final version. A git tag, a PyPI release and a
  documentation directory cannot be taken back.
- Use single quotes for strings unless the string contains a single quote.
- No trailing whitespace. Check before committing.

## Auto-generated code

`pika/spec.py` is generated by `utils/codegen.py`. Never edit `spec.py`
directly. Changes to spec classes or constants require modifying the code
generator and regenerating the file. Pull requests that modify `spec.py`
without corresponding `utils/codegen.py` changes will be rejected.

Regenerate with `hatch run spec-regen`, which is the only supported recipe:
`utils/regen_spec.py` downloads `amqp_codegen.py` and
`amqp-rabbitmq-0.9.1.json` from `rabbitmq/rabbitmq-server`, runs
`utils/codegen.py` in a temp tree, and reformats the result with yapf (`fmt`
excludes `spec.py`, so this step is not optional). `hatch run spec-check` is
the same code path without writing: it diffs against the committed file and is
what the `codegen` workflow enforces on every pull request.

- **Do not pin the upstream ref.** `DEFAULT_REF` is `main` for the same reason
  ruff is left unpinned: an upstream AMQP spec change shows up as a CI failure
  on the next push rather than leaving `spec.py` silently behind. Use `--ref`
  to generate from another revision when investigating; do not turn that into a
  committed pin to make a failure go away.
- **When `spec-check` fails with a diff you did not cause,** upstream changed
  the spec. Regenerating and committing `spec.py` is the fix, and it belongs in
  its own commit rather than buried in an unrelated pull request.

## Git conventions

- **Commit message format:** 50-70 character subject line in active voice,
  present tense. Wrap the body at 72 characters. Use backticks around code
  identifiers in commit messages.
- **No em-dashes** in any git or GitHub text (commit messages, PR titles, PR
  bodies, review comments, issue bodies). Use regular dashes (`-` or `--`)
  instead.
- **PR and review bodies:** write to a temp file and use `--body-file`, never
  inline `--body` for multi-line content.
- **No backticks around commit SHAs** in GitHub markdown.
- **No H1 headers** (`#`) in PR or review comment bodies. Use H2 (`##`) or
  lower.
- **Never hard-wrap markdown.** Every paragraph and list item is a single line,
  however long. This applies to every `.md` file in the repository and to every
  forge body (PR bodies, issue bodies, comments, reviews). **`HISTORY.md` and
  this file are the only exemptions, and both stay wrapped.** `HISTORY.md` is
  mostly frozen release history, where rewrapping would churn a thousand lines
  and destroy blame on every old entry for no reader benefit. This file is
  wrapped because unwrapping it broke it: the two-space continuation indent
  under a bullet is what keeps a multi-paragraph list item inside its list, and
  a reflow that drops it silently splits one list into three. Nothing rendered
  complains, so the damage is invisible until someone reads the source.

  Two different reasons, so do not reason from one to the other. In a forge
  body newlines are preserved, so a wrapped paragraph renders ragged. In an
  `.md` file the renderer reflows and the wraps are invisible when rendered;
  the cost there is to everything that reads the file as text, because
  line-anchored citation, `grep -n` and per-line diff review all become
  unreliable when a sentence spans several lines and a one-word edit reflows
  the paragraph.

  Commit messages are the one exception to both: plain text, never rendered,
  and still wrapped at 72. Converting the files that are still wrapped is
  tracked in #1730.
- Use `git commit -F <file>` for commit messages longer than a single line.

## CI workflows

- **Format** (`.github/workflows/yapf.yaml`): runs `yapf` check on every push
  and pull request.
- **Lint** (`.github/workflows/lint.yaml`): runs `ruff check` on every push and
  pull request.
- **Type check** (`.github/workflows/mypy.yaml`): runs `mypy` on every push and
  pull request. Requires `tornado` and `twisted` to be installed so mypy can
  resolve optional-dependency types.
- **Codegen** (`.github/workflows/codegen.yaml`): runs `hatch run spec-check`
  on every push and pull request to verify `pika/spec.py` still matches the
  output of `utils/codegen.py`. Deliberately not path-filtered: a skipped
  path-filtered job never reports to a required status check, and running
  unconditionally also catches a hand-edited `spec.py`.
- **Tests** (`.github/workflows/main.yaml`): the entry point on every push and
  pull request. Calls the reusable test workflow three times: once for Python
  3.10-3.14, once for 3.7-3.9, and once for pre-release 3.15 in a non-blocking,
  Linux-only `test-preview` leg. macOS runs as a separate, non-blocking
  `test-macos` job (latest supported Python on macos-latest only), kept minimal
  because macOS is slow and flaky. Both non-blocking legs are absent from the
  `tests-passed` needs list. It also builds the docs and gates the blocking
  legs behind `tests-passed`. Acceptance tests require a RabbitMQ server
  (started via Docker in CI). Coverage is uploaded to Codecov. Two docs jobs
  hang off it: `validate-docs-deploy` exercises the `mike` publish path on pull
  requests without pushing, and `deploy-dev-docs` publishes the `dev` docs
  after `tests-passed`.
- **Reusable tests** (`.github/workflows/_test.yaml`): the matrix itself, Linux
  and Windows crossed with each Python version and with TLS on and off (macOS
  runs separately; see the `test-macos` job in `main.yaml`). Invoked via
  `workflow_call`; never triggered directly.
- **Docs** (`.github/workflows/docs.yaml`): runs `hatch run docs:build`.
  Invoked via `workflow_call` from the test workflow.
- **Deploy docs** (`.github/workflows/deploy-docs.yaml`): manual
  (`workflow_dispatch`) entry point for publishing the documentation site,
  taking the version and aliases as inputs. The automated paths do not go
  through it: `main.yaml` publishes `dev` after `tests-passed`, and
  `release.yaml` publishes the release version as its last job.
- **Reusable docs deploy** (`.github/workflows/_deploy-docs.yaml`): builds the
  site and publishes it to `gh-pages` with `mike`, one directory per version.
  Invoked via `workflow_call`; never triggered directly. It validates its
  inputs, refuses to move an alias such as `latest` backwards onto an older
  version, and reads `gh-pages` back afterwards because `mike` skips its push
  when a deploy produces no change and still exits 0. Helper logic lives in
  `.ci/docs_site.py`. See `RELEASE.md` for the site layout and recovery
  procedures.
- **CodeQL** (`.github/workflows/codeql-analysis.yml`): security analysis on
  push, pull request, and a weekly schedule.
- **Release** (`.github/workflows/release.yaml`): a real release is triggered
  by **pushing a tag** (`X.Y.Z`, optionally with a canonical `a`/`b`/`rc`
  segment). The workflow never writes to `main`, because it cannot: `main`
  requires a pull request and restricts pushes to the Maintainers team, and
  classic branch protection cannot grant `GITHUB_TOKEN` a bypass. So the
  version bump lands as an ordinary pull request and the tag is pushed
  afterwards, which also means the tagged commit is one `tests-passed` has
  already seen. The `workflow_dispatch` entry point survives for `test` and
  `dry-run` only; it cannot publish to PyPI. Drive the whole thing with
  `.ci/release.py` (`bump`, `changelog`, `pr`, `check`, `tag`), and see
  `RELEASE.md`.

## Running tests locally

Install [Hatch](https://hatch.pypa.io/) if you do not have it:

```bash
pipx install hatch
```

Run tests with Hatch (it creates the environment and installs dependencies
automatically):

```bash
# Unit tests only (no RabbitMQ needed)
hatch run unit

# All tests (requires RabbitMQ running on localhost)
hatch run test

# Start RabbitMQ via Docker
hatch run rabbitmq
```

Tests run in parallel by default via `pytest-xdist` (`-n auto
--dist=loadscope`). `loadscope` is required so classes that generate test
methods dynamically (e.g. `tests/unit/io_services_test_stubs_test.py`) stay on
a single worker; their `tearDownClass` asserts every generated method ran. Pass
`-n 0` to disable parallelism.

## PR conventions

- Base branch is `main`.
- Branch naming: `gh-<issue-number>` for issue-linked work, descriptive slug
  for other work.
- Assign the PR to relevant maintainers.
- Set the milestone when one applies.
- Add labels from the existing set when appropriate. They are namespaced by
  prefix: `C-` category (`C-bug`, `C-enhancement`, `C-refactor`), `A-` area
  (`A-testing`, `A-documentation`, `A-typing`, per-adapter labels), `E-`
  effort, `P-` priority, `S-` status. Run `gh label list` for the full set.
- PR descriptions should include a summary of changes, what was tested, and
  reference any related issues with `Fixes #NNN` or `See #NNN`.

## Key technical details

- `pika/_utils.py` contains internal platform and socket utilities used across
  adapters and connection internals.
- The interrupt socket pair in `pika/adapters/select_connection.py` uses
  `_TRY_IO_AGAIN_SOCK_ERROR_CODES = (errno.EAGAIN, errno.EWOULDBLOCK)` to
  handle platform differences between POSIX and Windows. Both errno values must
  always be checked. The same constant is defined separately in
  `pika/adapters/utils/io_services_utils.py`; keep the two in sync.
- `pika/adapters/twisted_connection.py` and
  `pika/adapters/tornado_connection.py` depend on optional third-party
  libraries. Type annotations in these files use `type: ignore` comments that
  are only valid when the libraries are installed.
