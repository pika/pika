# Contributing

## Test Coverage

To contribute to Pika, please make sure that any new features or changes to existing functionality **include test coverage**.

Pull requests that add or change code without adequate test coverage will be rejected.

## Prerequisites

To run the full test suite, a RabbitMQ node with all defaults must be running on `localhost:5672`. Use `hatch run rabbitmq` to start one via Docker, or provide your own.

## Installing Dependencies

Install [Hatch](https://hatch.pypa.io/), which manages the development environment and dependencies automatically:

    pipx install hatch

If you do not have ``pipx``, you can use ``pip install hatch`` instead. Hatch will install all required dependencies when you first run a script.


## Running Tests

To run all test suites, use

    hatch run test

To run unit tests only (no RabbitMQ required), use

    hatch run unit

To start RabbitMQ via Docker for acceptance tests, use

    hatch run rabbitmq

Note that some tests are OS-specific (e.g. epoll on Linux or kqueue on MacOS and BSD). Those will be skipped automatically.

If you would like to run TLS/SSL tests, use the following procedure:

* Create a `rabbitmq.conf` file:

    ```
    sed -e "s#PIKA_DIR#$PWD#g" ./testdata/rabbitmq.conf.in > ./testdata/rabbitmq.conf
    ```

* Start RabbitMQ and use the configuration file you just created. An example command
  that works with the `generic-unix` package is as follows:

    ```
    $ RABBITMQ_CONFIG_FILE=/path/to/pika/testdata/rabbitmq.conf ./sbin/rabbitmq-server
    ```

* Run the tests indicating that TLS/SSL connections should be used:

    ```
    hatch run test -- --use-tls
    ```

## Building Documentation

Pika documentation is built with MkDocs and Material for MkDocs.

To install the documentation dependencies, use

    pip install -r docs/requirements.txt

To build the documentation locally, use

    mkdocs build --strict

To preview the documentation, use

    mkdocs serve

Or with live reload:

    mkdocs serve --livereload

`mkdocs serve` always serves **one** build. The site header version menu (Material `extra.version.provider: mike`) only appears when several versions exist in a **mike** layout (`versions.json` on the `gh-pages` branch), not in a plain `site/` output.

To preview **multiple versions** locally:

1. Install deps (includes `mike`).

2. Deploy the current tree as one or more version labels on your **local**
   `gh-pages` branch (omit `--push` to stay offline):

        mike deploy 1.3.2
        mike deploy --update-aliases 1.4.0b0 dev latest
        mike set-default latest

3. Serve that branch:

        mike serve

4. Open the URL it prints (default `http://127.0.0.1:8000`) and use the version
   selector. `mike list` shows what is installed; `mike delete VERSION` removes
   one version.
   
## Code Formatting and Linting

Please format your code using [yapf](https://pypi.org/project/yapf/)
with ``google`` style prior to issuing your pull request. *Note: only format those
lines that you have changed in your pull request. If you format an entire file and
change code outside of the scope of your PR, it will likely be rejected.*

    hatch run fmt

To verify formatting without modifying files (mirrors CI), use

    hatch run fmt-check

Please also ensure your code passes [ruff](https://docs.astral.sh/ruff/) linting:

    hatch run lint

Both checks run in CI on every push and pull request.
