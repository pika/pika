# Contributing

## Test Coverage

To contribute to Pika, please make sure that any new features or changes to
existing functionality **include test coverage**.

Pull requests that add or change code without adequate test coverage will be
rejected.

## Code Formatting

Please format your code using [yapf](http://pypi.python.org/pypi/yapf) with
`google` style prior to issuing your pull request.

!!! note

    Only format those lines that you have changed in your pull request. If you
    format an entire file and change code outside of the scope of your PR, it
    will likely be rejected.

To **see what formatting changes are required** by yapf, run the following command from the repository root:

    yapf --diff --style google --recursive --exclude 'pika/spec.py' pika/ tests/ examples/

To **automatically apply formatting changes** to your code, run:

    yapf -i --style google --recursive --exclude 'pika/spec.py' pika/ tests/ examples/

## Prerequisites

Pika test suite has a couple of requirements:

 * Dependencies from `test-requirements.txt` are installed
 * A RabbitMQ node with all defaults is running on `localhost:5672`


## Installing Dependencies

To install the dependencies needed to run Pika tests, use

    pip install -r test-requirements.txt

If your environment uses the `pip3` command name, run `pip3 install -r test-requirements.txt` instead.


## Running Tests

To run all test suites, use

    pytest

Note that some tests are OS-specific (e.g. epoll on Linux
or kqueue on MacOS and BSD). Those will be skipped
automatically.

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
    pytest --use-tls
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

`mkdocs serve` always serves **one** build. The site header version menu (Material
`extra.version.provider: mike`) only appears when several versions exist in a
**mike** layout (`versions.json` on the `gh-pages` branch), not in a plain
`site/` output.

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