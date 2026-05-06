# Contributing

## Test Coverage

To contribute to Pika, please make sure that any new features or changes
to existing functionality **include test coverage**.

*Pull requests that add or change code without coverage have a much lower chance
of being accepted.*


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
    hatch run test -- --use-tls
    ```


## Code Formatting and Linting

Please format your code using [yapf](https://pypi.python.org/pypi/yapf)
with ``google`` style prior to issuing your pull request. *Note: only format those
lines that you have changed in your pull request. If you format an entire file and
change code outside of the scope of your PR, it will likely be rejected.*

    hatch run fmt

To verify formatting without modifying files (mirrors CI), use

    hatch run fmt-check

Please also ensure your code passes [ruff](https://docs.astral.sh/ruff/) linting:

    hatch run lint

Both checks run in CI on every push and pull request.
