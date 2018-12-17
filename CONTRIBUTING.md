# Contributing

## Test Coverage

To contribute to Pika, please make sure that any new features or changes
to existing functionality **include test coverage**.

*Pull requests that add or change code without coverage have a much lower chance
of being accepted.*


## Prerequisites

Pika test suite has a couple of requirements:

 * Dependencies from `test-dependencies.txt` are installed
 * A RabbitMQ node with all defaults is running on `localhost:5672`


## Installing Dependencies

To install the dependencies needed to run Pika tests, use

    pip install -r test-requirements.txt

which on Python 3 might look like this

    pip3 install -r test-requirements.txt


## Running Tests

To run all test suites, use

    nosetests

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
    PIKA_TEST_TLS=true nosetests
    ```


## Code Formatting

Please format your code using [yapf](http://pypi.python.org/pypi/yapf)
with ``google`` style prior to issuing your pull request. *Note: only format those
lines that you have changed in your pull request. If you format an entire file and
change code outside of the scope of your PR, it will likely be rejected.*
