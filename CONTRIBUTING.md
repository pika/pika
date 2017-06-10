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


## Code Formatting

Please format your code using [yapf](http://pypi.python.org/pypi/yapf)
with ``google`` style prior to issuing your pull request.
