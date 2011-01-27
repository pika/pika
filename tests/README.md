# Pika Functional Tests

Tests are written for nose:

  http://somethingaboutorange.com/mrl/projects/nose/1.0.0/

Tests are expecting a RabbitMQ instance local to the machine where you are
running the tests from.

They are broken down into functional tests and unit tests. Note that if there
are platform specific tests, the test runner will only run tests for your
current platform and ignore other platforms.

In the tests directory, simply run:

     python run_tests.py
