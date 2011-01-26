# Pika Functional Tests

Tests are written for nose:

  http://somethingaboutorange.com/mrl/projects/nose/1.0.0/

Tests are expecting a RabbitMQ instance local to the machine where you are
running the tests from.

In the tests directory, simply run:

    nosetests

## Notes:

adapter_tests.py in this directory does not include epoll, poll, or kqueue

 * To test epoll and poll on linux run nosetests in the linux dir.
 * To test kqueue on bsd or mac osx run nosetests in the bsd_and_osx dir.
