Frequently Asked Questions
--------------------------

- Is Pika thread safe?

    Pika does not have any notion of threading in the code. If you want to use Pika with threading, make sure you have a Pika connection per thread to use. It is not safe to share one Pika connection across threads.

- Pika starts sending messages quickly but slows down over time, what gives?

    Most likely you are being throttled by the RabbitMQ broker, see :ref:`intro_to_backpressure`.

- I need to connect to a RabbitMQ version before 2.0 that only supports AMQP 0-8, what should I do?

    Use `version 0.5.2 <https://github.com/tonyg/pika/tree/v0.5.2>`_  which only supports 0-8. `[Download] <https://github.com/tonyg/pika/tarball/v0.5.2>`_

- How do I report a bug with Pika?

    The `main Pika repository <https://github.com/tonyg/pika>`_ is hosted on `Github <https://github.com>`_ and we use the Issue tracker at https://github.com/tonyg/pika/issues.

- Is there a mailing list for Pika?

    Traditionally questions about Pika have been asked on the `RabbitMQ-Discuss mailing list <http://lists.rabbitmq.com/cgi-bin/mailman/listinfo/rabbitmq-discuss>`_.

- Is there an IRC channel for Pika?

    People knowledgeable about Pika tend to hang out in #RabbitMQ on `irc.freenode.net <http://freenode.net/>`_.

- What versions of Python are supported?

    Main developent is currently on the Python 2.6 branch. For a release, Pika passes its tests on the latest versions of 2.4, 2.5 and 2.6.

- Does Python work with Python 3?

    Not yet, but there is an effort to port it underway.

- How can I contribute to Pika?

    You can `fork the project on Github <http://help.github.com/forking/>`_ and issue `Pull Requests <http://help.github.com/pull-requests/>`_ when you believe you have something solid to be added to the main repository.
