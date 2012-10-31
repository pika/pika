Frequently Asked Questions
--------------------------

- Is Pika thread safe?

    Pika does not have any notion of threading in the code. If you want to use Pika with threading, make sure you have a Pika connection per thread, created in that thread. It is not safe to share one Pika connection across threads.

- How do I report a bug with Pika?

    The `main Pika repository <https://github.com/pika/pika>`_ is hosted on `Github <https://github.com>`_ and we use the Issue tracker at https://github.com/pika/pika/issues.

- Is there a mailing list for Pika?

    Yes, Pika's mailing list is available `on Google Groups <https://groups.google.com/forum/?fromgroups#!forum/pika-python>`_ and the email address is pika-python@googlegroups.com, though traditionally questions about Pika have been asked on the `RabbitMQ-Discuss mailing list <http://lists.rabbitmq.com/cgi-bin/mailman/listinfo/rabbitmq-discuss>`_.

- Is there an IRC channel for Pika?

    People knowledgeable about Pika tend to hang out in #pika and #RabbitMQ on `irc.freenode.net <http://freenode.net/>`_.

- What versions of Python are supported?

    Main developent is currently on the Python 2.6 branch. For a release, Pika passes its tests on the latest versions of 2.5, 2.6 and 2.7.

- Does Pika work with Python 3?

    Not yet.

- How can I contribute to Pika?

    You can `fork the project on Github <http://help.github.com/forking/>`_ and issue `Pull Requests <http://help.github.com/pull-requests/>`_ when you believe you have something solid to be added to the main repository.
