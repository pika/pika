Frequently Asked Questions
--------------------------

- Is Pika thread safe?

    Pika does not have any notion of threading in the code. If you want to use Pika with threading, make sure you have a Pika connection per thread, created in that thread. It is not safe to share one Pika connection across threads, with one exception: you may call the connection method `add_callback_threadsafe` from another thread to schedule a callback within an active pika connection.

- How do I report a bug with Pika?

    The `main Pika repository <https://github.com/pika/pika>`_ is hosted on `Github <https://github.com>`_ and we use the Issue tracker at `https://github.com/pika/pika/issues <https://github.com/pika/pika/issues>`_.

- Is there a mailing list for Pika?

    Yes, Pika's mailing list is available `on Google Groups <https://groups.google.com/forum/?fromgroups#!forum/pika-python>`_ and the email address is pika-python@googlegroups.com, though traditionally questions about Pika have been asked on the `RabbitMQ mailing list <https://groups.google.com/forum/#!forum/rabbitmq-users>`_.

- How can I contribute to Pika?

    You can `fork the project on Github <https://help.github.com/en/articles/fork-a-repo/>`_ and issue `Pull Requests <https://help.github.com/en/articles/about-pull-requests/>`_ when you believe you have something solid to be added to the main repository.
