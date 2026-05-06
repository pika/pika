## Frequently Asked Questions

### Is Pika thread safe?

Pika does not have any notion of threading in the code. If you want to use Pika with threading, make sure you have a Pika connection per thread, created in that thread.

It is not safe to share one Pika connection across threads, with one exception: you may call the connection method `add_callback_threadsafe` from another thread to schedule a callback within an active Pika connection. See [connection adapters](modules/adapters/index.md) for more details.

### How do I report a bug with Pika?

The [main Pika repository](https://github.com/pika/pika) is hosted on [GitHub](https://github.com), and we use the issue tracker at [github.com/pika/pika/issues](https://github.com/pika/pika/issues).

### Is there a mailing list for Pika?

Yes. Pika's mailing list is available on [Google Groups](https://groups.google.com/g/pika-python), and the email address is `pika-python@googlegroups.com`.

Traditionally, questions about Pika have also been asked on the [RabbitMQ mailing list](https://groups.google.com/g/rabbitmq-users).

### How can I contribute to Pika?

You can [fork the project on GitHub](https://help.github.com/en/articles/fork-a-repo/) and open
[pull requests](https://help.github.com/en/articles/about-pull-requests/)
when you believe you have something solid to add to the main repository.
