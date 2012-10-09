# Pika, an AMQP 0-9-1 client library for Python

## Introduction
Pika is a pure-Python implementation of the AMQP 0-9-1 protocol that tries
to stay fairly independent of the underlying network support library.

 * Since threads aren't appropriate to every situation, it doesn't
   require threads. It takes care not to forbid them, either. The same
   goes for greenlets, callbacks, continuations and generators. It is
   not necessarily thread-safe however, and your milage will vary.

 * People may be using direct sockets, `asyncore`, plain old `select()`,
   or any of the wide variety of ways of getting network events to and from a
   python application. Pika tries to stay compatible with all of these, and to
   make adapting it to a new environment as simple as possible.

## Documentation
Pika's documentation is now at http://pika.github.com/

## Example
Here is the most simple example of use, sending a message with the BlockingConnection adapter:

    import pika
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    channel.basic_publish(exchange='example',
                          routing_key='test',
                          body='Test Message')
    connection.close()

## Pika provides the following adapters
 * AsyncoreConnection - based off the standard Python library asyncore
 * BlockingConnection - enables blocking, synchronous operation on top of
                        library for simple uses
 * SelectConnection   - fast asynchronous adapter
 * TornadoConnection  - adapter for use with the Tornado IO Loop http://tornadoweb.org

## License
Pika is licensed under the MPL, and may also be used under the terms
of the GPL. The full license text is included with the source code for
the package in the LICENSE-GPL-2.0 and LICENSE-MPL-Pika files. If you
have any questions regarding licensing, please contact the RabbitMQ team
at <info@rabbitmq.com>.
