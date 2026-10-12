import pika
import pika.exceptions

# Open a connection to RabbitMQ on localhost using all default parameters
connection = pika.BlockingConnection()

# Open the channel
channel = connection.channel()

# Declare the queue
channel.queue_declare(queue='test',
                      durable=True,
                      exclusive=False,
                      auto_delete=False)

# Enabled delivery confirmations. This is REQUIRED.
channel.confirm_delivery()

properties = pika.BasicProperties(content_type='text/plain',
                                  delivery_mode=pika.DeliveryMode.Transient)

# Send a message the broker can route. The default exchange ('') routes by
# queue name, so this reaches the queue declared above. The handler is here for
# symmetry with the unroutable case below; a message the broker accepts is
# never returned, so it cannot fire.
try:
    channel.basic_publish(exchange='',
                          routing_key='test',
                          body=b'Hello World!',
                          properties=properties,
                          mandatory=True)
    print('Message was published')
except pika.exceptions.UnroutableError:
    print('Message was returned')

# Clear it again, so repeated runs do not pile messages up in the queue and
# leave the other examples that read `test` dependent on what ran before.
channel.basic_get('test', auto_ack=True)

# Now send one the broker cannot route. `mandatory=True` is what makes the
# broker hand it back instead of discarding it silently.
try:
    channel.basic_publish(exchange='',
                          routing_key='no-such-queue',
                          body=b'Hello World!',
                          properties=properties,
                          mandatory=True)
    print('Message was published')
except pika.exceptions.UnroutableError:
    print('Message was returned')
