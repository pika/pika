import pika
import time
import logging

logging.basicConfig(level=logging.DEBUG)

ITERATIONS = 100

connection = pika.BlockingConnection(pika.URLParameters('amqp://guest:guest@localhost:5672/%2F?heartbeat_interval=1'))
channel = connection.channel()

def closeit():
    print('Close it')
    connection.close()

connection.add_timeout(5, closeit)

connection.sleep(100)

"""
channel.confirm_delivery()
start_time = time.time()

for x in range(0, ITERATIONS):
    if not channel.basic_publish(exchange='test',
                                 routing_key='',
                                 body='Test 123',
                                 properties=pika.BasicProperties(content_type='text/plain',
                                                                 app_id='test',
                                                                 delivery_mode=1)):
        print('Delivery not confirmed')
    else:
        print('Confirmed delivery')

channel.close()
connection.close()

duration = time.time() - start_time
print("Published %i messages in %.4f seconds (%.2f messages per second)" % (ITERATIONS, duration, (ITERATIONS/duration)))

"""
