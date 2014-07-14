import pika

def on_message(channel, method_frame, header_frame, body):
    channel.queue_declare(queue=body, auto_delete=True)

    if body.startswith("queue:"):
        queue = body.replace("queue:", "")
        key = body + "_key"
        print("Declaring queue %s bound with key %s" %(queue, key))
        channel.queue_declare(queue=queue, auto_delete=True)
        channel.queue_bind(queue=queue, exchange="test_exchange", routing_key=key)
    else:
        print("Message body", body)

    channel.basic_ack(delivery_tag=method_frame.delivery_tag)

credentials = pika.PlainCredentials('guest', 'guest')
parameters =  pika.ConnectionParameters('localhost', credentials=credentials)
connection = pika.BlockingConnection(parameters)

channel = connection.channel()
channel.exchange_declare(exchange="test_exchange", exchange_type="direct", passive=False, durable=True, auto_delete=False)
channel.queue_declare(queue="standard", auto_delete=True)
channel.queue_bind(queue="standard", exchange="test_exchange", routing_key="standard_key")
channel.basic_qos(prefetch_count=1)

channel.basic_consume(on_message, 'standard')

try:
    channel.start_consuming()
except KeyboardInterrupt:
    channel.stop_consuming()

connection.close()
