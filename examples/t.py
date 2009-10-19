import pika
import asyncore

def t_codec():
    import pika.spec
    import datetime

    p = pika.BasicProperties(content_type = 'text/plain',
                             delivery_mode = 2,
                             headers = {'hello': 'world',
                                        'time': datetime.datetime.utcnow()})
    print pika.BasicProperties()
    pe = ''.join(p.encode())
    print pe.encode('hex')
    print pika.BasicProperties().decode(pe)

    m = pika.spec.Connection.Start(server_properties = {"prop1": "hello",
                                                            "prop2": 123})
    print m
    me = ''.join(m.encode())
    print me.encode('hex')
    print pika.spec.Connection.Start().decode(me)

# t_codec()

def handle_delivery(method, header, body):
    print (method, header, body)
    ch.basic_ack(delivery_tag = method.delivery_tag)
    if body == 'quit':
        ch.basic_cancel(tag)
        ch.close()
        c.close()

c = pika.AsyncoreConnection('127.0.0.1')
ch = c.channel()
ch.queue_declare(queue = "test")
tag = ch.basic_consume(handle_delivery, queue = 'test')
ch.basic_publish('', "test", "hello!", pika.BasicProperties(content_type = "text/plain"))
asyncore.loop()
