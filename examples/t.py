import rabbitmq
import asyncore

def t_codec():
    import rabbitmq.spec
    import datetime

    p = rabbitmq.BasicProperties(content_type = 'text/plain',
                                 delivery_mode = 2,
                                 headers = {'hello': 'world',
                                            'time': datetime.datetime.utcnow()})
    print rabbitmq.BasicProperties()
    pe = ''.join(p.encode())
    print pe.encode('hex')
    print rabbitmq.BasicProperties().decode(pe)

    m = rabbitmq.spec.Connection.Start(server_properties = {"prop1": "hello",
                                                            "prop2": 123})
    print m
    me = ''.join(m.encode())
    print me.encode('hex')
    print rabbitmq.spec.Connection.Start().decode(me)

# t_codec()

def handle_delivery(method, header, body):
    print (method, header, body)
    ch.basic_ack(delivery_tag = method.delivery_tag)
    if body == 'quit':
        ch.basic_cancel(tag)
        ch.close()
        c.close()

c = rabbitmq.AsyncoreConnection('127.0.0.1')
ch = c.channel()
ch.queue_declare(queue = "test")
tag = ch.basic_consume(handle_delivery, queue = 'test')
ch.basic_publish('', "test", "hello!", rabbitmq.BasicProperties(content_type = "text/plain"))
asyncore.loop()
