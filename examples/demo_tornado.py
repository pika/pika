#!/usr/bin/env python
"""
Tornado and Pika Example

This is meant to illustrate that Pika+Tornado works, not to provide a template for how to use it.

We create the Tornado application object and store our Pika objects in that under a Pika dictionary. We 
then add a timeout to the IOLoop to start Pika off 1 second after starting the application.

The application serve a basic webpage that upon request, if Pika is not connected, attempts to connect. It
then will append a pending message list with the message we would have otherwise sent into RabbitMQ. On a
request where we are already connected to RabbitMQ, it will send the message then retrieve any messages
we received asynchronously from RabbitMQ.

After CLOSE_AFTER requests, the app will unbind the basic_consume with basic_cancel and then close the connection.
On the subsequent request it will reconnect to RabbitMQ. This is just to demonstrate reconnecting when needed.
"""

import logging
import os
import pika

import tornado.httpserver
import tornado.ioloop
import tornado.web

from pika.tornado_adapter import TornadoConnection

# Do a basic cancel after n messages
CLOSE_AFTER = 10

# Counter for messages received
close_count = 0

# Keep track of if we're connecting to RabbitMQ
connecting = False

class MainHandler(tornado.web.RequestHandler):

    @tornado.web.asynchronous
    def get(self):
        
        # Build a message to publish to RabbitMQ
        message = '%.8f: Request from %s [%s]' % (self.request._start_time,
                                                  self.request.remote_ip,
                                                  self.request.headers.get("User-Agent"))

        # Keep a variable so we dont have to call multiple times in one execution
        connected = self.application.pika['connection'].is_alive()

        # We won't always be connected, connect if we haven't or reconnect if we were disconnected
        if not connected:
            pika_connect()
            self.application.pika['pending'].append(message)
            connected = 'False'
        else:
            # Send our message to RabbitMQ    
            properties = pika.BasicProperties(content_type="text/text",
                                              delivery_mode=1)
    
            self.application.pika['channel'].basic_publish(exchange='tornado',
                                                           routing_key='tornado.*',
                                                           body=message,
                                                           properties=properties,
                                                           block_on_flow_control=False)
            connected = 'True'

        # Atomically pull all the messages out of the list and put them in a list for rendering
        message_list = []
        while len(self.application.pika['messages']) > 0:
            message_list.append(self.application.pika['messages'].pop(0))

        self.render("demo_tornado.html", 
                    CLOSE_AFTER=CLOSE_AFTER,
                    connected=connected, 
                    messages=message_list)

def pika_connect():

    global application, connecting

    if connecting:
        logging.info('Already connecting to RabbitMQ')
        return
        
    connecting = True

    logging.info('Connecting to RabbitMQ on localhost:5672')

    # Connect to RabbitMQ
    credentials = pika.PlainCredentials('guest', 'guest')
    
    param = pika.ConnectionParameters(host='localhost', port=5672, virtual_host="/",
                                      credentials=credentials)
    
    application.pika['connection'] = TornadoConnection(param,
                                                       wait_for_open=False,
                                                       callback=on_pika_connected)

def on_pika_connected():                                            

    global application, connecting
    
    connecting = False                                    
                    
    logging.info('Connected to RabbitMQ on localhost:5672')
                                                
    application.pika['channel'] = application.pika['connection'].channel()

    application.pika['channel'].exchange_declare(exchange='tornado',
                                                 type="direct",
                                                 auto_delete=True,
                                                 durable=False)

    queue_name = 'test-%i' % os.getpid()

    application.pika['channel'].queue_declare(queue=queue_name, 
                                              auto_delete=True, 
                                              durable=False, 
                                              exclusive=False)

    application.pika['channel'].queue_bind(exchange='tornado', 
                                           queue=queue_name,
                                           routing_key='tornado.*')
                                           

    application.pika['channel'].basic_consume(consumer=on_pika_message,
                                              queue=queue_name,
                                              no_ack=True)
                                           
    application.pika['connected'] = True

    # Send any messages pending
    for message in application.pika['pending']:

        properties = pika.BasicProperties(content_type="text/text",
                                          delivery_mode=1)

        application.pika['channel'].basic_publish(exchange='tornado',
                                                  routing_key='tornado.*',
                                                  body=message,
                                                  properties=properties,
                                                  block_on_flow_control=False)

def on_pika_message(channel, method, header, body):

    global application, close_count

    logging.info("Received Message from RabbitMQ: %s" % body)

    # Just append our message stack with the body of the RabbitMQ message    
    application.pika['messages'].append(body)

    close_count += 1
    if close_count == CLOSE_AFTER:
        close_count = 0
        
        # Cancel our basic_consumes
        for tag in application.pika['channel'].callbacks:
            logging.info('Sending basic_cancel for "%s"' % tag)
            application.pika['channel'].basic_cancel(tag)
        
        # Close our pika connection on a timer so we can finish our basic_cancel
        logging.info("Closing our RabbitMQ Connection in 1 second")        
        tornado.ioloop.IOLoop.instance().add_timeout(1000, application.pika['connection'].close)

settings = {"debug": True}

application = tornado.web.Application([
    (r"/", MainHandler),
], **settings)

# Just append a pika dict to hold our various needed bits
application.pika = {}

# Make a messages list for our test app
application.pika['messages'] = []
application.pika['pending'] = []

if __name__ == "__main__":
    
    logging.basicConfig(level=logging.DEBUG)
    logging.info("Starting Tornado HTTPServer")
    
    http_server = tornado.httpserver.HTTPServer(application)
    http_server.listen(8888)
    
    # Get a handle to the instance of IOLoop
    ioloop = tornado.ioloop.IOLoop.instance()

    # Add our Pika connect to the IOLoop
    ioloop.add_timeout(1000, pika_connect)

    # Start the IOLoop
    try:
        ioloop.start()
    except KeyboardInterrupt:
    
        # Shut Pika/RabbitMQ down cleanly
        if application.pika['connection'].is_alive():
            for tag in application.pika['channel'].callbacks:
                logging.info('Sending basic_cancel for "%s"' % tag)
                application.pika['channel'].basic_cancel(tag)
        application.pika['connection'].close()

        # Stop tornado
        ioloop.stop()

        logging.info("Application Shutdown")        
