# -*- coding: utf-8 -*-
# pylint: disable=C0111,C0103,R0205

import threading
from time import sleep
from pika import ConnectionParameters, BlockingConnection, PlainCredentials


class Publisher(threading.Thread):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.daemon = True
        self.is_running = True
        self.name = "Publisher"
        self.queue = "downstream_queue"

        credentials = PlainCredentials("guest", "guest")
        parameters = ConnectionParameters("localhost", credentials=credentials)
        self.connection = BlockingConnection(parameters)
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=self.queue, auto_delete=True)

    def run(self):
        while self.is_running:
            self.connection.process_data_events(time_limit=1)

    def _publish(self, message):
        self.channel.basic_publish("", self.queue, body=message.encode())

    def publish(self, message):
        self.connection.add_callback_threadsafe(lambda: self._publish(message))

    def stop(self):
        print("Stopping...")
        self.is_running = False
        # Wait until all the data events have been processed
        self.connection.process_data_events(time_limit=1)
        if self.connection.is_open:
            self.connection.close()
        print("Stopped")


if __name__ == "__main__":
    publisher = Publisher()
    publisher.start()
    try:
        for i in range(9999):
            msg = f"Message {i}"
            print(f"Publishing: {msg!r}")
            publisher.publish(msg)
            sleep(1)
    except KeyboardInterrupt:
        publisher.stop()
    finally:
        publisher.join()
