from pprint import pp
import logging
import traceback
import pika
from pika.credentials import PlainCredentials
import json
import os
import os.path
import threading
import time
from oauthlib.oauth2 import BackendApplicationClient
from requests_oauthlib import OAuth2Session
import abc

logger = logging.getLogger(__package__)

def mk_token_fetcher(token_url, client_id, client_secret, params):
    client = BackendApplicationClient(client_id=client_id)
    session = OAuth2Session(client=client)

    def fetch():
        token = session.fetch_token(
            token_url=token_url,
            client_id=client_id,
            client_secret=client_secret,
            **params,
        )
        return token['access_token']

    return fetch


class BaseConnectionSecretRefresher(metaclass=abc.ABCMeta):
    def __init__(self, secret_fetcher, refresh_interval=None):
        self.connection = None
        self.secret_fetcher = secret_fetcher
        self.current_secret = secret_fetcher()
        self.refresh_interval = 1200 if refresh_interval is None else refresh_interval

    def attach(self, connection):
        connection.add_on_open_callback(self.on_connected)

    @abc.abstractmethod
    def on_connected(self, connection):
        pass

    @abc.abstractmethod
    def on_close(self, connection):
        pass

    def update_secret(self, secret):
        """Updates connection secret in a thread-safe way."""
        self.current_secret = secret

        def update_it():
            logger.info("Updating secret")
            self.connection.update_secret(self.current_secret, "periodic_refresh")

        self.connection.ioloop.add_callback_threadsafe(update_it)


class BlockingConnectionSecretRefresher(BaseConnectionSecretRefresher):
    def __init__(self, secret_fetcher, refresh_interval=None):
        super().__init__(secret_fetcher, refresh_interval)
        self.timer = None

    def update_secret_and_reschedule(self):
        logger.info("Fetching new secret")
        self.update_secret(self.secret_fetcher())
        self.reschedule()

    def reschedule(self):
        logger.info("Scheduling secret refresh in {} seconds".format(self.refresh_interval))
        self.timer = self.connection.ioloop.call_later(self.refresh_interval, self.update_secret_and_reschedule)

    def on_connected(self, connection):
        self.connection = connection
        self.reschedule()

    def on_close(self, connection, exception):
        if self.timer is not None:
            logger.info("Removing secret refresh timer")
            self.connection.ioloop.remove_timeout(self.timer)


class TokenRefreshThread(threading.Thread):
    def __init__(self, secret_fetcher, on_success, on_error, refresh_interval):
        super().__init__()
        self.refresh_interval = refresh_interval
        self.stop_event = threading.Event()
        self.secret_fetcher = secret_fetcher
        self.on_success = on_success
        self.on_error = on_error

    def run(self):
        logger.info("Secret update thread started")
        while True:
            if self.stop_event.wait(timeout=self.refresh_interval):
                break
            try:
                self.on_success(self.secret_fetcher())
            except Exception as e:
                self.on_error(e)
                break
        logger.info("Exiting secret update thread")

    def stop(self):
        self.stop_event.set()


class ThreadedConnectionSecretRefresher(BaseConnectionSecretRefresher):
    def __init__(self, secret_fetcher, refresh_interval=None):
        super().__init__(secret_fetcher, refresh_interval)
        self.thread = None

    def on_connected(self, connection):
        self.connection = connection

        def raise_fun(exception):
            raise exception

        def on_error(exception):
            logger.error("Error while fetching secret: {}".format(traceback.format_exc()))
            logger.info("Making connection thread aware of the error")
            self.connection.ioloop.add_callback_threadsafe(lambda: raise_fun(exception))

        self.thread = TokenRefreshThread(self.secret_fetcher, self.update_secret, on_error, self.refresh_interval)
        self.thread.start()

    def on_close(self, connection):
        logger.info("Stopping secret refresh thread")
        self.thread.stop()
        self.thread.join()
        logger.info("Secret refresh thread cleanly stopped")


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')

    home = os.getenv("HOME")
    token_file = os.path.join(home, "saas-token.json")

    with open(token_file) as fp:
        token = json.load(fp)

    token_url = 'https://console.cloud.vmware.com/csp/gateway/am/api/auth/token'
    client_id = token['clientId']
    client_secret = token['clientSecret']
    params = {
        'orgId': token['orgId'], # XXX orgId is a part of service account name, but it's still required?
        'subject_token_type': 'urn:ietf:params:oauth:token-type:access_token',
    }

    instance_url = 'amqps://example.com:5671'
    conn_params = pika.URLParameters(instance_url)

    #secret_refresher = BlockingConnectionSecretRefresher(
    secret_refresher = ThreadedConnectionSecretRefresher(
        mk_token_fetcher(token_url, client_id, client_secret, params),
        refresh_interval=5,
    )

    conn_params.credentials = PlainCredentials("no-op", secret_refresher.current_secret)

    ping_timer = [None]

    def on_close(connection, exception):
        if ping_timer[0] is not None:
            logger.info("Removing ping timeout")
            connection.ioloop.remove_timeout(ping_timer[0])

    def on_connected(connection):
        connection.channel(on_open_callback=on_channel_open)

    def on_channel_open(new_channel):
        new_channel.queue_declare(queue="test", durable=True, callback=lambda frame: on_queue_declared(new_channel, frame))

    def on_queue_declared(channel, frame):
        channel.basic_consume("test", handle_delivery)
        ping_timer[0] = channel.connection.ioloop.call_later(5, lambda: ping(channel))

    def ping(channel):
        channel.basic_publish('', 'test', "ping", pika.BasicProperties(delivery_mode=pika.DeliveryMode.Persistent))
        ping_timer[0] = channel.connection.ioloop.call_later(5, lambda: ping(channel))

    def handle_delivery(channel, method_frame, header_frame, body):
        logger.info(body)
        channel.basic_ack(delivery_tag=method_frame.delivery_tag)

    connection = pika.SelectConnection(
        parameters=conn_params,
        on_open_callback=on_connected,
        on_close_callback=on_close,
    )

    secret_refresher.attach(connection)

    try:
        connection.ioloop.start()
    except KeyboardInterrupt:
        connection.close()
        connection.ioloop.start()
