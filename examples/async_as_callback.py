import asyncio
import functools
import logging
import ssl
import time
from enum import Enum
from typing import Dict
from typing import Optional

import msgpack
from pydantic import BaseModel, validator

import pika
from pika.exceptions import AMQPConnectionError

logger = logging.getLogger(__name__)


def sync(f):
    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        return asyncio.get_event_loop().run_until_complete(f(*args, **kwargs))

    return wrapper


class Priority(Enum):
    LOW = 1
    NORMAL = 5
    HIGH = 10


class Headers(BaseModel):
    job_id: str
    priority: Priority
    task_type: Optional[str] = None

    @validator("priority", pre=True)
    def _convert_priority(self, value):
        return Priority[value]


class RabbitMQConfig(BaseModel):
    host: str
    port: int
    username: str
    password: str
    protocol: str


class BasicPikaClient:
    def __init__(self):
        self.username = "username"
        self.password = "password"
        self.host = "localhost"
        self.port = 5672
        self.protocol = "amqp"

        self._init_connection_parameters()
        self._connect()

    def _connect(self):
        tries = 0
        while True:
            try:
                self.connection = pika.BlockingConnection(self.parameters)
                self.channel = self.connection.channel()
                if self.connection.is_open:
                    break
            except (AMQPConnectionError, Exception) as e:
                time.sleep(5)
                tries += 1
                if tries == 20:
                    raise AMQPConnectionError(e)

    def _init_connection_parameters(self):
        self.credentials = pika.PlainCredentials(self.username, self.password)
        self.parameters = pika.ConnectionParameters(
            self.host,
            int(self.port),
            "/",
            self.credentials,
        )
        if self.protocol == "amqps":
            # SSL Context for TLS configuration of Amazon MQ for RabbitMQ
            ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
            ssl_context.set_ciphers("ECDHE+AESGCM:!ECDSA")
            self.parameters.ssl_options = pika.SSLOptions(context=ssl_context)

    def check_connection(self):
        if not self.connection or self.connection.is_closed:
            self._connect()

    def close(self):
        self.channel.close()
        self.connection.close()

    def declare_queue(
        self, queue_name, exclusive: bool = False, max_priority: int = 10
    ):
        self.check_connection()
        logger.debug(f"Trying to declare queue({queue_name})...")
        self.channel.queue_declare(
            queue=queue_name,
            exclusive=exclusive,
            durable=True,
            arguments={"x-max-priority": max_priority},
        )

    def declare_exchange(self, exchange_name: str, exchange_type: str = "direct"):
        self.check_connection()
        self.channel.exchange_declare(
            exchange=exchange_name, exchange_type=exchange_type
        )

    def bind_queue(self, exchange_name: str, queue_name: str, routing_key: str):
        self.check_connection()
        self.channel.queue_bind(
            exchange=exchange_name, queue=queue_name, routing_key=routing_key
        )

    def unbind_queue(self, exchange_name: str, queue_name: str, routing_key: str):
        self.channel.queue_unbind(
            queue=queue_name, exchange=exchange_name, routing_key=routing_key
        )


class BasicMessageSender(BasicPikaClient):
    def encode_message(self, body: Dict, encoding_type: str = "bytes"):
        if encoding_type == "bytes":
            return msgpack.packb(body)
        else:
            raise NotImplementedError

    def send_message(
        self,
        exchange_name: str,
        routing_key: str,
        body: Dict,
        headers: Optional[Headers],
    ):
        body = self.encode_message(body=body)
        self.channel.basic_publish(
            exchange=exchange_name,
            routing_key=routing_key,
            body=body,
            properties=pika.BasicProperties(
                delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE,
                priority=headers.priority.value,
                headers=headers.dict(),
            ),
        )
        logger.debug(
            f"Sent message. Exchange: {exchange_name}, Routing Key: {routing_key}, Body: {body[:128]}"
        )


class BasicMessageReceiver(BasicPikaClient):
    def __init__(self):
        super().__init__()
        self.channel_tag = None

    def decode_message(self, body):
        if type(body) == bytes:
            return msgpack.unpackb(body)
        else:
            raise NotImplementedError

    def get_message(self, queue_name: str, auto_ack: bool = False):
        method_frame, header_frame, body = self.channel.basic_get(
            queue=queue_name, auto_ack=auto_ack
        )
        if method_frame:
            logger.debug(f"{method_frame}, {header_frame}, {body}")
            return method_frame, header_frame, body
        else:
            logger.debug("No message returned")
            return None

    def consume_messages(self, queue, callback):
        self.check_connection()
        self.channel_tag = self.channel.basic_consume(
            queue=queue, on_message_callback=callback, auto_ack=True
        )
        logger.debug(" [*] Waiting for messages. To exit press CTRL+C")
        self.channel.start_consuming()

    def cancel_consumer(self):
        if self.channel_tag is not None:
            self.channel.basic_cancel(self.channel_tag)
            self.channel_tag = None
        else:
            logger.error("Do not cancel a non-existing job")


class MyConsumer(BasicMessageReceiver):
    @sync
    async def consume(self, channel, method, properties, body):
        body = self.decode_message(body=body)
        file_content = await self._download_image(img_url=body["url"])
        # consume message logic ...

    async def _download_image(self, img_url):
        # do some async stuff here
        pass


def create_consumer():
    worker = MyConsumer()
    worker.declare_queue(queue_name="myqueue")
    worker.declare_exchange(exchange_name="myexchange")
    worker.bind_queue(
        exchange_name="myexchange", queue_name="myqueue", routing_key="randomkey"
    )
    worker.consume_messages(queue="myqueue", callback=worker.consume)


if __name__ == "__main__":
    create_consumer()
