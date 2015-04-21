# Copyright 2015 Vinicius Chiele. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
import uuid

from amqpstorm import UriConnection
from simplebus.transports.core import Cancellable
from simplebus.transports.core import Confirmation
from simplebus.transports.core import Transport
from simplebus.transports.core import Message
from threading import Lock
from threading import Thread


LOGGER = logging.getLogger(__name__)


class AmqpTransport(Transport):
    def __init__(self, url):
        self.__connection = None
        self.__connection_lock = Lock()
        self.__closed_lock = Lock()
        self.__closed_called = False
        self.__url = url

    @property
    def is_open(self):
        return self.__connection and self.__connection.is_open

    def open(self):
        if self.is_open:
            return

        self.__closed_called = False
        self.__ensure_connection()

    def close(self):
        if not self.is_open:
            return

        self.__connection.close()
        self.__connection = None

    def consume(self, queue, callback):
        def on_message(body, ch, method, properties):
            message = self.__to_message(body, ch, method, properties)
            callback(message)

        self.__create_queue(queue)

        id = str(uuid.uuid4())

        channel = self.__get_channel()
        channel.basic.qos(1)
        channel.basic.consume(on_message, queue, id)

        thread = Thread(target=self.__start_receiving, args=(channel,))
        thread.daemon = True
        thread.start()

        return AmqpCancellable(channel)

    def send(self, queue, message):
        self.__create_queue(queue)
        self.__send_message(queue, '', message)

    def publish(self, topic, message):
        self.__create_topic(topic)
        self.__send_message(topic, '', message)

    def subscribe(self, topic, callback):
        def on_message(body, ch, method, properties):
            message = self.__to_message(body, ch, method, properties)
            callback(message)

        self.__create_topic(topic)

        id = str(uuid.uuid4())

        queue_name = topic + '-' + id

        channel = self.__get_channel()
        channel.queue.declare(queue_name, exclusive=True, auto_delete=True)
        channel.queue.bind(queue_name, topic)
        channel.basic.qos(1)
        channel.basic.consume(on_message, queue_name, id)

        thread = Thread(target=self.__start_receiving, args=(channel,))
        thread.daemon = True
        thread.start()

        return AmqpCancellable(channel)

    def __create_queue(self, queue):
        with self.__get_channel() as channel:
            channel.queue.declare(queue, durable=True)
            channel.exchange.declare(queue, durable=True)
            channel.queue.bind(queue, queue, '')

    def __create_topic(self, topic):
        with self.__get_channel() as channel:
            channel.exchange.declare(topic, 'topic', durable=True)

    def __get_channel(self):
        if not self.is_open:
            raise RuntimeError('Transport is not opened.')

        self.__ensure_connection()
        return self.__connection.channel()

    def __ensure_connection(self):
        if self.__connection and self.__connection.is_open:
            return

        with self.__connection_lock:
            if self.__connection and self.__connection.is_open:
                return

            self.__connection = UriConnection(self.__url)
            self.__connection.open()

    def __on_exception(self, exc):
        if not self.closed:
            return

        if self.__closed_called:
            return

        with self.__closed_lock:
            if self.__closed_called:
                return
            self.__closed_called = True
            self.closed()

    def __send_message(self, exchange, routing_key, message):
        properties = {
            'message_id': message.id,
            'delivery_mode': 2,
            'expiration': None if message.expires is None else str(message.expires),
        }

        if message.delivery_count > 0:
            properties['headers'] = {
                'x-delivery-count': message.delivery_count
            }

        with self.__get_channel() as channel:
            channel.confirm_deliveries()
            channel.basic.publish(message.body, routing_key, exchange, properties=properties)

    def __start_receiving(self, channel):
        try:
            channel.start_consuming()
        except Exception as e:
            self.__on_exception(e)

    @staticmethod
    def __to_message(body, channel, method, properties):
        id = properties.get('message_id')

        if id:
            id = bytes.decode(properties.get('message_id'))
        else:
            id = None

        headers = properties.get('headers')
        if not headers:
            headers = {}
            properties['headers'] = headers

        delivery_count = headers.get(bytes('x-delivery-count', 'utf-8'))
        if not delivery_count:
            delivery_count = 0

        delivery_count += 1

        headers[bytes('x-delivery-count', 'utf-8')] = delivery_count

        expires = properties.get('expiration')
        if expires:
            expires = int(expires)

        confirmation = AmqpConfirmation(id, body, method, properties, channel)

        message = Message(id, body, delivery_count, expires, confirmation)

        return message


class AmqpCancellable(Cancellable):
    def __init__(self, channel):
        self.__channel = channel

    def cancel(self):
        self.__channel.close()


class AmqpConfirmation(Confirmation):
    def __init__(self, id, body, method, properties, channel):
        self.__id = id
        self.__body = body
        self.__method = method
        self.__properties = properties
        self.__channel = channel

    def complete(self):
        if self.__channel:
            self.__channel.basic.ack(self.__method.get('delivery_tag'))

    def defer(self):
        if self.__channel:
            self.__channel.basic.ack(self.__method.get('delivery_tag'))

            self.__channel.basic.publish(
                self.__body,
                str(self.__method.get('routing_key'), encoding='utf-8'),
                str(self.__method.get('exchange'), encoding='utf-8'),
                self.__properties)


