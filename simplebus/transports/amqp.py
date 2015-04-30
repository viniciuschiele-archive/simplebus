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

try:
    import amqpstorm
except ImportError:
    amqpstorm = None

import logging

from simplebus.transports.core import Transport
from simplebus.transports.core import Message
from threading import Lock
from threading import Thread


LOGGER = logging.getLogger(__name__)


class AmqpTransport(Transport):
    def __init__(self, url):
        if amqpstorm is None:
            raise ImportError('Missing amqp-storm library (pip install amqp-storm)')

        super().__init__()

        self.__connection = None
        self.__connection_lock = Lock()
        self.__closed_lock = Lock()
        self.__closed_called = False
        self.__url = url
        self.__cancellations = {}
        self.__subscriptions = {}

    @property
    def is_open(self):
        return self.__connection and self.__connection.is_open

    def open(self):
        if self.is_open:
            return
        self.__ensure_connection()

    def close(self):
        if not self.is_open:
            return

        self.__connection.close()
        self.__connection = None

    def cancel(self, id):
        channel = self.__cancellations.pop(id)
        if channel:
            channel.close()

    def push(self, queue, message):
        with self.__get_channel() as channel:
            self.__send_message(channel, '', queue, message)

    def pull(self, id, queue, callback, options):
        dead_letter_queue = None
        dead_letter_enabled = options.get('dead_letter_enabled')
        max_retry_count = options.get('max_retry_count')
        retry_delay = options.get('retry_delay')
        retry_queue = None

        def on_message(body, ch, method, properties):
            message = self.__to_message(body, ch, method, properties, dead_letter_queue, retry_queue)

            if message.retry_count > max_retry_count:
                message.dead_letter('Max retry exceeded.')
            else:
                callback(message)

        channel = self.__get_channel()
        channel.queue.declare(queue, durable=True)

        if dead_letter_enabled:
            dead_letter_queue = queue + '.error'
            channel.queue.declare(dead_letter_queue, durable=True)

        if max_retry_count and retry_delay > 0:
            retry_queue = queue + '.retry'
            channel.queue.declare(retry_queue, durable=True)

        channel.basic.qos(1)
        channel.basic.consume(on_message, queue, id)

        thread = Thread(target=self.__start_receiving, args=(channel,))
        thread.daemon = True
        thread.start()

        self.__cancellations[id] = channel

    def publish(self, topic, message):
        with self.__get_channel() as channel:
            try:
                self.__send_message(channel, topic, '', message)
                return
            except amqpstorm.AMQPChannelError as e:
                if 'NOT_FOUND' not in str(e):
                    raise e

        with self.__get_channel() as channel:
            self.__create_topic(channel, topic)
            self.__send_message(channel, topic, '', message)

    def subscribe(self, id, topic, callback):
        def on_message(body, ch, method, properties):
            message = self.__to_message(body, ch, method, properties, None, None)
            callback(message)

        queue_name = topic + '-' + id

        channel = self.__get_channel()
        self.__create_topic(channel, topic)
        channel.queue.declare(queue_name, exclusive=True, auto_delete=True)
        channel.queue.bind(queue_name, topic)
        channel.basic.qos(1)
        channel.basic.consume(on_message, queue_name, id)

        thread = Thread(target=self.__start_receiving, args=(channel,))
        thread.daemon = True
        thread.start()

        self.__subscriptions[id] = channel

    def unsubscribe(self, id):
        channel = self.__subscriptions.pop(id)
        if channel:
            channel.close()

    @staticmethod
    def __create_topic(channel, topic):
        channel.exchange.declare(topic, 'topic', durable=True)

    def __get_channel(self):
        if not self.is_open:
            raise RuntimeError('Transport is not opened.')

        self.__ensure_connection()
        return self.__connection.channel()

    def __ensure_connection(self):
        if self.is_open:
            return

        with self.__connection_lock:
            if self.is_open:
                return

            self.__connection = amqpstorm.UriConnection(self.__url)
            self.__connection.open()
            self.__closed_called = False

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

    @staticmethod
    def __send_message(channel, exchange, routing_key, message):
        properties = {
            'message_id': message.id,
            'delivery_mode': 2,
            'expiration': None if message.expires is None else str(message.expires),
        }

        if message.retry_count > 0:
            properties['headers'] = {
                'x-retry-count': message.retry_count
            }

        channel.confirm_deliveries()
        channel.basic.publish(message.body, routing_key, exchange, properties=properties)

    def __start_receiving(self, channel):
        try:
            channel.start_consuming()
        except Exception as e:
            self.__on_exception(e)

    @staticmethod
    def __to_message(body, channel, method, properties, dead_letter_queue, retry_queue):
        return AmqpMessage(body, method, properties, channel, dead_letter_queue, retry_queue)


class AmqpMessage(Message):
    def __init__(self, body, method, properties, channel, dead_letter_queue, retry_queue):
        super().__init__(body=body)

        self.__method = method
        self.__properties = properties
        self.__channel = channel

        self.id = properties.get('message_id')
        if self.id:
            self.id = bytes.decode(properties.get('message_id'))
        else:
            self.id = None

        headers = properties.get('headers')
        if not headers:
            headers = {}
            properties['headers'] = headers

        self.retry_count = headers.get(bytes('x-retry-count', 'utf-8')) or 0

        self.expires = self.__properties.get('expiration')
        if self.expires:
            self.expires = int(self.expires)

        self.__dead_letter_queue = dead_letter_queue
        self.__retry_queue = retry_queue

    def complete(self):
        if self.__channel:
            self.__channel.basic.ack(self.__method.get('delivery_tag'))
            self.__channel = None

    def dead_letter(self, reason):
        if self.__channel:
            self.__set_header_retry_count(0)

            self.__channel.basic.ack(self.__method.get('delivery_tag'))

            if not self.__dead_letter_queue:
                return

            self.__channel.basic.publish(
                self.body,
                self.__dead_letter_queue,
                '',
                self.__properties)
            self.__channel = None

    def defer(self):
        if self.__channel:
            self.__set_header_retry_count(self.retry_count + 1)

            if self.__retry_queue:
                routing_key = self.__retry_queue
                exchange = ''
            else:
                routing_key = str(self.__method.get('routing_key'), encoding='utf-8')
                exchange = str(self.__method.get('exchange'), encoding='utf-8')

            self.__channel.basic.ack(self.__method.get('delivery_tag'))
            self.__channel.basic.publish(
                self.body,
                routing_key,
                exchange,
                self.__properties)

    def __set_header_retry_count(self, retry_count):
        headers = self.__properties.get('headers')
        headers[bytes('x-retry-count', 'utf-8')] = retry_count