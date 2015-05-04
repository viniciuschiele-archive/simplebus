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

from simplebus.transports import base
from simplebus.utils import EventHandler
from threading import Lock
from threading import Thread


LOGGER = logging.getLogger(__name__)


class Transport(base.Transport):
    def __init__(self, url):
        if amqpstorm is None:
            raise ImportError('Missing amqp-storm library (pip install amqp-storm)')

        self.__connection = None
        self.__connection_lock = Lock()
        self.__closed_lock = Lock()
        self.__closed_called = False
        self.__url = url
        self.__cancellations = {}
        self.__subscriptions = {}
        self.closed = EventHandler()

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

    def push(self, queue, message, options):
        self.__send_message('', queue, message)

    def pull(self, id, queue, callback, options):
        dead_letter_queue = None
        dead_letter_enabled = options.get('dead_letter_enabled')

        retry_queue = None
        max_retry_count = options.get('max_retry_count')
        retry_delay = options.get('retry_delay')

        def on_message(body, ch, method, properties):
            message = self.__to_message(body, ch, method, properties, dead_letter_queue, retry_queue)

            if message.retry_count > max_retry_count:
                message.dead_letter('Max retry exceeded.')
            else:
                callback(message)

        if dead_letter_enabled:
            dead_letter_queue = queue + '.error'
            self.__create_dead_letter_queue(dead_letter_queue)

        if max_retry_count > 0 and retry_delay > 0:
            retry_queue = queue + '.retry'
            self.__create_retry_queue(queue, retry_queue, retry_delay)

        channel = self.__get_channel()
        channel.queue.declare(queue, durable=True)
        channel.basic.qos(options.get('prefetch_count'))
        channel.basic.consume(on_message, queue, id)

        thread = Thread(target=self.__start_receiving, args=(channel,))
        thread.daemon = True
        thread.start()

        self.__cancellations[id] = channel

    def publish(self, topic, message, options):
        self.__send_message(topic, '', message)

    def subscribe(self, id, topic, callback, options):
        def on_message(body, ch, method, properties):
            message = self.__to_message(body, ch, method, properties, None, None)
            callback(message)

        queue_name = topic + ':' + id

        channel = self.__get_channel()
        channel.exchange.declare(topic, 'topic', durable=True)
        channel.queue.declare(queue_name, exclusive=True, auto_delete=True)
        channel.queue.bind(queue_name, topic)
        channel.basic.qos(options.get('prefetch_count'))
        channel.basic.consume(on_message, queue_name, id)

        thread = Thread(target=self.__start_receiving, args=(channel,))
        thread.daemon = True
        thread.start()

        self.__subscriptions[id] = channel

    def unsubscribe(self, id):
        channel = self.__subscriptions.pop(id)
        if channel:
            channel.close()

    def __create_dead_letter_queue(self, dead_letter_queue):
        with self.__get_channel() as channel:
            channel.queue.declare(dead_letter_queue, durable=True)

    def __create_retry_queue(self, queue, retry_queue, retry_delay):
        args = {
            'x-dead-letter-exchange': '',
            'x-dead-letter-routing-key': queue,
            'x-message-ttl': retry_delay}

        try:
            with self.__get_channel() as channel:
                channel.queue.declare(retry_queue, durable=True, arguments=args)
        except amqpstorm.AMQPChannelError as e:
            if 'x-message-ttl' not in str(e):
                raise

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

    def __send_message(self, exchange, routing_key, message):
        properties = {
            'app_id': message.app_id,
            'message_id': message.message_id,
            'delivery_mode': 2
        }

        if message.expiration:
            properties['expiration'] = str(message.expiration)

        if message.retry_count > 0:
            properties['headers'] = {'x-retry-count': message.retry_count}

        with self.__get_channel() as channel:
            channel.confirm_deliveries()
            channel.basic.publish(message.body, routing_key, exchange, mandatory=True, properties=properties)

    def __start_receiving(self, channel):
        try:
            channel.start_consuming()
        except Exception as e:
            self.__on_exception(e)

    @staticmethod
    def __to_message(body, channel, method, properties, dead_letter_queue, retry_queue):
        return TransportMessage(body, method, properties, channel, dead_letter_queue, retry_queue)


class TransportMessage(base.TransportMessage):
    def __init__(self, body, method, properties, channel, dead_letter_queue, retry_queue):
        super().__init__(body=body)

        self.__method = method
        self.__properties = properties
        self.__channel = channel
        self.__dead_letter_queue = dead_letter_queue
        self.__retry_queue = retry_queue

        app_id = properties.get('app_id')
        if app_id:
            self.app_id = bytes.decode(properties.get('app_id'))

        message_id = properties.get('message_id')
        if message_id:
            self.message_id = bytes.decode(properties.get('message_id'))

        headers = properties.get('headers')
        if not headers:
            headers = {}
            properties['headers'] = headers

        self._retry_count = headers.get(bytes('x-retry-count', 'utf-8')) or 0

        expiration = self.__properties.get('expiration')
        if expiration:
            self.expiration = int(expiration)

    def delete(self):
        if self.__channel:
            self.__channel.basic.ack(self.__method.get('delivery_tag'))
            self.__channel = None

    def dead_letter(self, reason):
        if self.__channel:
            self.__set_header_retry_count(0)
            self.__set_header_death_reason(reason)

            self.__channel.basic.ack(self.__method.get('delivery_tag'))

            if not self.__dead_letter_queue:
                return

            self.__channel.basic.publish(
                self.body,
                self.__dead_letter_queue,
                '',
                self.__properties)
            self.__channel = None

    def retry(self):
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

    def __set_header_death_reason(self, reason):
        headers = self.__properties.get('headers')
        headers[bytes('x-death-reason', 'utf-8')] = reason

    def __set_header_retry_count(self, retry_count):
        headers = self.__properties.get('headers')
        headers[bytes('x-retry-count', 'utf-8')] = retry_count