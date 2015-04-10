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

import uuid

from amqpstorm import UriConnection
from simplebus.enums import DeliveryMode
from simplebus.transports.core import Cancellation
from simplebus.transports.core import Confirmation
from simplebus.transports.core import Message
from simplebus.transports.core import Transport
from threading import Lock
from threading import Thread


class AmqpTransport(Transport):
    def __init__(self, url):
        self.__connection = None
        self.__connection_lock = Lock()
        self.__cancellations = []
        self.__url = url

    def close(self):
        if not self.__connection:
            return

        while len(self.__cancellations) > 0:
            try:
                cancellation = self.__cancellations.pop()
                cancellation.cancel()
            except:
                pass

        self.__connection.close()
        self.__connection = None

    def dequeue(self, queue, exchange, callback):
        cancellation = AmqpCancellation()

        self.__dequeue(queue, exchange, callback, cancellation)

        return cancellation

    def enqueue(self, queue, exchange, message):
        if not exchange:
            exchange = 'amq.direct'

        self.__create_queue(queue, exchange)

        self.__send_message(exchange, queue, message)

    def publish(self, topic, exchange, message):
        if not exchange:
            exchange = 'amq.topic'

        self.__create_exchange(topic, exchange)

        self.__send_message(exchange, topic, message)

    def subscribe(self, topic, exchange, callback):
        cancellation = AmqpCancellation()

        self.__subscribe(topic, exchange, callback, cancellation)

        return cancellation

    def _get_channel(self):
        self.__ensure_connection()
        return self.__connection.channel()

    def __create_queue(self, queue, exchange):
        with self._get_channel() as channel:
            channel.queue.declare(queue, durable=True)

            if exchange:
                channel.exchange.declare(exchange, durable=True)
                channel.queue.bind(queue, exchange, queue)

    def __create_exchange(self, topic, exchange):
        if not exchange:
            exchange = topic

        with self._get_channel() as channel:
            channel.exchange.declare(exchange, 'topic', durable=True)

    def __dequeue(self, queue, exchange, callback, cancellation):
        def start():
            try:
                channel.start_consuming()
            except:
                pass

        def on_message(body, ch, method, properties):
            message = self.__to_message(body, ch, method, properties)
            callback(message)

        self.__create_queue(queue, exchange)

        consumer_tag = str(uuid.uuid1())

        channel = self._get_channel()
        channel.basic.qos(1)
        channel.basic.consume(on_message, queue, consumer_tag)

        thread = Thread(target=start)
        thread.daemon = True
        thread.start()

        cancellation.consumer_tag = consumer_tag
        cancellation.channel = channel
        cancellation.thread = thread

    def __subscribe(self, topic, exchange, callback, cancellation):
        def start():
            try:
                channel.start_consuming()
            except:
                pass

        def on_message(body, ch, method, properties):
            message = self.__to_message(body, ch, method, properties)
            callback(message)

        if not exchange:
            exchange = 'amq.topic'

        self.__create_exchange(topic, exchange)

        queue_name = topic + '_' + str(uuid.uuid1())
        consumer_tag = str(uuid.uuid1())

        channel = self._get_channel()
        channel.queue.declare(queue_name, exclusive=True, auto_delete=True)
        channel.queue.bind(queue_name, exchange, topic)

        channel.basic.qos(1)
        channel.basic.consume(on_message, queue_name, consumer_tag)

        thread = Thread(target=start)
        thread.daemon = True
        thread.start()

        cancellation.consumer_tag = consumer_tag
        cancellation.channel = channel
        cancellation.thread = thread

    def __send_message(self, exchange, routing_key, message):
        properties = {
            'delivery_mode': DeliveryMode.persistent if message.delivery_mode is None else message.delivery_mode.value,
            'expiration': None if message.expiration is None else str(message.expiration)
        }

        with self._get_channel() as channel:
            channel.confirm_deliveries()
            channel.basic.publish(message.body, routing_key, exchange, properties=properties)

    def __ensure_connection(self):
        if self.__connection and self.__connection.is_open:
            return

        with self.__connection_lock:
            if self.__connection and self.__connection.is_open:
                return

            if self.__connection:
                self.__connection.close()
                self.__connection = None

            self.__connection = UriConnection(self.__url)
            self.__connection.open()

    @staticmethod
    def __to_message(body, channel, method, properties):
        def get_property(name, default, convert=None):
            if name in properties:
                value = properties[name]
                if value is not None and value != '' and convert:
                    return convert(value)
                return value

            return default

        return Message(
            str(body, encoding='utf-8'),
            get_property('delivery_mode', DeliveryMode.persistent, lambda x: DeliveryMode(x)),
            get_property('expiration', None, lambda x: int(x)),
            AmqpConfirmation(channel, method.get('delivery_tag')))


class AmqpCancellation(Cancellation):
    def __init__(self):
        self.consumer_id = None
        self.channel = None
        self.thread = None

    def cancel(self):
        self.channel.basic.cancel(self.consumer_id)


class AmqpConfirmation(Confirmation):
    def __init__(self, channel, delivery_tag):
        self.__channel = channel
        self.__delivery_tag = delivery_tag

    def complete(self):
        self.__channel.basic.ack(self.__delivery_tag)

    def reject(self):
        self.__channel.basic.reject(self.__delivery_tag)
