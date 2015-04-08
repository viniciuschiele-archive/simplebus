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
        self.__consumers = {}
        self.__url = url

    def close(self):
        if not self.__connection:
            return

        while len(self.__consumers) > 0:
            try:
                consumer = self.__consumers.popitem()[1]
                consumer.cancel()
            except:
                pass

        try:
            self.__connection.close()
        except:
            pass

        self.__connection = None

    def push(self, queue, message):
        self.__ensure_connection()

        properties = {
            'delivery_mode': DeliveryMode.persistent if message.delivery_mode is None else message.delivery_mode.value,
            'expiration': None if message.expiration is None else str(message.expiration)
        }

        with self._get_channel() as channel:
            channel.queue.declare(queue, durable=True)
            channel.basic.publish(message.body, queue, properties=properties)

    def pull(self, queue, callback):
        self.__ensure_connection()

        consumer = AmqpConsumer(queue, callback, self)
        consumer.update()

        self.__consumers[id] = consumer
        return AmqpCancellation(consumer)

    def _get_channel(self):
        self.__ensure_connection()
        return self.__connection.channel()

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


class AmqpCancellation(Cancellation):
    def __init__(self, consumer):
        self.__consumer = consumer

    def cancel(self):
        self.__consumer.cancel()


class AmqpConfirmation(Confirmation):
    def __init__(self, channel, delivery_tag):
        self.__channel = channel
        self.__delivery_tag = delivery_tag

    def complete(self):
        self.__channel.basic.ack(self.__delivery_tag)

    def reject(self):
        self.__channel.basic.reject(self.__delivery_tag)


class AmqpConsumer(object):
    def __init__(self, queue, callback, transport):
        self.id = str(uuid.uuid1())
        self.__queue = queue
        self.__callback = callback
        self.__transport = transport
        self.__channel = None
        self.__thread = None

    def cancel(self):
        self.__channel.basic.cancel(self.id)

    def update(self):
        self.__channel = self.__transport._get_channel()
        self.__channel.queue.declare(self.__queue, durable=True)

        self.__channel.basic.qos(1)
        self.__channel.basic.consume(self.__on_message, self.__queue, self.id)

        self.__thread = Thread(target=self.__consume)
        self.__thread.daemon = True
        self.__thread.start()

    def __consume(self):
        self.__channel.start_consuming()

    def __on_message(self, body, channel, method, properties):
        message = self.__to_message(body, channel, method, properties)

        self.__callback(message)

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