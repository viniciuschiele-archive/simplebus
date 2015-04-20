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
import time
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
        self.__reconnection_lock = Lock()
        self.__opened = False
        self.__reconnecting = False
        self.__listeners = {}
        self.__url = url

    def open(self):
        if self.__opened:
            return

        self.__opened = True
        self.__reconnecting = False
        self.__ensure_connection()

    def close(self):
        if not self.__opened:
            return

        self.__opened = False
        self.__connection.close()
        self.__connection = None

    def cancel(self, id):
        listener = self.__listeners.pop(id)

        if listener:
            listener.close_channel()

    def consume(self, queue, callback):
        listener = ListenerData(queue, None, callback)
        self.__listeners[listener.id] = listener
        self.__consume(listener)
        return AmqpCancellable(listener.id, self)

    def send(self, queue, message):
        self.__create_queue(queue)

        self.__send_message(queue, '', message)

    def publish(self, topic, message):
        self.__create_topic(topic)

        self.__send_message(topic, '', message)

    def subscribe(self, topic, callback):
        listener = ListenerData(None, topic, callback)
        self.__listeners[listener.id] = listener
        self.__subscribe(listener)
        return AmqpCancellable(listener.id, self)

    def __create_queue(self, queue):
        with self.__get_channel() as channel:
            channel.queue.declare(queue, durable=True)
            channel.exchange.declare(queue, durable=True)
            channel.queue.bind(queue, queue, '')

    def __create_topic(self, topic):
        with self.__get_channel() as channel:
            channel.exchange.declare(topic, 'topic', durable=True)

    def __consume(self, listener):
        def on_message(body, channel, method, properties):
            message = self.__to_message(body, channel, method, properties)
            listener.callback(message)

        self.__create_queue(listener.queue)

        listener.close_channel()

        listener.channel = self.__get_channel()
        listener.channel.basic.qos(1)
        listener.channel.basic.consume(on_message, listener.queue, listener.id)

        thread = Thread(target=self.__start_receiving, args=(listener,))
        thread.daemon = True
        thread.start()

    def __get_channel(self):
        if not self.__opened:
            raise RuntimeError('Transport is not opened.')

        self.__ensure_connection()
        return self.__connection.channel()

    def __subscribe(self, listener):
        def on_message(body, channel, method, properties):
            message = self.__to_message(body, channel, method, properties)
            listener.callback(message)

        self.__create_topic(listener.topic)

        queue_name = listener.topic + '-' + listener.id

        listener.close_channel()

        listener.channel = self.__get_channel()
        listener.channel.queue.declare(queue_name, exclusive=True, auto_delete=True)
        listener.channel.queue.bind(queue_name, listener.topic)
        listener.channel.basic.qos(1)
        listener.channel.basic.consume(on_message, queue_name, listener.id)

        thread = Thread(target=self.__start_receiving, args=(listener,))
        thread.daemon = True
        thread.start()

    def __ensure_connection(self):
        if self.__connection and self.__connection.is_open:
            return

        with self.__connection_lock:
            if self.__connection and self.__connection.is_open:
                return

            self.__connection = UriConnection(self.__url)
            self.__connection.open()

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

    def __reconnect(self):
        count = 1
        while self.__opened and (self.__connection is None or not self.__connection.is_open):
            try:
                LOGGER.warn('Attempt %s to reconnect to the broker.' % count)
                self.__ensure_connection()
                self.__refresh_listeners()
                self.__reconnecting = False
                LOGGER.info('Connection re-established to the broker.')
            except:
                delay = count

                if delay > 10:
                    delay = 10

                time.sleep(delay)
                count += 1

    def __refresh_listeners(self):
        for listener in self.__listeners.values():
            try:
                if listener.queue:
                    self.__consume(listener)
                else:
                    self.__subscribe(listener)
            except:
                if listener.queue:
                    LOGGER.critical('Fail listening to the queue %s.' % listener.queue, exc_info=True)
                else:
                    LOGGER.critical('Fail listening to the topic %s.' % listener.topic, exc_info=True)

    def __start_receiving(self, listener):
        try:
            listener.channel.start_consuming()
        except Exception:
            # if 'shutdown' in str(e):
            self.__start_reconnecting()

    def __start_reconnecting(self):
        with self.__reconnection_lock:
            if self.__reconnecting:
                return
            self.__reconnecting = True
            LOGGER.critical('Connection to the broker is down.', exc_info=True)

        thread = Thread(target=self.__reconnect)
        thread.daemon = True
        thread.start()

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
    def __init__(self, id, transport):
        self.__id = id
        self.__transport = transport

    def cancel(self):
        self.__transport.cancel(self.__id)


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


class ListenerData(object):
    def __init__(self, queue, topic, callback):
        self.id = str(uuid.uuid4())
        self.queue = queue
        self.topic = topic
        self.callback = callback
        self.channel = None

    def close_channel(self):
        try:
            if self.channel:
                self.channel.close()
                self.channel = None
        except:
            pass

