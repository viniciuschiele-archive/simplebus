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

import time
import uuid

from amqpstorm import UriConnection
from simplebus.transports.core import Cancellable
from simplebus.transports.core import Transport
from simplebus.transports.core import Message
from threading import Lock
from threading import Thread


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

    def create_message(self):
        return AmqpMessage()

    def create_queue(self, queue):
        with self.get_channel() as channel:
            channel.queue.declare(queue, durable=True)
            channel.exchange.declare(queue, durable=True)
            channel.queue.bind(queue, queue, '')

    def create_topic(self, topic):
        with self.get_channel() as channel:
            channel.exchange.declare(topic, 'topic', durable=True)

    def cancel(self, id):
        listener = self.__listeners.pop(id)

        if listener:
            listener.cancel()

    def consume(self, queue, dispatcher):
        listener = AmqpListener(self, queue, None, dispatcher)
        listener.consume()
        self.__listeners[listener.id] = listener
        return AmqpCancellable(listener.id, self)

    def send(self, queue, message):
        self.create_queue(queue)

        self.__send_message(queue, '', message)

    def publish(self, topic, message):
        self.create_topic(topic)

        self.__send_message(topic, '', message)

    def subscribe(self, topic, dispatcher):
        listener = AmqpListener(self, None, topic, dispatcher)
        listener.subscribe()
        self.__listeners[listener.id] = listener
        return AmqpCancellable(listener.id, self)

    def get_channel(self):
        if not self.__opened:
            raise RuntimeError('Transport is not opened.')

        self.__ensure_connection()
        return self.__connection.channel()

    def on_exception(self, e):
        if 'shutdown' in str(e):
            self.__start_reconnecting()

    def __ensure_connection(self):
        if self.__connection and self.__connection.is_open:
            return

        with self.__connection_lock:
            if self.__connection and self.__connection.is_open:
                return

            self.__connection = UriConnection(self.__url)
            self.__connection.open()
            self.__refresh_listeners()

    def __send_message(self, exchange, routing_key, message):
        with self.get_channel() as channel:
            channel.confirm_deliveries()
            channel.basic.publish(message.body, routing_key, exchange, properties=message.properties)

    def __start_reconnecting(self):
        with self.__reconnection_lock:
            if self.__reconnecting:
                return
            self.__reconnecting = True

        thread = Thread(target=self.__ensure_connection)
        thread.daemon = True
        thread.start()

    def __reconnect(self):
        while self.__opened and (self.__connection is None or not self.__connection.is_open):
            try:
                self.__ensure_connection()
                self.__reconnecting = False
            except:
                time.sleep(1)

    def __refresh_listeners(self):
        for listener in self.__listeners:
            listener.refresh()


class AmqpCancellable(Cancellable):
    def __init__(self, id, transport):
        self.__id = id
        self.__transport = transport

    def cancel(self):
        self.__transport.cancel(self.__id)


class AmqpMessage(Message):
    DEFAULT_PROPERTIES = {
        'delivery_mode': 2,  # persistent
        'expiration': None
    }

    def __init__(self, body=None, method=None, properties=None, channel=None):
        self.__body = body
        self.__method = method
        self.__properties = properties or self.DEFAULT_PROPERTIES.copy()
        self.__channel = channel

    @property
    def id(self):
        return str.encode(self.__properties.get('message_id'), 'utf-8')

    @id.setter
    def id(self, value):
        self.__properties['message_id'] = value

    @property
    def body(self):
        return self.__body

    @body.setter
    def body(self, value):
        self.__body = value

    @property
    def delivery_count(self):
        headers = self.__properties.get('headers')

        if not headers:
            return 0

        return headers.get(bytes('x-delivery-count', 'utf-8'), 0)

    @delivery_count.setter
    def delivery_count(self, value):
        headers = self.__properties.get('headers')

        if headers is None:
            headers = dict()
            self.__properties['headers'] = headers

        headers[bytes('x-delivery-count', 'utf-8')] = value

    @property
    def expires(self):
        expiration = self.__properties.get('expiration')

        if expiration:
            return int(expiration)

        return None

    @expires.setter
    def expires(self, value):
        if value:
            self.__properties['expiration'] = int(value)

        self.__properties['expiration'] = None

    @property
    def properties(self):
        return self.__properties

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


class AmqpListener(object):
    def __init__(self, transport, queue, topic, dispatcher):
        self.id = str(uuid.uuid4())
        self.__transport = transport
        self.__queue = queue
        self.__topic = topic
        self.__dispatcher = dispatcher
        self.__channel = None

    def cancel(self):
        self.__channel.stop_consuming()
        self.__channel.close()

    def consume(self):
        self.__transport.create_queue(self.__queue)

        self.__channel = self.__transport.get_channel()
        self.__channel.basic.qos(1)
        self.__channel.basic.consume(self.__on_message, self.__queue, self.id)

        thread = Thread(target=self.__start_consuming)
        thread.daemon = True
        thread.start()

    def subscribe(self):
        self.__transport.create_topic(self.__topic)

        queue_name = self.__topic + '-' + self.id

        self.__channel = self.__transport.get_channel()
        self.__channel.queue.declare(queue_name, exclusive=True, auto_delete=True)
        self.__channel.queue.bind(queue_name, self.__topic)
        self.__channel.basic.qos(1)
        self.__channel.basic.consume(self.__on_message, queue_name, self.id)

        thread = Thread(target=self.__start_consuming)
        thread.daemon = True
        thread.start()

    def refresh(self):
        if self.__queue:
            self.consume()
        else:
            self.subscribe()

    def __start_consuming(self):
        try:
            self.__channel.start_consuming()
        except Exception as e:
            self.__transport.on_exception(e)

    def __on_message(self, body, channel, method, properties):
        message = AmqpMessage(body, method, properties, channel)
        message.delivery_count += 1

        self.__dispatcher.dispatch(message)
