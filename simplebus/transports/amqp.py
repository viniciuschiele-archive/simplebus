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
from simplebus.enums import DeliveryMode
from simplebus.transports.core import Cancellation
from simplebus.transports.core import Transport
from simplebus.transports.core import Confirmation
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
        self.__consumers = {}
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

    def create_queue(self, queue):
        with self.get_channel() as channel:
            channel.queue.declare(queue, durable=True)
            channel.exchange.declare(queue, durable=True)
            channel.queue.bind(queue, queue, '')

    def create_topic(self, topic):
        with self.get_channel() as channel:
            channel.exchange.declare(topic, 'topic', durable=True)

    def send_queue(self, queue, message):
        self.create_queue(queue)

        self.__send_message(queue, '', message)

    def send_topic(self, topic, message):
        self.create_topic(topic)

        self.__send_message(topic, '', message)

    def subscribe_queue(self, queue, dispatcher):
        consumer = AmqpConsumer(self, queue, None, dispatcher)
        consumer.subscribe()
        self.__consumers[consumer.id] = consumer
        return AmqpCancellation(self, consumer.id)

    def subscribe_topic(self, topic, dispatcher):
        consumer = AmqpConsumer(self, None, topic, dispatcher)
        consumer.subscribe()
        self.__consumers[consumer.id] = consumer
        return AmqpCancellation(self, consumer.id)

    def unsubscribe(self, id):
        consumer = self.__consumers.get(id)

        if consumer:
            consumer.unsubscribe()

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
            self.__resubscribe()

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

    def __resubscribe(self):
        for consumer in self.__consumers:
            consumer.subscribe()

    def __send_message(self, exchange, routing_key, message):
        properties = {
            'app_id': None,
            'delivery_mode': DeliveryMode.persistent if message.delivery_mode is None else message.delivery_mode.value,
            'expiration': None if message.expiration is None else str(message.expiration),
            'message_id': message.id,
        }

        with self.get_channel() as channel:
            channel.confirm_deliveries()
            channel.basic.publish(message.body, routing_key, exchange, properties=properties)


class AmqpCancellation(Cancellation):
    def __init__(self, transport, id):
        self.__transport = transport
        self.__id = id

    def cancel(self):
        self.__transport.unsubscribe(self.__id)


class AmqpConfirmation(Confirmation):
    def __init__(self, body, channel, method, properties):
        self.__body = body
        self.__channel = channel
        self.__method = method
        self.__properties = properties

    def complete(self):
        self.__channel.basic.ack(self.__method.get('delivery_tag'))

    def defer(self):
        self.__channel.basic.publish(self.__body,
                                     str(self.__method.get('routing_key'), encoding='utf-8'),
                                     str(self.__method.get('exchange'), encoding='utf-8'),
                                     self.__properties)

        self.complete()


class AmqpConsumer(object):
    def __init__(self, transport, queue, topic, dispatcher):
        self.id = str(uuid.uuid4())
        self.__transport = transport
        self.__queue = queue
        self.__topic = topic
        self.__dispatcher = dispatcher
        self.__channel = None

    def subscribe(self):
        if self.__queue:
            self.__subscribe_queue()
        else:
            self.__subscribe_topic()

    def unsubscribe(self):
        self.__channel.stop_consuming()
        self.__channel.close()

    def __subscribe_queue(self):
        self.__transport.create_queue(self.__queue)

        self.__channel = self.__transport.get_channel()
        self.__channel.basic.qos(1)
        self.__channel.basic.consume(self.__on_message, self.__queue, self.id)

        thread = Thread(target=self.__start_consuming)
        thread.daemon = True
        thread.start()

    def __subscribe_topic(self):
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

    def __start_consuming(self):
        try:
            self.__channel.start_consuming()
        except Exception as e:
            self.__transport.on_exception(e)

    def __on_message(self, body, ch, method, properties):
        message = to_message(body, ch, method, properties)
        self.__dispatcher.dispatch(message)


def to_message(body, channel, method, properties):
    def get_property(name, default, convert=None):
        if name in properties:
            value = properties[name]
            if value is not None and value != '' and convert:
                return convert(value)
            return value

        return default

    confirmation = AmqpConfirmation(body, channel, method, properties)

    return Message(
        str(body, encoding='utf-8'),
        get_property('delivery_mode', DeliveryMode.persistent, lambda x: DeliveryMode(x)),
        get_property('expiration', None, lambda x: int(x)),
        confirmation)
