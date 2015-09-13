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

"""AMQP transport implementation."""


try:
    import amqpstorm
except ImportError:
    amqpstorm = None

import logging
import time
import uuid

from amqpstorm.queue import Queue
from threading import Lock, Thread
from urllib import parse
from ..pipeline import IncomingContext
from ..pools import ResourcePool
from ..transports import base

LOGGER = logging.getLogger(__name__)


class Transport(base.Transport):
    def __init__(self, url):
        if amqpstorm is None:
            raise ImportError('Missing amqp-storm library (pip install amqp-storm==1.2.5)')

        self.__connection = None
        self.__connection_lock = Lock()
        self.__channel_pool = None
        self.__min_channels = 5
        self.__max_channels = 20
        self.__reconnect_min_delay = 10
        self.__reconnect_delta_exponent = 2
        self.__reconnect_max_delay = 10000
        self.__close_lock = Lock()
        self.__closed_by_user = None
        self.__url = url
        self.__subscribers = []

        self.__load_url_parameters(url)

    @property
    def min_channels(self):
        return self.__min_channels

    @property
    def max_channels(self):
        return self.__max_channels

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
        self.__close(True)

    def create_queue_publisher(self, address):
        return QueuePublisher(address, self)

    def create_queue_purger(self, address):
        return QueuePurger(address, self)

    def create_queue_subscriber(self, pipeline, address, concurrency, prefetch_count):
        subscriber = QueueSubscriber(self, pipeline, address, concurrency, prefetch_count)
        self.__subscribers.append(subscriber)
        return subscriber

    def create_topic_publisher(self, address):
        return TopicPublisher(address, self)

    def create_topic_subscriber(self, pipeline, address, concurrency, prefetch_count):
        subscriber = TopicSubscriber(self, pipeline, address, concurrency, prefetch_count)
        self.__subscribers.append(subscriber)
        return subscriber

    def create_transport_message(self, message):
        transport_message = base.TransportMessage()
        transport_message.app_id = message.app_id
        transport_message.message_id = message.message_id
        transport_message.content_type = message.content_type
        transport_message.content_encoding = message.content_encoding
        transport_message.body = message._body
        transport_message.headers.update(message.properties.get('headers'))

        expiration = message.properties.get('expiration')
        if expiration:
            transport_message.expiration = int(expiration)

        return transport_message

    def create_channel(self):
        self.__ensure_connection()
        return self.__connection.channel()

    def acquire_channel(self):
        self.__ensure_connection()
        return self.__channel_pool.acquire()

    def release_channel(self, channel):
        self.__channel_pool.release(channel)

    def send_message(self, exchange, routing_key, message, confirm_delivery, mandatory):
        properties = {
            'app_id': message.app_id,
            'message_id': message.message_id,
            'content_type': message.content_type,
            'content_encoding': message.content_encoding,
            'delivery_mode': 2,
            'headers': message.headers
        }

        if message.expiration:
            properties['expiration'] = str(message.expiration)

        channel = self.__channel_pool.acquire()
        try:
            if confirm_delivery:
                channel.confirm_deliveries()
            msg = amqpstorm.Message.create(channel, message.body, properties)
            msg.publish(routing_key, exchange, mandatory)
        finally:
            self.__channel_pool.release(channel)

    def __close(self, by_user):
        self.__closed_by_user = by_user

        if self.__channel_pool:
            self.__channel_pool.close()
            self.__channel_pool = None

        if self.__connection:
            self.__connection.close()
            self.__connection = None

        if not by_user:
            self.__start_reconnect()

    def __ensure_connection(self):
        if self.is_open:
            return

        if not self.__connection_lock.acquire(timeout=0.1):
            raise ConnectionError('Transport is closed.')

        try:
            if self.is_open:
                return

            self.__connection = amqpstorm.UriConnection(self.__url)
            self.__closed_by_user = None
            self.__channel_pool = ChannelPool(self.__min_channels, self.__max_channels, self.__connection)
        finally:
            self.__connection_lock.release()

    def __load_url_parameters(self, url):
        uri = parse.urlparse(url)
        params = parse.parse_qs(uri.query)

        min_channels = params.get('min_channels')
        if min_channels:
            self.__min_channels = int(min_channels[0])

        max_channels = params.get('max_channels')
        if max_channels:
            self.__max_channels = int(max_channels[0])

        reconnect_min_delay = params.get('reconnect_min_delay')
        if reconnect_min_delay:
            self.__reconnect_min_delay = int(reconnect_min_delay[0])

        reconnect_delta_exponent = params.get('reconnect_delta_exponent')
        if reconnect_delta_exponent:
            self.__reconnect_delta_exponent = int(reconnect_delta_exponent[0])

        reconnect_max_delay = params.get('reconnect_max_delay')
        if reconnect_max_delay:
            self.__reconnect_max_delay = int(reconnect_max_delay[0])

    def __on_consumer_error(self):
        # if the transport was closed by the user this error should have occurred
        # because the connection was closed, so ignore it.
        if self.__closed_by_user:
            return

        if self.__close_lock.acquire(False):
            try:
                if self.__closed_by_user is not None:
                    return
                # whatever the error closes the connection.
                LOGGER.critical('Connection to the broker went down.', exc_info=True)

                self.__close(False)
            finally:
                self.__close_lock.release()

    def __start_reconnect(self):
        """Starts the reconnection to the broker."""

        thread = Thread(target=self.__reconnect)
        thread.daemon = True
        thread.start()

    def __reconnect(self):
        """Tries to reconnect to the broker."""

        count = 1
        delay = self.__reconnect_min_delay

        while not self.is_open and not self.__closed_by_user:
            time.sleep(delay)

            try:
                LOGGER.warn('Attempt %s to reconnect to the RabbitMQ.' % count)
                self.open()
                LOGGER.info('Connection re-established to the broker.')

                for consumer in self.__consumers:
                    try:
                        if consumer.is_started:
                            consumer.stop()
                            consumer.start()
                    except:
                        LOGGER.critical('Subscribe failed for the address %s.' % consumer.address, exc_info=True)

            except:
                count += 1
                delay = max(delay * self.__reconnect_delta_exponent, self.__reconnect_max_delay)


class QueuePublisher(base.MessagePublisher):
    def __init__(self, address, transport):
        self.__address = address
        self.__transport = transport

    def publish(self, message):
        for i in range(2):
            try:
                self.__transport.send_message('', self.__address, message, True, True)
                break
            except Exception as e:
                if i == 1 or 'NO_ROUTE' not in str(e):
                    raise
                self.__create_queue()

    def __create_queue(self):
        channel = self.__transport.acquire_channel()
        try:
            channel.queue.declare(self.__address, durable=True)
        finally:
            self.__transport.release_channel(channel)


class QueuePurger(base.MessagePurger):
    def __init__(self, address, transport):
        self.__address = address
        self.__transport = transport

    def purge(self):
        channel = self.__transport.create_channel()

        try:
            Queue(channel).purge(self.__address)
        finally:
            channel.close()


class QueueSubscriber(base.MessageSubscriber):
    def __init__(self, transport, pipeline, address, concurrency, prefetch_count):
        self.__transport = transport
        self.__pipeline = pipeline
        self.__channels = []
        self.__is_started = False

        self.address = address
        self.concurrency = concurrency
        self.prefetch_count = prefetch_count

    @property
    def is_started(self):
        return self.__is_started

    def start(self):
        for i in range(self.concurrency):
            channel = self.__transport.create_channel()
            channel.queue.declare(self.address, durable=True)
            channel.basic.qos(self.prefetch_count)
            channel.basic.consume(self.__on_message, self.address)
            self.__channels.append(channel)

            thread = Thread(target=self.__start_receiving, args=(channel,))
            thread.daemon = True
            thread.start()

        self.__is_started = True

    def stop(self):
        for channel in self.__channels:
            channel.close()

        self.__channels.clear()
        self.__is_started = False

    def __start_receiving(self, channel):
        try:
            channel.start_consuming(to_tuple=False)
        except:
            self.__transport.__on_consumer_error()

    def __on_message(self, message):
        try:
            transport_message = self.__transport.create_transport_message(message)

            self.__pipeline.execute(IncomingContext(transport_message, self.address))

            message.ack()
        except:
            LOGGER.exception('RabbitMQ receive operation failed. Address: ' + self.address)

            message.reject(True)


class TopicPublisher(base.MessagePublisher):
    def __init__(self, address, transport):
        self.__address = address
        self.__transport = transport

    def publish(self, message):
        self.__transport.send_message(self.__address, '', message, False, True)


class TopicSubscriber(base.MessageSubscriber):
    def __init__(self, transport, pipeline, address, concurrency, prefetch_count):
        self.__transport = transport
        self.__pipeline = pipeline
        self.__queue = address + ':' + str(uuid.uuid4()).replace('-', '')
        self.__channels = []
        self.__is_started = False

        self.address = address
        self.concurrency = concurrency
        self.prefetch_count = prefetch_count


    @property
    def is_started(self):
        return self.__is_started

    def start(self):
        for i in range(self.concurrency):
            channel = self.__transport.create_channel()
            channel.exchange.declare(self.address, 'topic', durable=True)
            channel.queue.declare(self.__queue, auto_delete=True)
            channel.queue.bind(self.__queue, self.address)
            channel.basic.consume(self.__on_message, self.__queue)
            self.__channels.append(channel)

            thread = Thread(target=self.__start_receiving, args=(channel,))
            thread.daemon = True
            thread.start()

        self.__is_started = True

    def stop(self):
        for channel in self.__channels:
            channel.close()

        self.__channels.clear()
        self.__is_started = False

    def __start_receiving(self, channel):
        try:
            channel.start_consuming(to_tuple=False)
        except:
            self.__transport.__on_consumer_error()

    def __on_message(self, message):
        try:
            transport_message = self.__transport.create_transport_message(message)

            self.__pipeline.execute(IncomingContext(transport_message, self.address))

            message.ack()
        except:
            LOGGER.exception('RabbitMQ receive operation failed. Address: ' + self.address)

            message.reject(True)


class ChannelPool(ResourcePool):
    """Provides a pool of channels."""

    def __init__(self, min_size, max_size, connection):
        super().__init__(min_size, max_size)
        self.__connection = connection

    def _create_resource(self):
        """Creates a new channel."""
        return self.__connection.channel()

    def _close_resource(self, resource):
        """Close the specified channel."""
        resource.close()

    def _validate_resource(self, resource):
        """Validates whether channel is open."""
        return resource.is_open
