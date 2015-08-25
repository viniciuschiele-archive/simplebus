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
import uuid

from threading import Lock
from threading import Thread
from urllib import parse
from ..pools import ResourcePool
from ..transports import base
from ..utils import EventHandler

LOGGER = logging.getLogger(__name__)


class Transport(base.Transport):
    def __init__(self, url):
        if amqpstorm is None:
            raise ImportError('Missing amqp-storm library (pip install amqp-storm==1.2.3)')

        self.__connection = None
        self.__connection_lock = Lock()
        self.__channels = None
        self.__min_channels = 5
        self.__max_channels = 20
        self.__close_lock = Lock()
        self.__closed_by_user = None
        self.__url = url
        self.__pullers = {}
        self.__subscribers = {}
        self.closed = EventHandler()

        self.__load_channel_limits(url)

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

    def cancel(self, id):
        puller = self.__pullers.pop(id, None)
        if puller:
            puller.error -= self.__on_consumer_error
            puller.stop()

    def push(self, queue, message, options):
        self.__ensure_connection()
        self.__send_message('', queue, message, True)

    def pull(self, id, queue, callback, options):
        self.__ensure_connection()

        puller = Puller(self.__connection, queue, callback, options)
        puller.error += self.__on_consumer_error
        puller.start()
        self.__pullers[id] = puller

    def publish(self, topic, message, options):
        self.__ensure_connection()
        self.__send_message(topic, '', message, False)

    def subscribe(self, id, topic, callback, options):
        self.__ensure_connection()

        subscriber = Subscriber(self.__connection, topic, callback, options)
        subscriber.error += self.__on_consumer_error
        subscriber.start()
        self.__subscribers[id] = subscriber

    def unsubscribe(self, id):
        subscriber = self.__subscribers.pop(id, None)
        if subscriber:
            subscriber.error -= self.__on_consumer_error
            subscriber.stop()

    def __close(self, by_user):
        self.__closed_by_user = by_user

        for id in dict.fromkeys(self.__pullers):
            self.cancel(id)

        for id in dict.fromkeys(self.__subscribers):
            self.unsubscribe(id)

        if self.__channels:
            self.__channels.close()
            self.__channels = None

        if self.__connection:
            self.__connection.close()
            self.__connection = None

        if self.closed:
            self.closed(by_user)

    def __ensure_connection(self):
        if self.is_open:
            return

        if not self.__connection_lock.acquire(timeout=0.1):
            raise ConnectionError('Transport is closed.')

        try:
            if self.is_open:
                return

            self.__connection = amqpstorm.UriConnection(self.__url)
            self.__connection.open()
            self.__closed_by_user = None
            self.__channels = ChannelPool(self.__min_channels, self.__max_channels, self.__connection)
        finally:
            self.__connection_lock.release()

    def __on_consumer_error(self, consumer, e):
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

    def __load_channel_limits(self, url):
        uri = parse.urlparse(url)
        params = parse.parse_qs(uri.query)

        min_channels = params.get('min_channels')
        if min_channels:
            self.__min_channels = int(min_channels[0])

        max_channels = params.get('max_channels')
        if max_channels:
            self.__max_channels = int(max_channels[0])

    def __send_message(self, exchange, routing_key, message, mandatory):
        properties = {
            'app_id': message.app_id,
            'message_id': message.message_id,
            'content_type': message.content_type,
            'content_encoding': message.content_encoding,
            'delivery_mode': 2
        }

        if message.expiration:
            properties['expiration'] = str(message.expiration)

        headers = message.headers.copy()

        if message.retry_count > 0:
            headers['x-retry-count'] = message.retry_count

        properties['headers'] = headers

        channel = self.__channels.acquire()
        try:
            channel.confirm_deliveries()
            msg = amqpstorm.Message.create(channel, message.body, properties)
            msg.publish(routing_key, exchange, mandatory)
        finally:
            self.__channels.release(channel)


class TransportMessage(base.TransportMessage):
    def __init__(self, message, dead_letter_queue=None, retry_queue=None):
        super().__init__()

        self.__message = message
        self.__dead_letter_queue = dead_letter_queue
        self.__retry_queue = retry_queue

        self.app_id = message.app_id
        self.message_id = message.message_id
        self.content_type = message.content_type
        self.content_encoding = message.content_encoding
        self.body = message._body
        self.headers.update(message.properties.get('headers'))

        self._retry_count = self.headers.pop('x-retry-count', 0)

        expiration = message.properties.get('expiration')
        if expiration:
            self.expiration = int(expiration)

    def delete(self):
        if self.__message:
            self.__message.ack()
            self.__message = None

    def dead_letter(self, reason):
        if not self.__message:
            return

        self.__message.ack()

        if not self.__dead_letter_queue:
            return

        self.__set_header_retry_count(0)
        self.__set_header_death_reason(reason)

        self.__message.publish(self.__dead_letter_queue, '')
        self.__message = None

    def retry(self):
        if not self.__message:
            return

        self.__set_header_retry_count(self.retry_count + 1)

        if self.__retry_queue:
            routing_key = self.__retry_queue
            exchange = ''
        else:
            method = self.__message.method
            routing_key = method.get('routing_key')
            exchange = method.get('exchange')

        self.__message.ack()
        self.__message.publish(routing_key, exchange)
        self.__message = None

    def __set_header_death_reason(self, reason):
        headers = self.__message._properties.get('headers')
        headers[bytes('x-death-reason', 'utf-8')] = reason

    def __set_header_retry_count(self, retry_count):
        headers = self.__message._properties.get('headers')
        headers[bytes('x-retry-count', 'utf-8')] = retry_count


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


class Puller(object):
    def __init__(self, connection, queue, callback, options):
        self.__connection = connection
        self.__queue = queue
        self.__callback = callback
        self.__channels = []

        self.__dead_letter = options.get('dead_letter')
        self.__retry = options.get('retry')
        self.__max_retries = options.get('max_retries')
        self.__retry_delay = options.get('retry_delay')
        self.__max_concurrency = options.get('max_concurrency')
        self.__prefetch_count = options.get('prefetch_count')

        self.__dead_letter_queue = None
        self.__retry_queue = None

        if self.__dead_letter:
            self.__dead_letter_queue = queue + '.error'

        if self.__retry:
            self.__retry_queue = queue
            if self.__retry_delay > 0:
                self.__retry_queue += '.retry'

        self.error = EventHandler()

    def start(self):
        if self.__dead_letter:
            self.__create_dead_letter_queue(self.__dead_letter_queue)

        if self.__retry and self.__retry_delay > 0:
            self.__create_retry_queue(self.__queue, self.__retry_queue, self.__retry_delay)

        for i in range(self.__max_concurrency):
            channel = self.__connection.channel()
            channel.queue.declare(self.__queue, durable=True)
            channel.basic.qos(self.__prefetch_count)
            channel.basic.consume(self.__on_message, self.__queue)
            self.__channels.append(channel)

            thread = Thread(target=self.__start_receiving, args=(channel,))
            thread.daemon = True
            thread.start()

    def stop(self):
        for channel in self.__channels:
            channel.close()

        self.__channels.clear()

    def __on_message(self, message):
        try:
            transport_message = TransportMessage(message, self.__dead_letter_queue, self.__retry_queue)

            if self.__retry and transport_message.retry_count > self.__max_retries:
                transport_message.dead_letter('Max retries exceeded.')
            else:
                self.__callback(transport_message)
        except:
            LOGGER.exception("Puller failed, queue '%s'." % self.__queue)

    def __create_dead_letter_queue(self, dead_letter_queue):
        with self.__connection.channel() as channel:
            channel.queue.declare(dead_letter_queue, durable=True)

    def __create_retry_queue(self, queue, retry_queue, retry_delay):
        args = {
            'x-dead-letter-exchange': '',
            'x-dead-letter-routing-key': queue,
            'x-message-ttl': retry_delay}

        try:
            with self.__connection.channel() as channel:
                channel.queue.declare(retry_queue, durable=True, arguments=args)
        except amqpstorm.AMQPChannelError as e:
            if 'x-message-ttl' not in str(e):  # already exists a queue with ttl
                raise

    def __start_receiving(self, channel):
        try:
            channel.start_consuming(to_tuple=False)
        except Exception as e:
            self.error(self, e)


class Subscriber(object):
    def __init__(self, connection, topic, callback, options):
        self.__connection = connection
        self.__topic = topic
        self.__callback = callback
        self.__channels = []

        self.__queue = topic + ':' + str(uuid.uuid4()).replace('-', '')
        self.__max_concurrency = options.get('max_concurrency')
        self.__prefetch_count = options.get('prefetch_count')

        self.error = EventHandler()

    def start(self):
        for i in range(self.__max_concurrency):
            channel = self.__connection.channel()
            channel.exchange.declare(self.__topic, 'topic', durable=True)
            channel.queue.declare(self.__queue, auto_delete=True)
            channel.queue.bind(self.__queue, self.__topic)
            channel.basic.consume(self.__on_message, self.__queue)
            self.__channels.append(channel)

            thread = Thread(target=self.__start_receiving, args=(channel,))
            thread.daemon = True
            thread.start()

    def stop(self):
        for channel in self.__channels:
            channel.close()

        self.__channels.clear()

    def __on_message(self, message):
        try:
            transport_message = TransportMessage(message)
            self.__callback(transport_message)
        except:
            LOGGER.exception("Subscriber failed, topic: '%s'." % self.__topic)

    def __start_receiving(self, channel):
        try:
            channel.start_consuming(to_tuple=False)
        except Exception as e:
            self.error(self, e)
