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

"""Base classes to implement a transport."""


import logging

from abc import ABCMeta, abstractmethod
from ..errors import SimpleBusError
from ..pipeline import PipelineStep
from ..state import set_transport_message
from ..utils import create_random_id, import_string

LOGGER = logging.getLogger(__name__)


TRANSPORT_ALIASES = {
    'amqp': 'simplebus.transports.amqp.Transport',
}


def create_transport(url):
    if '://' not in url:
        raise ValueError('Invalid url %s.' % url)

    schema = url.partition('://')[0]

    class_name = TRANSPORT_ALIASES.get(schema)

    if class_name is None:
        raise ValueError('Invalid schema %s.' % schema)

    transport_cls = import_string(class_name)

    return transport_cls(url)


def get_transport(transports, endpoint):
    """Gets the transport for the specified endpoint."""

    if endpoint is None:
        endpoint = 'default'

    transport = transports.get(endpoint)

    if transport is None:
        raise RuntimeError("Transport '%s' not found" % endpoint)

    return transport


class Transport(metaclass=ABCMeta):
    """
    Main base class to a transport.
    This class is used to establish a connection to the broker and
    send/receive messages.
    """
    @property
    @abstractmethod
    def is_open(self):
        """Gets the value that indicates whether the transport is open."""
        pass

    @abstractmethod
    def open(self):
        """Opens the connection to the broker."""
        pass

    @abstractmethod
    def close(self):
        """Closes the connection to the broker."""
        pass

    @abstractmethod
    def create_queue_publisher(self, destination):
        pass

    @abstractmethod
    def create_queue_purger(self, destination):
        pass

    @abstractmethod
    def create_queue_subscriber(self, pipeline, destination, concurrency, prefetch_count):
        pass

    @abstractmethod
    def create_topic_publisher(self, destination):
        pass

    @abstractmethod
    def create_topic_subscriber(self, pipeline, destination, concurrency, prefetch_count):
        pass


class TransportMessage(object):
    """An envelope used by SimpleBus to package messages for transmission."""

    def __init__(self, app_id=None, message_id=None, content_type=None, content_encoding=None,
                 body=None, expiration=None, type=None, headers=None):
        self.app_id = app_id
        self.message_id = message_id
        self.content_type = content_type
        self.content_encoding = content_encoding
        self.body = body
        self.expiration = expiration
        self.type = type
        self.headers = headers or {}


class MessagePublisher(metaclass=ABCMeta):
    @abstractmethod
    def publish(self, message):
        pass


class MessageSubscriber(metaclass=ABCMeta):
    @property
    @abstractmethod
    def is_started(self):
        pass

    @abstractmethod
    def start(self):
        pass

    @abstractmethod
    def stop(self):
        pass


class MessagePurger(metaclass=ABCMeta):
    @abstractmethod
    def purge(self):
        pass


class ReceiveFromTransportStep(PipelineStep):
    id = 'ReceiveFromTransport'

    def __init__(self, messages):
        self.__messages = messages

    def execute(self, context, next_step):
        try:
            set_transport_message(context.transport_message)

            context.message_def = self.__get_best_message_def(context.transport_message, context.destination)

            next_step()
        finally:
            set_transport_message(None)

    def __get_best_message_def(self, transport_message, destination):
        if transport_message.type:
            message_def = self.__messages.get_by_name(transport_message.type)
            if message_def:
                return message_def
            raise SimpleBusError('Not found a message class for the type \'%s\'.' % transport_message.type)
        else:
            message_defs = self.__messages.get_by_destination(destination)
            if len(message_defs) == 1:
                return message_defs[0]

            if message_defs:
                raise SimpleBusError('Multiple message classes for the destination \'%s\'.' % destination)

            raise SimpleBusError('Not found a message class for the destination \'%s\'.' % destination)


class SendToTransportStep(PipelineStep):
    id = 'SendToTransport'

    def __init__(self, app_id, transports):
        self.__app_id = app_id
        self.__transports = transports

    def execute(self, context, next_step):
        message_def = context.message_def

        transport_message = TransportMessage()
        transport_message.app_id = self.__app_id
        transport_message.message_id = create_random_id()
        transport_message.expiration = message_def.expires
        transport_message.content_type = context.content_type
        transport_message.content_encoding = context.content_encoding
        transport_message.body = context.body
        transport_message.type = message_def.message_name

        transport = get_transport(self.__transports, message_def.endpoint)

        if message_def.is_command():
            publisher = transport.create_queue_publisher(message_def.destination)
        else:
            publisher = transport.create_topic_publisher(message_def.destination)

        publisher.publish(transport_message)

        next_step()
