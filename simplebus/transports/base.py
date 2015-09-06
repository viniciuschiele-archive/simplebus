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

LOGGER = logging.getLogger(__name__)


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
    def create_sender(self, options):
        pass

    @abstractmethod
    def create_pumper(self, pipeline, address, concurrency, prefetch_count):
        pass

    @abstractmethod
    def create_publisher(self, options):
        pass

    @abstractmethod
    def create_subscriber(self, pipeline, address, concurrency, prefetch_count):
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


class MessageDispatcher(metaclass=ABCMeta):
    @abstractmethod
    def dispatch(self, message):
        pass


class MessageConsumer(metaclass=ABCMeta):
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


class ReceiveFromTransportStep(PipelineStep):
    id = 'ReceiveFromTransport'

    def __init__(self, messages):
        self.__messages = messages

    def execute(self, context, next_step):
        try:
            set_transport_message(context.transport_message)

            context.message_cls = self.__get_best_message_cls(context.transport_message, context.address)
            context.options = self.__messages.get_options(context.message_cls)

            next_step()
        finally:
            set_transport_message(None)

    def __get_best_message_cls(self, transport_message, address):
        if transport_message.type:
            message_cls = self.__messages.get_by_type(transport_message.type)
            if message_cls:
                return message_cls
            raise SimpleBusError('Not found a message class for the type \'%s\'.' % transport_message.type)
        else:
            messages_cls = self.__messages.get_by_address(address)
            if len(messages_cls) == 1:
                return messages_cls[0]

            if not messages_cls:
                raise SimpleBusError('Not found a message class for the address \'%s\'.' % address)
            else:
                raise SimpleBusError('Multiple message classes for the address \'%s\'.' % address)

