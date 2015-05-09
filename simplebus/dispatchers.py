# Copyright 2015 Vinicius Chiele. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Dispatchers are used to process all messages received and
dispatch them to the message handlers.
"""


import logging

from abc import ABCMeta
from abc import abstractmethod
from simplebus.exceptions import SerializationError
from simplebus.exceptions import SerializerNotFoundError
from simplebus.state import set_transport_message


LOGGER = logging.getLogger(__name__)


class MessageDispatcher(metaclass=ABCMeta):
    """Base class for the dispatchers."""
    def __call__(self, *args, **kwargs):
        self.dispatch(*args)

    @abstractmethod
    def dispatch(self, message):
        """Dispatches the message."""
        pass


class PullerDispatcher(MessageDispatcher):
    """
    Dispatcher responsible for receiving messages from the queue and
    send them to the message handler.
    """
    def __init__(self, queue, handler, serializer_registry, serializer):
        self.__queue = queue
        self.__handler = handler
        self.__serializer_registry = serializer_registry
        self.__serializer = serializer

    def dispatch(self, transport_message):
        """Dispatches the message."""

        set_transport_message(transport_message)

        try:
            message = self.__serializer_registry.deserialize(
                transport_message.body,
                transport_message.content_type,
                transport_message.content_encoding,
                self.__serializer)

            self.__handler.handle(message)
        except (SerializerNotFoundError, SerializationError) as e:
            transport_message.error(e.message)
            LOGGER.exception(e.message)
        except:
            LOGGER.exception("Error processing message '%s' from the queue '%s'." %
                             (transport_message.message_id, self.__queue))
            transport_message.retry()
        else:
            transport_message.delete()

        set_transport_message(None)


class SubscriberDispatcher(MessageDispatcher):
    """
    Dispatcher responsible for receiving messages from the topic and
    send them to the message handler.
    """

    def __init__(self, topic, handler, serializer_registry, serializer):
        self.__topic = topic
        self.__handler = handler
        self.__serializer_registry = serializer_registry
        self.__serializer = serializer

    def dispatch(self, transport_message):
        """Dispatches the message."""

        set_transport_message(transport_message)

        try:
            message = self.__serializer_registry.deserialize(
                transport_message.body,
                transport_message.content_type,
                transport_message.content_encoding,
                self.__serializer)

            self.__handler.handle(message)
        except:
            LOGGER.exception(
                "Error processing the message '%s' from the topic '%s'." %
                (transport_message.message_id, self.__topic))

        transport_message.delete()

        set_transport_message(None)
