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
from .compression import decompress
from .errors import CompressionError
from .errors import CompressionNotFoundError
from .errors import NoRetryError
from .errors import SerializationError
from .errors import SerializerNotFoundError
from .serialization import loads
from .state import set_transport_message


LOGGER = logging.getLogger(__name__)


class MessageDispatcher(metaclass=ABCMeta):
    """Base class for the dispatchers."""
    def __call__(self, *args, **kwargs):
        self.dispatch(*args, **kwargs)

    @abstractmethod
    def dispatch(self, message):
        """Dispatches the message."""
        pass


class DefaultDispatcher(MessageDispatcher):
    """
    Dispatcher responsible for dispatching the message to the message handler.
    """

    def __init__(self,
                 handler,
                 serializer,
                 compression):
        self._handler = handler
        self._serializer = serializer
        self._compression = compression

    def dispatch(self, transport_message):
        """Dispatches the message."""

        set_transport_message(transport_message)

        try:
            compression = self._compression or transport_message.headers.get('x-compression')
            if compression:
                transport_message.body = decompress(transport_message.body, compression)

            message = loads(
                transport_message.body,
                transport_message.content_type,
                transport_message.content_encoding,
                self._serializer)

            self._handler.handle(message)
        finally:
            set_transport_message(None)


class PullerDispatcher(DefaultDispatcher):
    """
    Dispatcher responsible for dispatching the message to the message handler.
    """
    def __init__(self,
                 queue,
                 handler,
                 serializer,
                 compression):
        super().__init__(handler, serializer, compression)
        self.__queue = queue

    def dispatch(self, transport_message):
        """Dispatches the message."""

        try:
            super().dispatch(transport_message)
        except (CompressionError, CompressionNotFoundError,
                NoRetryError, SerializerNotFoundError, SerializationError) as e:
            transport_message.dead_letter(str(e))
            LOGGER.exception(str(e))
        except:
            LOGGER.exception("Message dispatch failed. message id: '%s', queue: '%s'." %
                             (transport_message.message_id, self.__queue))
            transport_message.retry()
        else:
            transport_message.delete()


class SubscriberDispatcher(DefaultDispatcher):
    """
    Dispatcher responsible for dispatching the message to the message handler.
    """

    def __init__(self, topic,
                 handler,
                 serializer,
                 compression):
        super().__init__(handler, serializer, compression)
        self.__topic = topic

    def dispatch(self, transport_message):
        """Dispatches the message."""

        try:
            super().dispatch(transport_message)
        except:
            LOGGER.exception(
                "Message dispatch failed. message id: '%s', topic: '%s'." %
                (transport_message.message_id, self.__topic))
        finally:
            transport_message.delete()
