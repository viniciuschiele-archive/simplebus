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
from .pipeline import PipelineStep
from .serialization import loads
from .state import set_transport_message
from .transports.base import TransportMessage
from .utils import create_random_id


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

            transport_message.delete()
        finally:
            set_transport_message(None)


class DispatchMessageStep(PipelineStep):
    def __init__(self, transports):
        self.__transports = transports

    def invoke(self, context, next_step):
        transport_message = TransportMessage(context.app_id,
                                             create_random_id(),
                                             context.content_type,
                                             context.content_encoding,
                                             context.message,
                                             context.options.get('expiration'))

        if context.headers:
            transport_message.headers.update(context.headers)

        transport = self.__get_transport(context.options.get('endpoint'))

        if context.queue:
            transport.push(context.queue, transport_message, context.options)
        else:
            transport.publish(context.topic, transport_message, context.options)

        next_step()

    def __get_transport(self, endpoint):
        """Gets the transport for the specified endpoint."""

        if endpoint is None:
            endpoint = 'default'

        transport = self.__transports.get(endpoint)

        if transport is None:
            raise RuntimeError("Endpoint '%s' not found" % endpoint)

        return transport
