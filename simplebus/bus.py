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

"""Message bus implementation."""

from .config import Config
from .compression import CompressMessageStep, DecompressMessageStep, CompressorRegistry
from .errors import SimpleBusError
from .faults import MoveFaultsToDeadLetterStep
from .handlers import InvokeHandlerStep
from .messages import MessageRegistry, COMMAND_MESSAGE_TYPE, EVENT_MESSAGE_TYPE
from .pipeline import Pipeline, OutgoingContext
from .serialization import DeserializeMessageStep, SerializeMessageStep, SerializerRegistry
from .state import set_current_bus
from .transports import create_transport, get_transport, ReceiveFromTransportStep, SendToTransportStep
from .utils import Loop


class SimpleBus(object):
    """This class provides methods for receiving and sending messages over multiple brokers."""

    def __init__(self, app_id=None):
        self.__app_id = app_id
        self.__handlers = {}
        self.__transports = {}
        self.__cached_options = {}
        self.__loop = Loop()
        self.__started = False

        self.config = Config()

        self.compressors = CompressorRegistry()
        self.serializers = SerializerRegistry()
        self.messages = MessageRegistry(self)

        self.incoming_pipeline = Pipeline()
        self.incoming_pipeline.add_step(ReceiveFromTransportStep(self.messages))
        self.incoming_pipeline.add_step(DecompressMessageStep(self.compressors))
        self.incoming_pipeline.add_step(DeserializeMessageStep(self))
        self.incoming_pipeline.add_step(MoveFaultsToDeadLetterStep(self.__transports))
        self.incoming_pipeline.add_step(InvokeHandlerStep(self.__handlers))

        self.outgoing_pipeline = Pipeline()
        self.outgoing_pipeline.add_step(SerializeMessageStep(self.serializers))
        self.outgoing_pipeline.add_step(CompressMessageStep(self.compressors))
        self.outgoing_pipeline.add_step(SendToTransportStep(self.app_id, self.__transports))

    @property
    def app_id(self):
        """Gets the application id."""
        return self.__app_id

    @property
    def is_started(self):
        """Gets the value whether the bus is started."""
        return self.__started

    @property
    def loop(self):
        return self.__loop

    def add_handler(self, message_cls, f):
        if self.__handlers.get(message_cls):
            raise SimpleBusError('Message \'%s\' already has a handler.' % str(message_cls))
        self.__handlers[message_cls] = f

    def command(self, name=None, **options):
        def decorator(cls):
            self.messages.add(cls, COMMAND_MESSAGE_TYPE, name, **options)
            return cls
        return decorator

    def event(self, name=None, **options):
        def decorator(cls):
            self.messages.add(cls, EVENT_MESSAGE_TYPE, name, **options)
            return cls
        return decorator

    def handle(self, message_cls):
        def decorator(f):
            self.add_handler(message_cls, f)
        return decorator

    def publish(self, message):
        """Publishes a message to the specified topic."""
        self.__ensure_started()

        message_def = self.messages.get_by_cls(type(message))
        context = OutgoingContext(message, message_def)
        self.outgoing_pipeline.execute(context)

    def subscribe(self, message_cls):
        """Subscribes to receive published messages to the specified topic."""
        self.__ensure_started()

        message_def = self.messages.get_by_cls(message_cls)

        transport = get_transport(self.__transports, message_def.endpoint)

        if message_def.purge_on_subscribe and message_def.is_command():
            purger = transport.create_queue_purger(message_def.destination)
            purger.purge()

        if message_def.is_command():
            subscriber = transport.create_queue_subscriber(self.incoming_pipeline, message_def.destination,
                                                           message_def.concurrency, message_def.prefetch_count)
        else:
            subscriber = transport.create_topic_subscriber(self.incoming_pipeline, message_def.destination,
                                                           message_def.concurrency, message_def.prefetch_count)

        subscriber.start()
        return subscriber

    def start(self):
        """Starts the bus."""
        if self.is_started:
            return

        if len(self.config.SIMPLEBUS_ENDPOINTS) == 0:
            raise RuntimeError('SimpleBus must have at least one endpoint')

        for key, endpoint in self.config.SIMPLEBUS_ENDPOINTS.items():
            transport = create_transport(endpoint)
            transport.open()
            self.__transports[key] = transport

        self.incoming_pipeline.start()
        self.outgoing_pipeline.start()

        self.__started = True

        set_current_bus(self)

        self.__load_imports()

        if self.config.SIMPLEBUS_AUTO_SUBSCRIBE:
            self.__auto_subscribe()

    def stop(self):
        """Stops the bus."""
        if not self.__started:
            return

        self.__started = False

        self.incoming_pipeline.stop()
        self.outgoing_pipeline.stop()

        for transport in self.__transports.values():
            transport.close()
        self.__transports.clear()

        set_current_bus(None)

    def __auto_subscribe(self):
        for message_cls in self.__handlers:
            self.subscribe(message_cls)

    def __ensure_started(self):
        """If the bus is not started it starts."""
        if not self.is_started:
            self.start()

    def __load_imports(self):
        """Loads the modules configured in the configuration object."""

        modules = self.config.SIMPLEBUS_IMPORTS
        if not modules:
            return
        for module in modules:
            __import__(module)
