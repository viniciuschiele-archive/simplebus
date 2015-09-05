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

from .cancellables import Cancellation, Subscription
from .config import Config
from .compression import CompressMessageStep, DecompressMessageStep, CompressorRegistry
from .dispatchers import DispatchMessageStep
from .faults import MoveFaultsToDeadLetterStep, RetryFaultsStep
from .handlers import InvokeHandlerStep
from .pipeline import Pipeline, IncomingContext, OutgoingContext
from .serialization import DeserializeMessageStep, SerializeMessageStep, SerializerRegistry
from .state import set_current_bus
from .transports import create_transport, get_transport, ReceiveFromTransportStep
from .utils import create_random_id, Loop


class Bus(object):
    """This class provides methods for receiving and sending messages over multiple brokers."""

    def __init__(self, app_id=None):
        self.__app_id = app_id
        self.__queue_options = {}
        self.__topic_options = {}
        self.__started = False
        self.__transports = {}
        self.__queues_cached = {}
        self.__topics_cached = {}
        self.__loop = Loop()

        self.compressors = CompressorRegistry()
        self.serializers = SerializerRegistry()

        self.incoming_pipeline = Pipeline()
        self.incoming_pipeline.add_step(ReceiveFromTransportStep())
        self.incoming_pipeline.add_step(DecompressMessageStep(self.compressors))
        self.incoming_pipeline.add_step(DeserializeMessageStep(self.serializers))
        self.incoming_pipeline.add_step(MoveFaultsToDeadLetterStep())
        self.incoming_pipeline.add_step(RetryFaultsStep())
        self.incoming_pipeline.add_step(InvokeHandlerStep())

        self.outgoing_pipeline = Pipeline()
        self.outgoing_pipeline.add_step(SerializeMessageStep(self.serializers))
        self.outgoing_pipeline.add_step(CompressMessageStep(self.compressors))
        self.outgoing_pipeline.add_step(DispatchMessageStep(self.app_id, self.__transports))

        self.config = Config()

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

    def start(self):
        """Starts the bus."""

        if len(self.config.SIMPLEBUS_ENDPOINTS) == 0:
            raise RuntimeError('SimpleBus must have at least one endpoint')

        for key, endpoint in self.config.SIMPLEBUS_ENDPOINTS.items():
            transport = create_transport(
                endpoint,
                self.config.SIMPLEBUS_RECOVERY,
                self.config.SIMPLEBUS_RECOVERY_MIN_DELAY,
                self.config.SIMPLEBUS_RECOVERY_DELTA_DELAY,
                self.config.SIMPLEBUS_RECOVERY_MAX_DELAY)
            transport.open()
            self.__transports[key] = transport

        self.incoming_pipeline.start()
        self.outgoing_pipeline.start()

        self.__started = True

        set_current_bus(self)

        self.__load_imports()

    def stop(self):
        """Stops the bus."""

        self.__started = False

        self.incoming_pipeline.stop()
        self.outgoing_pipeline.stop()

        for transport in self.__transports.values():
            transport.close()
        self.__transports.clear()

        set_current_bus(None)

    def push(self, queue, message, **options):
        """Sends a message to the specified queue."""

        self.__ensure_started()

        options = self.__get_queue_options(queue, options)
        context = OutgoingContext(queue, False, message, options)
        self.outgoing_pipeline.execute(context)

    def pull(self, queue, callback, **options):
        """Starts receiving messages from the specified queue."""

        self.__ensure_started()

        id = create_random_id()
        options = self.__get_queue_options(queue, options)
        transport = get_transport(self.__transports, options.get('endpoint'))
        transport.pull(id, queue, self.__transport_receiver(callback, options), options)
        return Cancellation(id, transport)

    def publish(self, topic, message, **options):
        """Publishes a message to the specified topic."""

        self.__ensure_started()

        options = self.__get_topic_options(topic, options)
        context = OutgoingContext(topic, True, message, options)
        self.outgoing_pipeline.execute(context)

    def subscribe(self, topic, callback, **options):
        """Subscribes to receive published messages to the specified topic."""

        self.__ensure_started()

        id = create_random_id()
        options = self.__get_topic_options(topic, options)
        transport = get_transport(self.__transports, options.get('endpoint'))
        transport.subscribe(id, topic, self.__transport_receiver(callback, options), options)
        return Subscription(id, transport)

    def __ensure_started(self):
        """If the bus is not started it starts."""
        if not self.is_started:
            self.start()

    def __get_queue_options(self, queue, override_options):
        """Gets the options for the specified queue."""

        return self.__get_options(
            self.__queues_cached,
            queue,
            override_options,
            self.config.SIMPLEBUS_QUEUES)

    def __get_topic_options(self, topic, override_options):
        """Gets the options for the specified topic."""

        return self.__get_options(
            self.__topics_cached,
            topic,
            override_options,
            self.config.SIMPLEBUS_TOPICS)

    @staticmethod
    def __get_options(cache, key, override_options, pool_options):
        """Gets the options for the specified key."""

        options = cache.get(key)

        if not options:
            options = pool_options.get('*').copy()
            options_form_config = pool_options.get(key)

            if options_form_config:
                options.update(options_form_config)

            cache[key] = options

        if override_options:
            options = options.copy()
            options.update(override_options)

        return options

    def __load_imports(self):
        """Loads the modules configured in the configuration object."""

        modules = self.config.SIMPLEBUS_IMPORTS
        if not modules:
            return
        for module in modules:
            __import__(module)

    def __transport_receiver(self, callback, options):
        def wrapper(transport_message):
            context = IncomingContext(transport_message, callback, options)
            self.incoming_pipeline.execute(context)
        return wrapper
