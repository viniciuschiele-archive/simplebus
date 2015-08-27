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

from .cancellables import Cancellation
from .cancellables import Subscription
from .config import Config
from .compression import compress
from .dispatchers import DefaultDispatcher
from .handlers import CallbackHandler
from .handlers import MessageHandler
from .serialization import dumps
from .state import set_current_bus
from .transports import create_transport
from .transports.base import TransportMessage
from .utils import create_random_id
from .utils import Loop


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
            raise RuntimeError('SimpleBus should have at least one endpoint')

        for key, endpoint in self.config.SIMPLEBUS_ENDPOINTS.items():
            transport = create_transport(
                endpoint,
                self.config.SIMPLEBUS_RECOVERY,
                self.config.SIMPLEBUS_RECOVERY_MIN_DELAY,
                self.config.SIMPLEBUS_RECOVERY_DELTA_DELAY,
                self.config.SIMPLEBUS_RECOVERY_MAX_DELAY)
            transport.open()
            self.__transports[key] = transport

        self.__started = True

        set_current_bus(self)

        self.__load_imports()

    def stop(self):
        """Stops the bus."""

        self.__started = False

        for transport in self.__transports.values():
            transport.close()
        self.__transports.clear()

        set_current_bus(None)

    def push(self, queue, message, **options):
        """Sends a message to the specified queue."""

        self.__ensure_started()

        options = self.__get_queue_options(queue, options)

        transport_message = self.__create_transport_message(message, **options)

        transport = self.__get_transport(options.get('endpoint'))
        transport.push(queue, transport_message, options)

    def pull(self, queue, callback, **options):
        """Starts receiving messages from the specified queue."""

        self.__ensure_started()

        id = create_random_id()
        options = self.__get_queue_options(queue, options)
        handler = self.__get_handler(callback)
        dispatcher = DefaultDispatcher(handler, options.get('serializer'), options.get('compression'))
        transport = self.__get_transport(options.get('endpoint'))
        transport.pull(id, queue, dispatcher, options)
        return Cancellation(id, transport)

    def publish(self, topic, message, **options):
        """Publishes a message to the specified topic."""

        self.__ensure_started()

        options = self.__get_topic_options(topic, options)

        transport_message = self.__create_transport_message(message, **options)

        transport = self.__get_transport(options.get('endpoint'))
        transport.publish(topic, transport_message, options)

    def subscribe(self, topic, callback, **options):
        """Subscribes to receive published messages to the specified topic."""

        self.__ensure_started()

        id = create_random_id()
        options = self.__get_topic_options(topic, options)
        handler = self.__get_handler(callback)
        dispatcher = DefaultDispatcher(handler, options.get('serializer'), options.get('compression'))
        transport = self.__get_transport(options.get('endpoint'))
        transport.subscribe(id, topic, dispatcher, options)
        return Subscription(id, transport)

    def __create_transport_message(self, message, **options):
        content_type, content_encoding, body = dumps(message, options.get('serializer'))

        transport_message = TransportMessage(self.__app_id,
                                             create_random_id(),
                                             content_type,
                                             content_encoding,
                                             body,
                                             options.get('expiration'))

        compression = options.get('compression')
        if compression:
            transport_message.headers['x-compression'], transport_message.body = \
                compress(body, compression)

        return transport_message

    def __ensure_started(self):
        """If the bus is not started it starts."""
        if not self.is_started:
            self.start()

    @staticmethod
    def __get_handler(callback):
        """Gets a MessageHandler instance for the specified callback."""

        if isinstance(callback, MessageHandler):
            return callback

        if callable(callback):
            return CallbackHandler(callback)

        raise TypeError('Parameter callback must be an instance of MessageHandler or a callable object.')

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

    def __get_transport(self, endpoint):
        """Gets the transport for the specified endpoint."""

        if endpoint is None:
            endpoint = 'default'

        transport = self.__transports.get(endpoint)

        if transport is None:
            raise RuntimeError("Endpoint '%s' not found" % endpoint)

        return transport

    def __load_imports(self):
        """Loads the modules configured in the configuration object."""

        modules = self.config.SIMPLEBUS_IMPORTS
        if not modules:
            return
        for module in modules:
            __import__(module)
