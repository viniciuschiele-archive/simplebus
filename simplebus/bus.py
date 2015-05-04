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

import inspect
import simplejson

from simplebus.config import Config
from simplebus.cancellables import Cancellation
from simplebus.cancellables import Subscription
from simplebus.dispatchers import PullerDispatcher
from simplebus.dispatchers import SubscriberDispatcher
from simplebus.handlers import CallbackHandler
from simplebus.handlers import MessageHandler
from simplebus.state import set_current_bus
from simplebus.transports import create_transport
from simplebus.transports.base import TransportMessage
from simplebus.utils import create_random_id


class Bus(object):
    def __init__(self, app_id=None):
        self.__app_id = app_id
        self.__cancellations = []
        self.__queue_options = {}
        self.__topic_options = {}
        self.__started = False
        self.__transports = {}
        self.config = Config()

    @property
    def is_started(self):
        return self.__started

    def start(self):
        if len(self.config.SIMPLEBUS_ENDPOINTS) == 0:
            raise RuntimeError('SimpleBus must have at least one endpoint')

        for key, endpoint in self.config.SIMPLEBUS_ENDPOINTS.items():
            transport = create_transport(
                endpoint,
                self.config.SIMPLEBUS_RECOVERY,
                self.config.SIMPLEBUS_RECOVERY_DELAY)
            transport.open()
            self.__transports[key] = transport

        self.__started = True

        set_current_bus(self)

    def stop(self):
        self.__started = False

        for cancellation in self.__cancellations:
            cancellation.cancel()
        self.__cancellations.clear()

        for transport in self.__transports.values():
            transport.close()
        self.__transports.clear()

        set_current_bus(None)

    def push(self, queue, message, **options):
        self.__ensure_started()

        options = self.config.get_queue_options(queue, options)
        transport = self.__get_transport(options.get('endpoint'))
        transport_message = TransportMessage(self.__app_id,
                                             create_random_id(),
                                             simplejson.dumps(message),
                                             options.get('expiration'))
        transport.push(queue, transport_message, options)

    def pull(self, queue, callback, **options):
        self.__ensure_started()

        id = create_random_id()
        handler = self.__get_handler(callback)
        dispatcher = PullerDispatcher(queue, handler)
        options = self.config.get_queue_options(queue, options)
        transport = self.__get_transport(options.get('endpoint'))
        transport.pull(id, queue, dispatcher, options)
        return Cancellation(id, transport)

    def publish(self, topic, message, **options):
        self.__ensure_started()

        options = self.config.get_topic_options(topic, options)
        transport = self.__get_transport(options.get('endpoint'))
        transport_message = TransportMessage(self.__app_id,
                                             create_random_id(),
                                             simplejson.dumps(message),
                                             options.get('expiration'))
        transport.publish(topic, transport_message, options)

    def subscribe(self, topic, callback, **options):
        self.__ensure_started()

        id = create_random_id()
        handler = self.__get_handler(callback)
        dispatcher = SubscriberDispatcher(topic, handler)
        options = self.config.get_topic_options(topic, options)
        transport = self.__get_transport(options.get('endpoint'))
        transport.subscribe(id, topic, dispatcher, options)
        return Subscription(id, transport)

    def __ensure_started(self):
        if not self.is_started:
            self.start()

    @staticmethod
    def __get_handler(callback):
        if isinstance(callback, MessageHandler):
            return callback

        if inspect.isfunction(callback):
            return CallbackHandler(callback)

        raise TypeError('Parameter handler must be an instance of MessageHandler or a function.')

    def __get_transport(self, endpoint):
        if endpoint is None:
            endpoint = 'default'

        transport = self.__transports.get(endpoint)

        if transport is None:
            raise RuntimeError("Endpoint '%s' not found" % endpoint)

        return transport
