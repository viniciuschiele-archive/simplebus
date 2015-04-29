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

import inspect
import simplejson
import uuid

from simplebus.config import Config
from simplebus.cancellables import Cancellation
from simplebus.cancellables import Subscription
from simplebus.dispatchers import PullerDispatcher
from simplebus.dispatchers import SubscriberDispatcher
from simplebus.handlers import CallbackHandler
from simplebus.handlers import MessageHandler
from simplebus.state import set_current_bus
from simplebus.transports import create_transport
from simplebus.transports.core import Message


class Bus(object):
    def __init__(self):
        self.__cancellations = []
        self.__started = False
        self.__transports = {}
        self.config = Config()

    @property
    def is_started(self):
        return self.__started

    def start(self):
        if len(self.config.endpoints) == 0:
            raise RuntimeError('SimpleBus must have at least one endpoint')

        for endpoint in self.config.endpoints.items():
            transport = create_transport(endpoint[1])
            transport.open()
            self.__transports[endpoint[0]] = transport

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

    def push(self, queue, message, expires=None, endpoint=None):
        self.__ensure_started()

        transport = self.__get_transport(endpoint)

        msg = Message(
            id=self.__create_message_id(),
            body=simplejson.dumps(message),
            expires=expires)

        transport.push(queue, msg)

    def pull(self, queue, callback, max_delivery_count=3, endpoint=None):
        self.__ensure_started()

        id = str(uuid.uuid4())
        handler = self.__get_handler(callback)
        transport = self.__get_transport(endpoint)
        dispatcher = PullerDispatcher(queue, handler, max_delivery_count)
        transport.pull(id, queue, dispatcher)
        return Cancellation(id, transport)

    def publish(self, topic, message, endpoint=None):
        self.__ensure_started()

        transport = self.__get_transport(endpoint)

        msg = Message(
            id=self.__create_message_id(),
            body=simplejson.dumps(message))

        transport.publish(topic, msg)

    def subscribe(self, topic, callback, endpoint=None):
        self.__ensure_started()

        id = str(uuid.uuid4())
        handler = self.__get_handler(callback)
        transport = self.__get_transport(endpoint)
        dispatcher = SubscriberDispatcher(topic, handler)
        transport.subscribe(id, topic, dispatcher)
        return Subscription(id, transport)

    @staticmethod
    def __create_message_id():
        return str(uuid.uuid4()).replace('-', '')

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
