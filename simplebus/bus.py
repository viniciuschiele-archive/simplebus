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
from simplebus.dispatchers import ConsumerDispatcher
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

    def consume(self, queue, handler, max_delivery_count=3, endpoint=None):
        self.__check_started()

        handler = self.__get_handler(handler)
        transport = self.__get_transport(endpoint)
        dispatcher = ConsumerDispatcher(queue, handler, max_delivery_count)
        cancellation = transport.consume(queue, dispatcher)
        self.__cancellations.append(cancellation)

        return cancellation

    def send(self, queue, message, expires=None, endpoint=None):
        self.__check_started()

        transport = self.__get_transport(endpoint)

        msg = Message(
            id=str(uuid.uuid4()),
            body=simplejson.dumps(message),
            expires=expires)

        transport.send(queue, msg)

    def subscribe(self, topic, handler, endpoint=None):
        self.__check_started()

        handler = self.__get_handler(handler)
        transport = self.__get_transport(endpoint)
        dispatcher = SubscriberDispatcher(topic, handler)
        cancellation = transport.subscribe(topic, dispatcher)
        self.__cancellations.append(cancellation)

        return cancellation

    def publish(self, topic, message, expires=None, endpoint=None):
        self.__check_started()

        transport = self.__get_transport(endpoint)

        msg = Message(id=str(uuid.uuid4()),
                      body=simplejson.dumps(message),
                      expires=expires)

        transport.publish(topic, msg)

    def __check_started(self):
        if not self.is_started:
            raise RuntimeError('Bus must be started.')

    @staticmethod
    def __get_handler(handler):
        if isinstance(handler, MessageHandler):
            return handler

        if inspect.isfunction(handler):
            return CallbackHandler(handler)

        raise TypeError('Parameter handler must be a MessageHandler class or a method.')

    def __get_transport(self, endpoint):
        if endpoint is None:
            endpoint = 'default'

        transport = self.__transports.get(endpoint)

        if transport is None:
            raise RuntimeError("Endpoint '%s' not found" % endpoint)

        return transport
