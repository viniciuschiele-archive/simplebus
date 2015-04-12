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

import simplejson

from simplebus.config import Config
from simplebus.consumers import ConsumerRegistry
from simplebus.enums import DeliveryMode
from simplebus.transports import create_transport
from simplebus.transports.core import Message


class Bus(object):
    def __init__(self):
        self.__consumers = ConsumerRegistry(self)
        self.__started = False
        self.__transports = {}
        self.config = Config()

    @property
    def consumers(self):
        return self.__consumers

    @property
    def is_started(self):
        return self.__started

    def consumer(self, consumer_cls):
        self.consumers.register(consumer_cls())
        return consumer_cls

    def producer(self, producer_cls):
        producer_cls.bus = self
        return producer_cls

    def send(self, queue, message, expiration=None, endpoint=None):
        self.__check_started()

        body = simplejson.dumps(message)
        msg = Message(body, DeliveryMode.persistent, expiration)

        transport = self._get_transport(endpoint)
        transport.send_queue(queue, msg)

    def publish(self, topic, message, expiration=None, endpoint=None):
        self.__check_started()

        body = simplejson.dumps(message)
        msg = Message(body, DeliveryMode.persistent, expiration)

        transport = self._get_transport(endpoint)
        transport.send_topic(topic, msg)

    def start(self):
        self.__started = True

        for endpoint in self.config.endpoints.items():
            transport = create_transport(endpoint[1])
            transport.open()
            self.__transports[endpoint[0]] = transport

        self.__consumers._start()

    def stop(self):
        for transport in self.__transports.values():
            transport.close()

        self.__transports.clear()

        self.__started = False

    def _get_transport(self, endpoint):
        if endpoint is None:
            endpoint = 'default'

        transport = self.__transports.get(endpoint)

        if transport is None:
            raise RuntimeError("Endpoint '%s' not found" % endpoint)

        return transport

    def __check_started(self):
        if not self.__started:
            raise RuntimeError('Bus must be started first.')
