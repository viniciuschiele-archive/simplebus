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
from simplebus.transports import create_transport
from simplebus.transports.core import Message


class Bus(object):
    def __init__(self):
        self.__consumers = []
        self.__started = False
        self.__transport = None
        self.config = Config()

    def dequeue(self, queue, callback=None, exchange=None):
        self.__check_started()
        if callback:
            return self.__transport.dequeue(
                queue,
                exchange,
                lambda message: self.__dispatch_message(message, callback))

        def decorator(func):
            self.dequeue(queue, func, exchange)
            return func
        return decorator

    def enqueue(self, queue, content, delivery_mode=None, expiration=None, exchange=None):
        self.__check_started()
        body = simplejson.dumps(content)
        msg = Message(body, delivery_mode, expiration)
        self.__transport.enqueue(queue, exchange, msg)

    def publish(self, topic, content, delivery_mode=None, expiration=None, exchange=None):
        self.__check_started()
        body = simplejson.dumps(content)
        msg = Message(body, delivery_mode, expiration)
        self.__transport.publish(topic, exchange, msg)

    def subscribe(self, topic, callback=None, exchange=None):
        self.__check_started()
        if callback:
            return self.__transport.subscribe(
                topic,
                exchange,
                lambda message: self.__dispatch_message(message, callback))

        def decorator(func):
            self.subscribe(topic, func, exchange)
            return func
        return decorator

    def start(self):
        self.__started = True
        self.__transport = create_transport(self.config.BROKER_URL)

    def stop(self):
        if self.__transport:
            self.__transport.close()
            self.__transport = None

        self.__started = False

    @staticmethod
    def __dispatch_message(message, callback):
        try:
            obj = simplejson.loads(message.body)
            callback(obj)
            message.complete()
        except Exception:
            message.reject()

    def __check_started(self):
        if not self.__started:
            raise RuntimeError('Bus must be started first.')
