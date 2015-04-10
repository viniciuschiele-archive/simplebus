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

from simplebus.enums import DeliveryMode


class Transport(object):
    def close(self):
        raise NotImplementedError

    def dequeue(self, queue, exchange, callback):
        raise NotImplementedError

    def enqueue(self, queue, exchange, message):
        raise NotImplementedError

    def publish(self, topic, exchange, message):
        raise NotImplementedError

    def subscribe(self, topic, exchange, callback):
        raise NotImplementedError


class Cancellation(object):
    def cancel(self):
        raise NotImplementedError


class Confirmation(object):
    def complete(self):
        raise NotImplementedError

    def reject(self):
        raise NotImplementedError


class Message(object):
    def __init__(self, body, delivery_mode=None, expiration=None, confirmation=None):
        self.body = body
        self.delivery_mode = DeliveryMode.persistent if delivery_mode is None else delivery_mode
        self.expiration = expiration
        self.__confirmation = confirmation

    def complete(self):
        if self.__confirmation:
            self.__confirmation.complete()
            self.__confirmation = None

    def reject(self):
        if self.__confirmation:
            self.__confirmation.reject()
            self.__confirmation = None
