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

import uuid
from simplebus.enums import DeliveryMode


class Transport(object):
    def open(self):
        raise NotImplementedError

    def close(self):
        raise NotImplementedError

    def send_queue(self, queue, message):
        raise NotImplementedError

    def send_topic(self, topic, message):
        raise NotImplementedError

    def subscribe_queue(self, queue, dispatcher):
        raise NotImplementedError

    def subscribe_topic(self, topic, dispatcher):
        raise NotImplementedError


class Confirmation(object):
    def complete(self):
        raise NotImplementedError

    def defer(self):
        raise NotImplementedError


class Cancellation(object):
    def cancel(self):
        raise NotImplementedError


class Message(object):
    def __init__(self, body, delivery_mode=None, expiration=None, confirmation=None):
        self.id = str(uuid.uuid4())
        self.body = body
        self.delivery_mode = DeliveryMode.persistent if delivery_mode is None else delivery_mode
        self.expiration = expiration
        self.__confirmation = confirmation

    def complete(self):
        if self.__confirmation:
            self.__confirmation.complete()
            self.__confirmation = None

    def defer(self):
        if self.__confirmation:
            self.__confirmation.defer()
            self.__confirmation = None


class MessageDispatcher(object):
    def dispatch(self, message):
        raise NotImplementedError