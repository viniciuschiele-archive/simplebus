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

from abc import ABCMeta
from abc import abstractmethod


class Transport(metaclass=ABCMeta):
    @abstractmethod
    def open(self):
        pass

    @abstractmethod
    def close(self):
        pass

    @abstractmethod
    def send(self, queue, message):
        pass

    @abstractmethod
    def publish(self, topic, message):
        pass

    @abstractmethod
    def consume(self, queue, dispatcher):
        pass

    @abstractmethod
    def subscribe(self, topic, dispatcher):
        pass


class Cancellable(metaclass=ABCMeta):
    @abstractmethod
    def cancel(self):
        pass


class Confirmation(metaclass=ABCMeta):
    @abstractmethod
    def complete(self):
        raise NotImplementedError

    @abstractmethod
    def defer(self):
        raise NotImplementedError


class Dispatcher(metaclass=ABCMeta):
    @abstractmethod
    def dispatch(self, message):
        pass


class Message(object):
    def __init__(self, id=None, body=None, delivery_count=None, expires=None, confirmation=None):
        self.id = id
        self.body = body
        self.delivery_count = 0 if delivery_count is None else delivery_count
        self.expires = expires
        self.__confirmation = confirmation

    def complete(self):
        if self.__confirmation:
            self.__confirmation.complete()

    def defer(self):
        if self.__confirmation:
            self.__confirmation.defer()
