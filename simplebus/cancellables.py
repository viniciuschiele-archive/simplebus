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

"""Classes used to cancel the receiving messages from the broker."""


from abc import ABCMeta
from abc import abstractmethod


class Cancellable(metaclass=ABCMeta):
    """Interface that provides methods to cancel the receipt of messages."""

    @abstractmethod
    def cancel(self):
        """Cancels the receipt of messages."""
        pass


class Cancellation(Cancellable):
    """Class responsible for canceling the receipt of messages from queues."""

    def __init__(self, id, transport):
        self.__id = id
        self.__transport = transport

    def cancel(self):
        """Cancels the receipt of messages."""

        self.__transport.cancel(self.__id)


class Subscription(Cancellable):
    """Class responsible for canceling the receipt of messages from topics."""

    def __init__(self, id, transport):
        self.__id = id
        self.__transport = transport

    def cancel(self):
        """Cancels the receipt of messages."""

        self.__transport.unsubscribe(self.__id)