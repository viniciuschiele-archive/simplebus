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

"""
Message Handler is used to process a message received.
"""

from abc import ABCMeta
from abc import abstractmethod


class MessageHandler(metaclass=ABCMeta):
    """Defines a message handler."""

    @abstractmethod
    def handle(self, message):
        """Handles a message."""
        pass


class CallbackHandler(MessageHandler):
    """Message handler that calls a method."""

    def __init__(self, callback):
        self.__callback = callback

    def handle(self, message):
        """Handles a message calling a method."""
        self.__callback(message)
