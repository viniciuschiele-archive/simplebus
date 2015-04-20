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

import logging
import simplejson

from abc import ABCMeta
from abc import abstractmethod
from simplebus.state import set_current_message


LOGGER = logging.getLogger(__name__)


class MessageDispatcher(metaclass=ABCMeta):
    def __call__(self, *args, **kwargs):
        self.dispatch(*args)

    @abstractmethod
    def dispatch(self, message):
        pass


class ConsumerDispatcher(MessageDispatcher):
    def __init__(self, queue, handler, max_delivery_count):
        self.__queue = queue
        self.__handler = handler
        self.__max_delivery_count = max_delivery_count

    def dispatch(self, message):
        if message.delivery_count > self.__max_delivery_count:
            message.complete()

        content = simplejson.loads(message.body)

        set_current_message(message)

        try:
            self.__handler.handle(content)
        except:
            LOGGER.exception("Error processing the message '%s' from the queue '%s'." % (message.id, self.__queue))
            message.defer()
        else:
            message.complete()

        set_current_message(None)


class SubscriberDispatcher(MessageDispatcher):
    def __init__(self, topic, handler):
        self.__topic = topic
        self.__handler = handler

    def dispatch(self, message):
        content = simplejson.loads(message.body)

        set_current_message(message)

        try:
            self.__handler.handle(content)
        except:
            LOGGER.exception("Error processing the message '%s' from the topic '%s'." % (message.id, self.__topic))

        message.complete()

        set_current_message(None)
