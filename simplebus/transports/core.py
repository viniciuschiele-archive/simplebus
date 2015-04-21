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
import time
import uuid

from abc import ABCMeta
from abc import abstractmethod
from threading import Thread


LOGGER = logging.getLogger(__name__)


class Transport(metaclass=ABCMeta):
    closed = None

    @property
    @abstractmethod
    def is_open(self):
        pass

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
    def consume(self, queue, callback):
        pass

    @abstractmethod
    def subscribe(self, topic, callback):
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


class Message(object):
    def __init__(self, id=None, body=None, delivery_count=0, expires=None, confirmation=None):
        self.id = id
        self.body = body
        self.delivery_count = delivery_count
        self.expires = expires
        self.__confirmation = confirmation

    def complete(self):
        if self.__confirmation:
            self.__confirmation.complete()
            self.__confirmation = None

    def defer(self):
        if self.__confirmation:
            self.__confirmation.defer()
            self.__confirmation = None


class AlwaysOpenTransport(Transport):
    def __init__(self, transport):
        self.__is_open = False
        self.__listeners = {}
        self.__transport = transport
        self.__transport.closed = self.__on_closed

    @property
    def is_open(self):
        return self.__is_open

    def close(self):
        self.__transport.close()
        self.__is_open = False

    def open(self):
        self.__transport.open()
        self.__is_open = True

    def send(self, queue, message):
        self.__transport.send(queue, message)

    def publish(self, topic, message):
        self.__transport.publish(topic, message)

    def consume(self, queue, callback):
        listener = self._Listener(queue, None, callback, AlwaysOpenCancellable())
        self.__consume(listener)
        self.__listeners[listener.id] = listener
        return listener.cancellable

    def subscribe(self, topic, callback):
        listener = self._Listener(None, topic, callback, AlwaysOpenCancellable())
        self.__subscribe(listener)
        self.__listeners[listener.id] = listener
        return listener.cancellable

    def __consume(self, listener, silent=False):
        try:
            listener.cancellable._cancellable = self.__transport.consume(listener.queue, listener.callback)
        except:
            LOGGER.critical('Fail consuming the queue %s.' % listener.queue, exc_info=True)

            if not silent:
                raise

    def __subscribe(self, listener, silent=False):
        try:
            listener.cancellable._cancellable = self.__transport.subscribe(listener.topic, listener.callback)
        except:
            LOGGER.critical('Fail subscribing the topic %s.' % listener.topic, exc_info=True)

            if not silent:
                raise

    def __on_closed(self):
        if self.closed:
            self.close()

        self.__start_reconnecting()

    def __reopen(self):
        count = 1
        while self.is_open and not self.__transport.is_open:
            try:
                LOGGER.warn('Attempt %s to reconnect to the broker.' % count)
                self.__transport.open()
                self.__start_listeners()
                LOGGER.info('Connection re-established to the broker.')
            except:
                delay = count

                if delay > 10:
                    delay = 10

                time.sleep(delay)
                count += 1

    def __start_reconnecting(self):
        LOGGER.critical('Connection to the broker is down.', exc_info=True)

        thread = Thread(target=self.__reopen)
        thread.daemon = True
        thread.start()

    def __start_listeners(self):
        for listener in self.__listeners.values():
            if listener.queue:
                self.__consume(listener, silent=True)
            else:
                self.__subscribe(listener, silent=True)

    class _Listener(object):
        def __init__(self, queue, topic, callback, cancellable):
            self.id = str(uuid.uuid4())
            self.queue = queue
            self.topic = topic
            self.callback = callback
            self.cancellable = cancellable


class AlwaysOpenCancellable(Cancellable):
    def __init__(self):
        self._cancellable = None

    def cancel(self):
        self._cancellable.cancel()
