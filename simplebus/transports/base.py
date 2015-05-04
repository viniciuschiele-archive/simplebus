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

from abc import ABCMeta
from abc import abstractmethod
from simplebus.utils import EventHandler
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
    def cancel(self, id):
        pass

    @abstractmethod
    def push(self, queue, message, options):
        pass

    @abstractmethod
    def pull(self, id, queue, callback, options):
        pass

    @abstractmethod
    def publish(self, topic, message, options):
        pass

    @abstractmethod
    def subscribe(self, id, topic, callback, options):
        pass

    @abstractmethod
    def unsubscribe(self, id):
        pass


class TransportMessage(object):
    def __init__(self, app_id=None, message_id=None, body=None, expiration=None):
        self._app_id = app_id
        self._message_id = message_id
        self._body = body
        self._expiration = expiration
        self._retry_count = 0

    @property
    def app_id(self):
        return self._app_id

    @app_id.setter
    def app_id(self, value):
        self._app_id = value

    @property
    def message_id(self):
        return self._message_id

    @message_id.setter
    def message_id(self, value):
        self._message_id = value

    @property
    def body(self):
        return self._body

    @body.setter
    def body(self, value):
        self._body = value

    @property
    def expiration(self):
        return self._expiration

    @expiration.setter
    def expiration(self, value):
        self._expiration = value

    @property
    def retry_count(self):
        return self._retry_count

    def delete(self):
        pass

    def dead_letter(self, reason):
        pass

    def retry(self):
        pass


class RecoveryAwareTransport(Transport):
    def __init__(self, transport, recovery_delay):
        self.__is_open = False
        self.__cancellations = {}
        self.__subscriptions = {}
        self.__recovery_delay = recovery_delay
        self.__transport = transport
        self.__transport.closed += self.__on_closed
        self.closed = EventHandler()

    @property
    def is_open(self):
        return self.__is_open

    def close(self):
        self.__transport.close()
        self.__is_open = False

    def open(self):
        self.__transport.open()
        self.__is_open = True

    def cancel(self, id):
        cancellation = self.__cancellations.pop(id)
        if cancellation:
            self.__transport.cancel(id)

    def push(self, queue, message, options):
        self.__transport.push(queue, message, options)

    def pull(self, id, queue, callback, options):
        self.__cancellations[id] = dict(id=id, queue=queue, callback=callback, options=options)
        self.__transport.pull(id, queue, callback, options)

    def publish(self, topic, message, options):
        self.__transport.publish(topic, message, options)

    def subscribe(self, id, topic, callback, options):
        self.__subscriptions[id] = dict(id=id, topic=topic, callback=callback, options=options)
        self.__transport.subscribe(id, topic, callback, options)

    def unsubscribe(self, id):
        subscriber = self.__subscriptions.pop(id)
        if subscriber:
            self.__transport.unsubscribe(id)

    def __on_closed(self):
        self.closed()

        self.__start_recovery()

    def __recover(self):
        count = 1
        while self.is_open and not self.__transport.is_open:
            try:
                LOGGER.warn('Attempt %s to reconnect to the broker.' % count)
                self.__transport.open()
                self.__recover_cancellations()
                self.__recover_subscriptions()
                LOGGER.info('Connection re-established to the broker.')
            except:
                time.sleep(self.__recovery_delay)
                count += 1

    def __recover_cancellations(self):
        for cancellation in self.__cancellations.values():
            id = cancellation.get('id')
            queue = cancellation.get('queue')
            callback = cancellation.get('callback')
            options = cancellation.get('options')

            try:
                self.pull(id, queue, callback, options)
            except:
                LOGGER.critical('Fail pulling the queue %s.' % queue, exc_info=True)

    def __recover_subscriptions(self):
        for subscription in self.__subscriptions.values():
            id = subscription.get('id')
            topic = subscription.get('topic')
            callback = subscription.get('callback')
            options = subscription.get('options')

            try:
                self.subscribe(id, topic, callback, options)
            except:
                LOGGER.critical('Fail subscribing the topic %s.' % topic, exc_info=True)

    def __start_recovery(self):
        LOGGER.critical('Connection to the broker is down.', exc_info=True)

        thread = Thread(target=self.__recover)
        thread.daemon = True
        thread.start()
