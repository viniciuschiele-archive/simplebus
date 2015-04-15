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

from simplebus.dispatchers import DefaultDispatcher


class Consumer(object):
    endpoint = None
    queue = None
    topic = None
    max_delivery_count = 3

    def handle(self, message):
        raise NotImplementedError


class ConsumerRegistry(object):
    def __init__(self, bus):
        self.__consumers = []
        self.__bus = bus

    def register(self, consumer):
        if not consumer.queue and not consumer.topic:
            raise RuntimeError('Consumer must have Queue or Topic.')

        self.__consumers.append(consumer)

        if self.__bus.is_started:
            self.__subscribe(consumer)

    def remove(self, consumer):
        self.__consumers.remove(consumer)

        if self.__bus.is_started:
            self.__unsubscribe(consumer)

    @property
    def length(self):
        return len(self.__consumers)

    def _start(self):
        for consumer in self.__consumers:
            self.__subscribe(consumer)

    def __subscribe(self, consumer):
        transport = self.__bus._get_transport(consumer.endpoint)
        dispatcher = DefaultDispatcher(consumer)

        if consumer.queue:
            subscription = transport.subscribe_queue(
                consumer.queue,
                dispatcher)
        else:
            subscription = transport.subscribe_topic(
                consumer.topic,
                dispatcher)

        setattr(consumer, '__cancellation', subscription)

    def __unsubscribe(self, consumer):
        if hasattr(consumer, '__cancellation'):
            cancellation = getattr(consumer, '__cancellation')
            cancellation.cancel()
