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

from simplebus import Bus
from simplebus import Config
from simplebus import Consumer
from simplebus import Producer
from threading import Event
from unittest import TestCase


class TestConfig(TestCase):
    def test_default(self):
        config = Config()
        self.assertEqual('amqp://guest:guest@localhost/', config.endpoints.get('default'))

    def test_from_object(self):
        config = Config()
        config.from_object(self.CustomConfig())
        self.assertEqual('amqp://test:test@localhost/', config.endpoints.get('default'))

    class CustomConfig(object):
        SIMPLEBUS = {
            'endpoints': {
                'default': 'amqp://test:test@localhost/'
            }
        }


class TestConsumerRegistry(TestCase):
    def setUp(self):
        self.bus = Bus()
        self.bus.start()

    def tearDown(self):
        self.bus.stop()

    def test_registry(self):
        class Consumer1(Consumer):
            queue = 'tests.queue1'

            def handle(self_, message):
                pass

        consumer = Consumer1()

        self.bus.consumers.register(consumer)
        self.assertEqual(1, self.bus.consumers.length)

        self.bus.consumers.remove(consumer)
        self.assertEqual(0, self.bus.consumers.length)


class TestConsumer(TestCase):
    queue = 'tests.queue1'
    topic = 'tests.topic1'

    def setUp(self):
        self.bus = Bus()
        self.bus.start()

    def tearDown(self):
        self.bus.stop()

    def test_consumer_with_queue(self):
        event = Event()

        class Consumer1(Consumer):
            queue = self.queue

            def handle(self_, message):
                self.assertEqual('hello', message)
                event.set()

        self.bus.consumers.register(Consumer1())
        self.bus.send(self.queue, 'hello')

        event.wait()

    def test_consumer_with_queue_and_decorator(self):
        event = Event()

        @self.bus.consumer
        class Consumer1(Consumer):
            queue = self.queue

            def handle(self_, message):
                self.assertEqual('hello', message)
                event.set()

        self.bus.send(self.queue, 'hello')

        event.wait()

    def test_consumer_with_topic(self):
        event = Event()

        class Consumer1(Consumer):
            topic = self.topic

            def handle(self_, message):
                self.assertEqual('hello', message)
                event.set()

        self.bus.consumers.register(Consumer1())
        self.bus.publish(self.topic, 'hello')

        event.wait()

    def test_consumer_with_topic_and_decorator(self):
        event = Event()

        @self.bus.consumer
        class Consumer1(Consumer):
            topic = self.topic

            def handle(self_, message):
                self.assertEqual('hello', message)
                event.set()

        self.bus.publish(self.topic, 'hello')

        event.wait()


class TestProducer(TestCase):
    queue = 'tests.queue1'

    def setUp(self):
        self.bus = Bus()
        self.bus.start()

    def tearDown(self):
        self.bus.stop()

    def test_publish(self):
        event = Event()

        @self.bus.consumer
        class Consumer1(Consumer):
            queue = self.queue

            def handle(self_, message):
                self.assertEqual('hello', message)
                event.set()

        @self.bus.producer
        class Producer1(Producer):
            queue = self.queue

        producer = Producer1()
        producer.publish('hello')

        event.wait()
