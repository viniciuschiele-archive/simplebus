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
from threading import Event
from unittest import TestCase


class TestConfig(TestCase):
    def test_from_object(self):
        config = Config()
        config.from_object(self.CustomConfig())
        self.assertEqual('amqp://test:test@localhost/', config.BROKER_URL)

    class CustomConfig(object):
        BROKER_URL = 'amqp://test:test@localhost/'


class TestDequeue(TestCase):

    def setUp(self):
        self.bus = Bus()
        self.bus.start()

    def tearDown(self):
        self.bus.stop()

    def test_dequeue_as_function(self):
        event = Event()

        def on_message(message):
            self.assertEqual('hello', message)
            event.set()

        self.bus.dequeue('queue_test', on_message)
        self.bus.enqueue('queue_test', 'hello')

        event.wait()

    def test_dequeue_as_decorator(self):
        event = Event()

        @self.bus.dequeue('queue_test')
        def on_message(message):
            self.assertEqual('hello', message)
            event.set()

        self.bus.enqueue('queue_test', 'hello')

        event.wait()


class TestSubscribe(TestCase):

    def setUp(self):
        self.bus = Bus()
        self.bus.start()

    def tearDown(self):
        self.bus.stop()

    def test_subscribe_as_function(self):
        event = Event()

        def on_message(message):
            self.assertEqual('hello', message)
            event.set()

        self.bus.subscribe('project.app.topic1', on_message)
        self.bus.publish('project.app.topic1', 'hello')

        event.wait()

    def test_subscribe_as_decorator(self):
        event = Event()

        @self.bus.subscribe('project.app.topic1')
        def on_message(message):
            self.assertEqual('hello', message)
            event.set()

        self.bus.publish('project.app.topic1', 'hello')

        event.wait()
