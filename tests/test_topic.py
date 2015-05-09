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

"""Unit tests."""

from simplebus import MessageHandler
from simplebus import subscribe
from tests import create_bus
from unittest import TestCase


class TestTopic(TestCase):
    topic = 'tests.topic1'

    def setUp(self):
        self.bus = create_bus()
        self.bus.start()

    def tearDown(self):
        self.bus.stop()

    def test_subscribe_as_class(self):
        class Handler1(MessageHandler):
            def handle(self_, message):
                self.assertEqual('hello', message)
                self.bus.loop.stop()

        self.bus.subscribe(self.topic, Handler1())
        self.bus.publish(self.topic, 'hello')
        self.bus.loop.start()

    def test_subscribe_as_decorator(self):
        @subscribe(self.topic)
        def handle(message):
            self.assertEqual('hello', message)
            self.bus.loop.stop()

        self.bus.publish(self.topic, 'hello')
        self.bus.loop.start()

    def test_subscribe_as_function(self):
        def handle(message):
            self.assertEqual('hello', message)
            self.bus.loop.stop()

        self.bus.subscribe(self.topic, handle)
        self.bus.publish(self.topic, 'hello')
        self.bus.loop.start()

    def test_unsubscribe(self):
        def handle(message):
            pass

        subscription = self.bus.subscribe(self.topic, handle)
        subscription.cancel()