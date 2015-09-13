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

from simplebus import transport_message
from tests import create_simplebus
from unittest import TestCase


class TestSimpleBus(TestCase):
    def setUp(self):
        self.bus = create_simplebus()
        self.bus.start()

    def tearDown(self):
        self.bus.stop()

    def test_publish_subscribe(self):
        @self.bus.command()
        class Message(object):
            def __init__(self, text):
                self.text = text

        @self.bus.handle(Message)
        def handle(message):
            self.assertEqual('hello', message.text)
            self.bus.loop.stop()

        self.bus.subscribe(Message)
        self.bus.publish(Message('hello'))
        self.bus.loop.start()

    def test_compression(self):
        @self.bus.command(compressor='gzip')
        class Message(object):
            def __init__(self, text):
                self.text = text

        @self.bus.handle(Message)
        def handle(message):
            self.assertEqual('gzip', transport_message.content_encoding)
            self.assertEqual('hello', message.text)
            self.bus.loop.stop()

        self.bus.subscribe(Message)
        self.bus.publish(Message('hello'))
        self.bus.loop.start()

    def test_serializer(self):
        @self.bus.command(serializer='json')
        class Message(object):
            def __init__(self, text):
                self.text = text

        @self.bus.handle(Message)
        def handle(message):
            self.assertEqual('application/json', transport_message.content_type)
            self.assertEqual('hello', message.text)
            self.bus.loop.stop()

        self.bus.subscribe(Message)
        self.bus.publish(Message('hello'))
        self.bus.loop.start()

    def test_concurrency(self):
        import threading

        self.thread_ids = []

        @self.bus.command(concurrency=2, prefetch_count=1)
        class Message(object):
            def __init__(self, text):
                self.text = text

        @self.bus.handle(Message)
        def handle(message):
            self.assertEqual('hello', message.text)
            self.thread_ids.append(threading.current_thread().ident)
            if len(self.thread_ids) == 2:
                self.bus.loop.stop()

        self.bus.subscribe(Message)
        self.bus.publish(Message('hello'))
        self.bus.publish(Message('hello'))
        self.bus.loop.start()
        self.assertIsNot(self.thread_ids[0], self.thread_ids[1])
