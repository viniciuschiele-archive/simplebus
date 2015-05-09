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

import datetime
import uuid

from simplebus import transport_message
from simplebus import MessageHandler
from simplebus import pull
from simplebus import SerializerNotFoundError
from tests import create_bus
from unittest import TestCase


class TestQueue(TestCase):
    queue = 'tests.queue1'

    def setUp(self):
        self.bus = create_bus()
        self.bus.start()

    def tearDown(self):
        self.bus.stop()

    def test_pull_as_class(self):
        class Handler1(MessageHandler):
            def handle(self_, message):
                self.assertEqual('hello', message)
                self.bus.loop.stop()

        self.bus.pull(self.queue, Handler1())
        self.bus.push(self.queue, 'hello')
        self.bus.loop.start()

    def test_pull_as_decorator(self):
        @pull(self.queue)
        def handle(message):
            self.assertEqual('hello', message)
            self.bus.loop.stop()

        self.bus.push(self.queue, 'hello')
        self.bus.loop.start()

    def test_pull_as_function(self):
        def handle(message):
            self.assertEqual('hello', message)
            self.bus.loop.stop()

        self.bus.pull(self.queue, handle)
        self.bus.push(self.queue, 'hello')
        self.bus.loop.start()

    def test_cancel(self):
        def handle(message):
            pass

        cancellation = self.bus.pull(self.queue, handle)
        cancellation.cancel()

    def test_message_properties(self):
        def handle(message):
            self.assertEqual('application/json', transport_message.content_type)
            self.bus.loop.stop()

        self.bus.pull(self.queue, handle)
        self.bus.push(self.queue, 'hello')
        self.bus.loop.start()

    def test_max_retries(self):
        key = str(uuid.uuid4())

        def handle(message):
            raise RuntimeError('error')

        def handle_error(message):
            self.assertEqual(key, message)
            self.bus.loop.stop()

        self.bus.pull(self.queue, handle, error_queue_enabled=True)
        self.bus.pull(self.queue + '.error', handle_error, error_queue_enabled=False)
        self.bus.push(self.queue, key)
        self.bus.loop.start()

    def test_retry_delay(self):
        started_at = None

        def handle(message):
            global started_at
            if transport_message.retry_count == 0:
                started_at = datetime.datetime.now()
                raise RuntimeError('error')
            elapsed = datetime.datetime.now() - started_at

            try:
                if elapsed.microseconds < 100 * 1000:
                    self.fail('Message received too early.')
            finally:
                self.bus.loop.stop()

        self.bus.pull(self.queue, handle, max_retries=1, retry_delay=200)
        self.bus.push(self.queue, 'hello')
        self.bus.loop.start()

    def test_serializer_not_found(self):
        self.assertRaises(SerializerNotFoundError, self.bus.push, self.queue, 'hello', serializer='unknown')

    def test_serializer_binary(self):
        def handle(message):
            self.assertEqual('application/data', transport_message.content_type)
            self.assertEqual('binary', transport_message.content_encoding)
            self.assertEqual(b'hello', message)
            self.bus.loop.stop()

        self.bus.pull(self.queue, handle)
        self.bus.push(self.queue, b'hello', serializer='raw')
        self.bus.loop.start()

    def test_serializer_text(self):
        def handle(message):
            self.assertEqual('text/plain', transport_message.content_type)
            self.assertEqual('utf-8', transport_message.content_encoding)
            self.assertEqual('hello', message)
            self.bus.loop.stop()

        self.bus.pull(self.queue, handle)
        self.bus.push(self.queue, 'hello', serializer='raw')
        self.bus.loop.start()
