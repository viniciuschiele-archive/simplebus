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
            self.assertEqual('utf-8', transport_message.content_encoding)
            self.bus.loop.stop()

        self.bus.pull(self.queue, handle)
        self.bus.push(self.queue, 'hello')
        self.bus.loop.start()

    def test_max_retries(self):
        key = str(uuid.uuid4())

        def handle(message):
            self.assertLessEqual(transport_message.retry_count, 2)
            raise RuntimeError('error')

        def handle_error(message):
            self.assertEqual(key, message)
            self.bus.loop.stop()

        self.bus.pull(self.queue, handle, dead_letter=True, retry=True, max_retries=2)
        self.bus.pull(self.queue + '.error', handle_error)
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

        self.bus.pull(self.queue, handle, retry=True, max_retries=1, retry_delay=200)
        self.bus.push(self.queue, 'hello')
        self.bus.loop.start()

    def test_max_concurrency(self):
        import threading

        self.thread_ids = []

        def handle(message):
            self.thread_ids.append(threading.current_thread().ident)
            if len(self.thread_ids) == 2:
                self.bus.loop.stop()

        self.bus.pull(self.queue, handle, max_concurrency=2, prefetch_count=1)
        self.bus.push(self.queue, 'hello')
        self.bus.push(self.queue, 'hello')
        self.bus.loop.start()
        self.assertIsNot(self.thread_ids[0], self.thread_ids[1])
