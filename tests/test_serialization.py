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

from simplebus.errors import SerializerNotFoundError
from simplebus.errors import SerializationError
from simplebus.serialization import SerializerRegistry
from simplebus.serialization import JsonSerializer
from unittest import TestCase


class TestSerialization(TestCase):
    def setUp(self):
        self.registry = SerializerRegistry()

    def test_default(self):
        # string
        content_type, content_encoding, body = self.registry.dumps('hello')
        self.assertEqual('text/plain', content_type)
        self.assertEqual('utf-8', content_encoding)
        self.assertEqual(b'hello', body)

        message = self.registry.loads(body, content_type, content_encoding)
        self.assertEqual('hello', message)

        # bytes
        content_type, content_encoding, body = self.registry.dumps(b'hello')
        self.assertEqual('application/octet-stream', content_type)
        self.assertEqual('binary', content_encoding)
        self.assertEqual(b'hello', body)

        message = self.registry.loads(body, content_type, content_encoding)
        self.assertEqual(b'hello', message)

    def test_raw(self):
        # string
        content_type, content_encoding, body = self.registry.dumps('hello', serializer='raw')
        self.assertEqual('application/data', content_type)
        self.assertEqual('utf-8', content_encoding)
        self.assertEqual(b'hello', body)

        message = self.registry.loads(body, content_type, content_encoding, serializer='raw')
        self.assertEqual('hello', message)

        message = self.registry.loads(body, None, None, serializer='raw')
        self.assertEqual(b'hello', message)

        # bytes
        content_type, content_encoding, body = self.registry.dumps(b'hello', serializer='raw')
        self.assertEqual('application/data', content_type)
        self.assertEqual('binary', content_encoding)
        self.assertEqual(b'hello', body)

        message = self.registry.loads(body, content_type, content_encoding, serializer='raw')
        self.assertEqual(b'hello', message)

        message = self.registry.loads(body, None, None, serializer='raw')
        self.assertEqual(b'hello', message)

    def test_json(self):
        self.registry.register('json', JsonSerializer())

        content_type, content_encoding, body = self.registry.dumps('hello', serializer='json')
        self.assertEqual('application/json', content_type)
        self.assertEqual('utf-8', content_encoding)
        self.assertEqual(b'"hello"', body)

        message = self.registry.loads(body, content_type, content_encoding)
        self.assertEqual('hello', message)

        message = self.registry.loads(body, None, None, serializer='json')
        self.assertEqual('hello', message)

    def test_not_found(self):
        self.assertRaises(SerializerNotFoundError, self.registry.dumps, 'hello', serializer='unknown')
        self.assertRaises(SerializerNotFoundError, self.registry.loads, 'hello', None, None, serializer='unknown')

    def test_invalid_message(self):
        self.assertRaises(SerializationError, self.registry.dumps, self)

