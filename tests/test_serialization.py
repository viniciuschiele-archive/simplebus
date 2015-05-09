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

from simplebus.exceptions import SerializerNotFoundError
from simplebus.exceptions import SerializationError
from simplebus.serialization import SerializerRegistry
from simplebus.serialization import JsonSerializer
from unittest import TestCase


class TestSerialization(TestCase):
    def setUp(self):
        self.registry = SerializerRegistry()

    def tearDown(self):
        self.registry.unregister_all()

    def test_default_serializer(self):
        self.registry.default_serializer = 'raw'
        content_type, content_encoding, body = self.registry.serialize('hello')

        self.assertEqual('text/plain', content_type)
        self.assertEqual('utf-8', content_encoding)
        self.assertEqual(b'hello', body)

        message = self.registry.deserialize(body, None, None)

        self.assertEqual(b'hello', message)

    def test_serializer_not_found(self):
        self.assertRaises(SerializerNotFoundError, self.registry.serialize, 'hello', serializer='unknown')
        self.assertRaises(SerializerNotFoundError, self.registry.deserialize, 'hello', None, None, serializer='unknown')

    def test_serializer_raw(self):
        # test bytes
        content_type, content_encoding, body = self.registry.serialize(b'hello', serializer='raw')

        self.assertEqual('application/data', content_type)
        self.assertEqual('binary', content_encoding)
        self.assertEqual(b'hello', body)

        message = self.registry.deserialize(body, content_type, content_encoding)

        self.assertEqual(b'hello', message)

        # test text
        content_type, content_encoding, body = self.registry.serialize('hello', serializer='raw')

        self.assertEqual('text/plain', content_type)
        self.assertEqual('utf-8', content_encoding)
        self.assertEqual(b'hello', body)

        message = self.registry.deserialize(body, content_type, content_encoding)

        self.assertEqual('hello', message)

        # test invalid message
        self.assertRaises(SerializationError, self.registry.serialize, dict(property=1), serializer='raw')

    def test_serializer_json(self):
        self.registry.register('json', JsonSerializer())
        content_type, content_encoding, body = self.registry.serialize('hello', serializer='json')

        self.assertEqual('application/json', content_type)
        self.assertEqual('utf-8', content_encoding)
        self.assertEqual('"hello"', body)

        message = self.registry.deserialize(body, content_type, content_encoding)

        self.assertEqual('hello', message)

    def test_deserialize_with_invalid_content_type(self):
        self.assertRaises(SerializerNotFoundError, self.registry.deserialize, b'hello', None, None)
        self.assertRaises(SerializerNotFoundError, self.registry.deserialize, b'hello', 'image/jpeg', None)
