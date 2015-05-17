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

from simplebus.exceptions import CompressionNotFoundError
from simplebus.exceptions import CompressionError
from simplebus.compression import CompressorRegistry
from simplebus.compression import GzipCompressor
from unittest import TestCase


class TestCompression(TestCase):
    def setUp(self):
        self.registry = CompressorRegistry()
        self.registry.register('gzip', GzipCompressor())

    def tearDown(self):
        self.registry.unregister_all()

    def test_invalid_body(self):
        self.assertRaises(CompressionError, self.registry.compress, dict(property=1), 'gzip')
        self.assertRaises(CompressionError, self.registry.decompress, dict(property=1), None, 'gzip')

    def test_gzip_compression(self):
        content_type, body = self.registry.compress(b'hello', 'gzip')
        self.assertEqual('application/x-gzip', content_type)
        body = self.registry.decompress(body, content_type)
        self.assertEqual(b'hello', body)

    def test_not_found(self):
        self.assertRaises(CompressionNotFoundError, self.registry.compress, 'hello', 'unknown')
        self.assertRaises(CompressionNotFoundError, self.registry.decompress, 'hello', 'unknown')
        self.assertRaises(CompressionNotFoundError, self.registry.decompress, 'hello', '', 'unknown')
