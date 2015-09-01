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

from simplebus.compression import CompressMessageStep, DecompressMessageStep, GzipCompressor
from simplebus.errors import CompressionError
from simplebus.transports.base import TransportMessage
from simplebus.pipeline import PipelineContext
from unittest import TestCase


class TestCompressionStep(TestCase):
    def setUp(self):
        self.compress_step = CompressMessageStep({'gzip': GzipCompressor()})
        self.decompress_step = DecompressMessageStep({'gzip': GzipCompressor()})

    def __dummy_step(self):
        pass

    def test_invalid_body(self):
        context = PipelineContext()
        context.message = dict(property=1)
        context.options = {'compression': 'gzip'}
        self.assertRaises(CompressionError, self.compress_step.invoke, context, self.__dummy_step)

        context = PipelineContext()
        context.transport_message = TransportMessage(content_encoding='gzip', body='invalid body')
        context.options = {}
        self.assertRaises(CompressionError, self.decompress_step.invoke, context, self.__dummy_step)

    def test_gzip_compression(self):
        context = PipelineContext()
        context.message = b'hello'
        context.options = {'compression': 'gzip'}
        self.compress_step.invoke(context, self.__dummy_step)

        self.assertEqual('gzip', context.content_encoding)
        self.assertIsNot(b'hello', context.message)

        context2 = PipelineContext()
        context2.transport_message = TransportMessage(content_encoding=context.content_encoding, body=context.message)
        context2.options = {}
        self.decompress_step.invoke(context2, self.__dummy_step)

        self.assertEqual(b'hello', context2.transport_message.body)

    def test_not_found(self):
        context = PipelineContext()
        context.options = {'compression': 'unknown'}
        self.assertRaises(CompressionError, self.compress_step.invoke, context, self.__dummy_step)

        context = PipelineContext()
        context.options = {'compression': 'unknown'}
        self.assertRaises(CompressionError, self.decompress_step.invoke, context, self.__dummy_step)

        context = PipelineContext()
        context.transport_message = TransportMessage(content_encoding='unknown')
        context.options = {}
        self.assertRaises(CompressionError, self.decompress_step.invoke, context, self.__dummy_step)
