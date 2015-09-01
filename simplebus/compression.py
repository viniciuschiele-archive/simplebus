# Copyright 2015 Vinicius Chiele. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Implements the compression related objects."""

import zlib

from abc import ABCMeta
from abc import abstractmethod
from .errors import CompressionError
from .pipeline import PipelineStep


def get_compressor(compressors, name):
    """Gets the compressor by the name."""

    compressor = compressors.get(name)
    if compressor:
        return compressor
    raise CompressionError("Compression '%s' not found." % name)


def find_compressor(compressors, content_encoding):
    """Gets the compressor by the content type."""

    for compressor in compressors.values():
        if compressor.content_encoding == content_encoding:
            return compressor
    raise CompressionError("Compression '%s' not found." % content_encoding)


class Compressor(metaclass=ABCMeta):
    """Base class for a compressor."""

    @property
    @abstractmethod
    def content_encoding(self):
        """Gets the content encoding used to compress."""
        pass

    @abstractmethod
    def compress(self, body):
        """Compress the specified body."""
        pass

    @abstractmethod
    def decompress(self, body):
        """Decompress the specified body."""
        pass


class GzipCompressor(Compressor):
    """Gzip compressor."""

    @property
    def content_encoding(self):
        """Gets the content encoding used to compress."""
        return 'gzip'

    def compress(self, body):
        """Compress the specified body."""
        return zlib.compress(body)

    def decompress(self, body):
        """Decompress the specified body."""
        return zlib.decompress(body)


class CompressMessageStep(PipelineStep):
    id = 'CompressMessage'

    def __init__(self, compressors):
        self.__compressors = compressors

    def invoke(self, context, next_step):
        compression = context.options.get('compression')
        if compression:
            compressor = get_compressor(self.__compressors, compression)

            try:
                context.message = compressor.compress(context.message)
                context.content_encoding = compressor.content_encoding
            except Exception as e:
                raise CompressionError(e)

        next_step()


class DecompressMessageStep(PipelineStep):
    id = 'DecompressMessage'

    def __init__(self, compressors):
        self.__compressors = compressors

    def invoke(self, context, next_step):
        transport_message = context.transport_message

        compressor = None

        if context.options.get('compression'):
            compressor = get_compressor(self.__compressors, context.options.get('compression'))
        elif transport_message.content_encoding:
            compressor = find_compressor(self.__compressors, transport_message.content_encoding)

        if compressor:
            try:
                transport_message.body = compressor.decompress(transport_message.body)
            except Exception as e:
                raise CompressionError(e)

        next_step()
