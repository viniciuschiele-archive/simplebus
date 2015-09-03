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

    for n, compressor in compressors:
        if n == name:
            return compressor
    raise CompressionError("Compression '%s' not found." % name)


class CompressMessageStep(PipelineStep):
    id = 'CompressMessage'

    def __init__(self, compressors):
        self.__compressors = compressors

    def invoke(self, context, next_step):
        name = context.options.get('compressor')
        if name:
            compressor = get_compressor(self.__compressors, name)

            try:
                context.message = compressor.compress(context.message)
                context.content_encoding = name
            except Exception as e:
                raise CompressionError(e)

        next_step()


class DecompressMessageStep(PipelineStep):
    id = 'DecompressMessage'

    def __init__(self, compressors):
        self.__compressors = compressors

    def invoke(self, context, next_step):
        transport_message = context.transport_message

        name = context.options.get('decompressor') or transport_message.content_encoding

        if name:
            compressor = get_compressor(self.__compressors, name)

            try:
                transport_message.body = compressor.decompress(transport_message.body)
            except Exception as e:
                raise CompressionError(e)

        next_step()


class Compressor(metaclass=ABCMeta):
    """Base class for a compressor."""

    @abstractmethod
    def compress(self, data):
        """Compress the specified body."""
        pass

    @abstractmethod
    def decompress(self, data):
        """Decompress the specified body."""
        pass


class GzipCompressor(Compressor):
    """Gzip compressor."""

    def compress(self, data):
        """Compress the specified body."""
        return zlib.compress(data)

    def decompress(self, data):
        """Decompress the specified body."""
        return zlib.decompress(data)
