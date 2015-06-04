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
from .errors import CompressionNotFoundError


class CompressorRegistry(object):
    """Stores the compressors used by simplebus."""

    def __init__(self):
        self.__compressors = {}

    def register(self, name, compressor):
        """Register a new compressor."""
        self.__compressors[name] = compressor

    def unregister(self, name):
        """Unregister the specified compressor."""
        self.__compressors.pop(name)

    def get(self, name):
        """Gets the compressor by the name."""

        compressor = self.__compressors.get(name)
        if compressor:
            return compressor
        raise CompressionNotFoundError("Compression '%s' not found." % name)

    def find(self, content_type):
        """Gets the compressor by the content type."""

        for compressor in self.__compressors.values():
            if compressor.content_type == content_type:
                return compressor
        raise CompressionNotFoundError("Compression '%s' not found." % content_type)

    def compress(self, body, compression):
        """Compress the specified body using the specified compression."""

        compressor = self.get(compression)
        return compressor.content_type, compressor.compress(body)

    def decompress(self, body, content_type, compression=None):
        """Decompress the specified body using the specified compression."""

        if compression:
            return self.get(compression).decompress(body)

        return self.find(content_type).decompress(body)


class Compressor(metaclass=ABCMeta):
    """Base class for a compressor."""

    @property
    @abstractmethod
    def content_type(self):
        """Gets the content type used to compress."""
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
    def content_type(self):
        """Gets the content type used to compress."""
        return 'application/x-gzip'

    def compress(self, body):
        """Compress the specified body."""

        try:
            return zlib.compress(body)
        except Exception as e:
            raise CompressionError(e)

    def decompress(self, body):
        """Decompress the specified body."""

        try:
            return zlib.decompress(body)
        except Exception as e:
            raise CompressionError(e)


registry = CompressorRegistry()
registry.register('gzip', GzipCompressor())


def compress(body, compression):
    """Compress the specified body using the specified compression."""
    return registry.compress(body, compression)


def decompress(body, content_type, compression=None):
    """Decompress the specified body using the specified compression."""
    return registry.decompress(body, content_type, compression)
