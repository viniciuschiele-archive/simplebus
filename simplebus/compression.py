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

from .errors import CompressionError
from .pipeline import PipelineStep


class CompressMessageStep(PipelineStep):
    id = 'CompressMessage'

    def __init__(self, compressors):
        self.__compressors = compressors

    def execute(self, context, next_step):
        name = context.options.get('compressor')
        if name:
            try:
                compressor = self.__compressors.get(name)
                context.body = compressor.compress(context.body)
                context.content_encoding = name
            except Exception as e:
                raise CompressionError(e)

        next_step()


class DecompressMessageStep(PipelineStep):
    id = 'DecompressMessage'

    def __init__(self, compressors):
        self.__compressors = compressors

    def execute(self, context, next_step):
        if context.content_encoding:
            compressor = self.__compressors.get(context.content_encoding)

            try:
                context.body = compressor.decompress(context.body)
            except Exception as e:
                raise CompressionError(e)

        next_step()


class Compressor(object):
    def __init__(self, compress, decompress):
        self.compress = compress
        self.decompress = decompress


class CompressorRegistry(object):
    def __init__(self):
        self.__compressors = {}
        self.add('gzip', zlib.compress, zlib.decompress)

    def add(self, name, compress, decompress):
        self.__compressors[name] = Compressor(compress, decompress)

    def get(self, name):
        compressor = self.__compressors.get(name)
        if compressor:
            return compressor
        raise CompressionError("Compressor '%s' not found." % name)

    def remove(self, name):
        self.__compressors.pop(name)

