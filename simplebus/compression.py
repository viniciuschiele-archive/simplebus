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

from .errors import CompressionError
from .pipeline import PipelineStep


def _get_compressor(compressors, name):
    """Gets the compressor by the name."""

    for n, f in compressors:
        if n == name:
            return f
    raise CompressionError("Compressor '%s' not found." % name)


def _get_decompressor(decompressors, name):
    """Gets the decompressor by the name."""

    for n, f in decompressors:
        if n == name:
            return f
    raise CompressionError("Decompressor '%s' not found." % name)


class CompressMessageStep(PipelineStep):
    id = 'CompressMessage'

    def __init__(self, compressions):
        self.__compressions = compressions

    def invoke(self, context, next_step):
        name = context.options.get('compressor')
        if name:
            compress = _get_compressor(self.__compressions, name)

            try:
                context.message = compress(context.message)
                context.content_encoding = name
            except Exception as e:
                raise CompressionError(e)

        next_step()


class DecompressMessageStep(PipelineStep):
    id = 'DecompressMessage'

    def __init__(self, compressions):
        self.__compressions = compressions

    def invoke(self, context, next_step):
        transport_message = context.transport_message

        name = context.options.get('decompressor') or transport_message.content_encoding

        if name:
            decompress = _get_decompressor(self.__compressions, name)

            try:
                transport_message.body = decompress(transport_message.body)
            except Exception as e:
                raise CompressionError(e)

        next_step()
