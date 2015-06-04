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

"""Implements the serialization related objects."""

import codecs
import pickle

from abc import ABCMeta
from abc import abstractmethod
from .errors import SerializationError
from .errors import SerializerNotFoundError

try:
    import simplejson as json
except ImportError:
    import json

try:
    import msgpack
except ImportError:
    msgpack = None


class SerializerRegistry(object):
    """Stores the serializers used by simplebus."""

    def __init__(self):
        self.__serializers = {}

    def register(self, name, serializer):
        """Registers a new serializer."""
        self.__serializers[name] = serializer

    def unregister(self, name):
        """Unregister the specified serializer."""
        self.__serializers.pop(name)

    def get(self, name):
        """Gets the serializer by the name."""

        serializer = self.__serializers.get(name)
        if serializer:
            return serializer
        raise SerializerNotFoundError("Serializer '%s' not found." % name)

    def find(self, content_type):
        """Gets the serializer by the content type."""

        for serializer in self.__serializers.values():
            if content_type == serializer.content_type:
                return serializer
        return None

    def dumps(self, body, serializer=None):
        """Serializes the specified body using the specified serializer."""

        if serializer == 'raw':
            return self.__raw_dumps(body)

        if serializer:
            ser = self.get(serializer)
            return ser.content_type, ser.content_encoding, ser.dumps(body)

        if isinstance(body, str):
            return 'text/plain', 'utf-8', body.encode()

        if isinstance(body, bytes):
            return 'application/octet-stream', 'binary', body

        raise SerializationError('No serializer specified to serialize.')

    def loads(self, body, content_type, content_encoding, serializer=None):
        """Deserializes the specified body using the specified serializer."""

        if content_type:
            ser = self.find(content_type)
            if ser:
                return ser.loads(body)

        if content_type == 'text/plain':
            return codecs.decode(body, content_encoding or 'utf-8')

        if content_type == 'application/octet-stream':
            return body

        if serializer == 'raw':
            return self.__raw_loads(body, content_encoding)

        if serializer:
            return self.get(serializer).loads(body)

        raise SerializationError('No serializer specified to deserialize.')

    @staticmethod
    def __raw_dumps(body):
        """Serializes the body as str or bytes."""

        if isinstance(body, bytes):
            return 'application/data', 'binary', body

        if isinstance(body, str):
            return 'application/data', 'utf-8', body.encode()

        raise SerializationError('Serializer raw only serializes str or bytes.')

    @staticmethod
    def __raw_loads(body, content_encoding):
        """Deserializes the body using the encoding."""
        if not content_encoding or content_encoding == 'binary':
            return body

        return codecs.decode(body, content_encoding)


class Serializer(metaclass=ABCMeta):
    """Base class for a serializer."""

    @property
    @abstractmethod
    def content_type(self):
        """Gets the content type used to serialize."""
        pass

    @property
    @abstractmethod
    def content_encoding(self):
        """Gets the content encoding used to serialize."""
        pass

    @abstractmethod
    def dumps(self, body):
        """Serializes the specified body into bytes."""
        pass

    @abstractmethod
    def loads(self, body):
        """Deserializes the specified body into a object."""
        pass


class JsonSerializer(Serializer):
    """Json serializer."""

    @property
    def content_type(self):
        """Gets the content type used to serialize."""
        return 'application/json'

    @property
    def content_encoding(self):
        """Gets the content encoding used to serialize."""
        return 'utf-8'

    def dumps(self, body):
        """Serializes the specified body into bytes."""

        try:
            return json.dumps(body).encode(self.content_encoding)
        except Exception as e:
            raise SerializationError(str(e))

    def loads(self, body):
        """Deserializes the specified body into a object."""

        try:
            return json.loads(body.decode(self.content_encoding))
        except Exception as e:
            raise SerializationError(str(e))


class MsgPackSerializer(Serializer):
    """msgpack serializer."""

    def __init__(self):
        if not msgpack:
            raise ImportError('Missing msgpack library (pip install msgpack-python)')

    @property
    def content_type(self):
        """Gets the content type used to serialize."""
        return 'application/x-msgpack'

    @property
    def content_encoding(self):
        """Gets the content encoding used to serialize."""
        return 'binary'

    def dumps(self, body):
        """Serializes the specified body into bytes."""

        try:
            return msgpack.packb(body)
        except Exception as e:
            raise SerializationError(str(e))

    def loads(self, body):
        """Deserializes the specified body into a object."""

        try:
            return msgpack.unpackb(body, encoding='utf-8')
        except Exception as e:
            raise SerializationError(str(e))


class PickleSerializer(Serializer):
    """Pickle serializer."""

    @property
    def content_type(self):
        """Gets the content type used to serialize."""
        return 'application/x-pickle'

    @property
    def content_encoding(self):
        """Gets the content encoding used to serialize."""
        return 'utf-8'

    def dumps(self, body):
        """Serializes the specified body into bytes."""

        try:
            return pickle.dumps(body)
        except Exception as e:
            raise SerializationError(str(e))

    def loads(self, body):
        """Deserializes the specified body into a object."""

        try:
            return pickle.loads(body, encoding=self.content_encoding)
        except Exception as e:
            raise SerializationError(str(e))


registry = SerializerRegistry()
registry.register('json', JsonSerializer())
registry.register('pickle', PickleSerializer())

if msgpack:
    registry.register('msgpack', MsgPackSerializer())


def dumps(body, serializer=None):
    """Serializes the specified body using the specified serializer."""
    return registry.dumps(body, serializer)


def loads(body, content_type, content_encoding, serializer=None):
    """Deserializes the specified body using the specified serializer."""
    return registry.loads(body, content_type, content_encoding, serializer)
