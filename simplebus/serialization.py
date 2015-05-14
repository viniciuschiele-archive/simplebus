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

from abc import ABCMeta
from abc import abstractmethod
from simplebus.exceptions import SerializationError
from simplebus.exceptions import SerializerNotFoundError

try:
    import simplejson as json
except ImportError:  # pragma: no cover
    import json  # noqa

try:
    import msgpack
except ImportError:
    msgpack = None


class SerializerRegistry(object):
    """Stores the serializers used by simplebus."""

    def __init__(self):
        self.__serializers = {}

    def register(self, name, serializer):
        """Register a new serializer."""
        self.__serializers[name] = serializer

    def unregister_all(self):
        """Unregister all serializers."""
        self.__serializers.clear()

    def get(self, name):
        """Gets the serializer by the name."""

        serializer = self.__serializers.get(name)
        if serializer:
            return serializer
        raise SerializerNotFoundError("Serializer '%s' not found." % name)

    def serialize(self, message, serializer=None):
        """Serializes the specified message using the specified serializer."""

        if serializer:
            ser = self.get(serializer)
            return ser.content_type, ser.content_encoding, ser.serialize(message)

        if isinstance(message, bytes):
            return 'application/data', 'binary', message
        if isinstance(message, str):
            return 'text/plain', 'utf-8', message.encode()

        raise SerializationError('Message should be bytes or str to be serialized.')

    def deserialize(self, body, content_encoding, serializer=None):
        """Deserializes the specified message using the specified serializer."""

        if serializer:
            return self.get(serializer).deserialize(body)

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
    def serialize(self, value):
        """Serializes the specified value into bytes."""
        pass

    @abstractmethod
    def deserialize(self, buffer):
        """Deserializes the specified bytes into a object."""
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

    def serialize(self, message):
        """Serializes the specified value into bytes."""

        try:
            return json.dumps(message).encode(self.content_encoding)
        except Exception as e:
            raise SerializationError(str(e))

    def deserialize(self, buffer):
        """Deserializes the specified bytes into a object."""

        try:
            return json.loads(buffer.decode(self.content_encoding))
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

    def serialize(self, message):
        """Serializes the specified value into bytes."""

        try:
            return msgpack.packb(message)
        except Exception as e:
            raise SerializationError(str(e))

    def deserialize(self, buffer):
        """Deserializes the specified bytes into a object."""

        try:
            return msgpack.unpackb(buffer, encoding='utf-8')
        except Exception as e:
            raise SerializationError(str(e))
