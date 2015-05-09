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

"""Classes used to cancel the receiving messages from the broker."""


from abc import ABCMeta
from abc import abstractmethod
from simplebus.exceptions import SerializationError
from simplebus.exceptions import SerializerNotFoundError

try:
    import simplejson as json
except ImportError:  # pragma: no cover
    import json  # noqa


class SerializerRegistry(object):
    def __init__(self):
        self.__serializers = {}
        self.__default_serializer = None

    @property
    def default_serializer(self):
        return self.__default_serializer

    @default_serializer.setter
    def default_serializer(self, value):
        self.__default_serializer = value

    def register(self, name, serializer):
        self.__serializers[name] = serializer

    def unregister_all(self):
        self.__serializers.clear()

    def get(self, name):
        serializer = self.__serializers.get(name)

        if serializer:
            return serializer

        raise SerializerNotFoundError("Serializer '%s' not found." % name)

    def find(self, content_type):
        for ser in self.__serializers.values():
            if ser.content_type == content_type:
                return ser
        raise SerializerNotFoundError("Serializer not found for the content type '%s'." % content_type)

    def serialize(self, message, serializer=None):
        serializer = serializer or self.default_serializer

        if not serializer:
            raise SerializationError('There is no serializer.')

        if serializer == 'raw':
            if isinstance(message, bytes):
                return 'application/data', 'binary', message

            if isinstance(message, str):
                return 'text/plain', 'utf-8', message.encode()

            raise SerializationError('Message must be a string or bytes to be serialized using Raw.')

        ser = self.get(serializer)
        return ser.content_type, ser.content_encoding, ser.serialize(message)

    def deserialize(self, body, content_type, content_encoding, serializer=None):
        if serializer:
            if serializer == 'raw':
                return body
            return self.get(serializer).deserialize(body)

        if content_encoding == 'binary':
            return body

        if content_type:
            if content_type == 'text/plain':
                return body.decode(content_encoding or 'utf-8')
            return self.find(content_type).deserialize(body)

        if self.default_serializer:
            if self.default_serializer == 'raw':
                return body
            return self.get(self.default_serializer).deserialize(body)

        raise SerializerNotFoundError('Deserialize needs a content_type or a default serializer.')


class Serializer(metaclass=ABCMeta):
    @property
    @abstractmethod
    def content_type(self):
        pass

    @property
    @abstractmethod
    def content_encoding(self):
        pass

    @abstractmethod
    def serialize(self, message):
        pass

    @abstractmethod
    def deserialize(self, buffer):
        pass


class JsonSerializer(Serializer):
    @property
    def content_type(self):
        return 'application/json'

    @property
    def content_encoding(self):
        return 'utf-8'

    def serialize(self, message):
        try:
            return json.dumps(message)
        except Exception as e:
            raise SerializationError(str(e))

    def deserialize(self, buffer):
        try:
            return json.loads(buffer)
        except Exception as e:
            raise SerializationError(str(e))
