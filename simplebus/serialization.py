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


class MessageSerializers(object):
    def __init__(self):
        self.__serializers = {}

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


class MessageSerializer(metaclass=ABCMeta):
    @property
    @abstractmethod
    def content_type(self):
        pass

    @abstractmethod
    def serialize(self, message):
        pass

    @abstractmethod
    def deserialize(self, buffer):
        pass


class JsonSerializer(MessageSerializer):
    @property
    def content_type(self):
        return 'application/json'

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
