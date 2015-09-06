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

import pickle

from .errors import SerializationError
from .messages import dict_to_message, message_to_dict
from .pipeline import PipelineStep

try:
    import simplejson as json
except ImportError:
    import json

try:
    import msgpack
except ImportError:
    msgpack = None


class DeserializeMessageStep(PipelineStep):
    id = 'DeserializeMessage'

    def __init__(self, bus):
        self.__messages = bus.messages
        self.__serializers = bus.serializers

    def execute(self, context, next_step):
        serializer = self.__get_best_serializer(context)
        message_cls = self.__get_best_message_cls(context)

        try:
            context.body = serializer.deserialize(context.body)
            context.body = dict_to_message(message_cls, context.body)
        except Exception as e:
            raise SerializationError(e)

        next_step()

    def __get_best_serializer(self, context):
        if context.content_type:
            return self.__serializers.find(context.content_type)

        return self.__serializers.get(context.options.get('serializer'))

    def __get_best_message_cls(self, context):
        if context.type:
            message_cls = self.__messages.get_by_type(context.type)
            if message_cls:
                return message_cls
            raise SerializationError('Not found a message class for the type \'%s\'.' % context.type)
        else:
            address = context.options.get('address')

            messages_cls = self.__messages.get_by_address(address)
            if len(messages_cls) == 1:
                return messages_cls[0]

            if not messages_cls:
                raise SerializationError('Not found a message class for the address \'%s\'.' % address)
            else:
                raise SerializationError('Multiple message classes for the address \'%s\'.' % address)


class SerializeMessageStep(PipelineStep):
    id = 'SerializeMessage'

    def __init__(self, serializers):
        self.__serializers = serializers

    def execute(self, context, next_step):
        serializer = self.__serializers.get(context.options.get('serializer'))

        try:
            context.body = serializer.serialize(message_to_dict(context.body))
            context.content_type = serializer.mimetype
        except Exception as e:
            raise SerializationError(e)

        next_step()


class Serializer(object):
    def __init__(self, mimetype, serialize, deserialize):
        self.mimetype = mimetype
        self.serialize = serialize
        self.deserialize = deserialize


class SerializerRegistry(object):
    def __init__(self):
        self.__serializers_by_name = {}
        self.__serializers_by_mimetype = {}

        self.add('json', 'application/json', lambda obj: json.dumps(obj).encode(), lambda s: json.loads(s.decode()))
        self.add('pickle', 'application/x-pickle', pickle.dumps, pickle.loads)

        if msgpack:
            self.add('msgpack', 'application/x-msgpack', msgpack.packb, msgpack.unpackb)

        self.add('text', 'text/plain', lambda data: data.encode(), lambda data: data.decode())
        self.add('binary', 'application/octet-stream', lambda data: data, lambda data: data)

    def add(self, name, mimetype, compress, decompress):
        serializer = Serializer(mimetype, compress, decompress)
        self.__serializers_by_name[name] = serializer
        self.__serializers_by_mimetype[mimetype] = serializer

    def get(self, name):
        serializer = self.__serializers_by_name.get(name)
        if not serializer:
            raise SerializationError("Serializer '%s' not found." % name)
        return serializer

    def find(self, mimetype):
        serializer = self.__serializers_by_mimetype.get(mimetype)
        if not serializer:
            raise SerializationError("Serializer '%s' not found." % mimetype)
        return serializer

    def remove(self, name):
        serializer = self.__serializers_by_name.pop(name)
        if serializer:
            self.__serializers_by_mimetype.pop(serializer[0])
