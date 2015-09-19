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

import inspect

from .errors import SimpleBusError

COMMAND_MESSAGE_TYPE = 0
EVENT_MESSAGE_TYPE = 1


class MessageDefinition(object):
    def __init__(self, message_name, message_type, message_cls):
        self.message_name = message_name
        self.message_type = message_type
        self.message_cls = message_cls

        self.endpoint = None
        self.error_queue = None
        self.expires = None
        self.concurrency = None
        self.prefetch_count = None
        self.compressor = None
        self.serializer = None
        self.purge_on_subscribe = None
        self.destination = None

        self.dict_to_message = self.__generate_dict_to_message()
        self.message_to_dict = self.__generate_message_to_dict()

    def is_command(self):
        return self.message_type == COMMAND_MESSAGE_TYPE

    def is_event(self):
        return self.message_type == EVENT_MESSAGE_TYPE

    def update(self, options):
        self.__dict__.update(options)

    def __generate_dict_to_message(self):
        def set_dict(data):
            message = self.message_cls()
            message.__dict__.update(data)

        args = inspect.getargspec(self.message_cls.__init__)[0]
        if len(args) > 1:
            return lambda data: self.message_cls(**data)
        elif hasattr(self.message_cls, '__dict__'):
            return set_dict

        raise TypeError('Message \'%s\' has no __init__ to be instantiated.' % str(self.message_cls))

    def __generate_message_to_dict(self):
        if hasattr(self.message_cls, 'as_dict'):
            return lambda message: message.as_dict()
        if isinstance(self.message_cls, dict):
            return lambda message: message
        if hasattr(self.message_cls, '__dict__'):
            return lambda message: message.__dict__
        raise TypeError('Message \'%s\' cannot be converted to dict' % str(self.message_cls))


class MessageRegistry(object):
    def __init__(self, bus):
        self.__config = bus.config
        self.__messages_by_cls = {}
        self.__messages_by_destination = {}
        self.__messages_by_name = {}

    def add(self, message_cls, message_type, message_name=None, **options):
        if self.__messages_by_cls.get(message_cls):
            raise SimpleBusError('Message already registered.')

        if not message_name:
            message_name = message_cls.__name__

        message_def = MessageDefinition(message_name, message_type, message_cls)
        message_def.endpoint = self.__config.SIMPLEBUS_MESSAGE_ENDPOINT
        message_def.error_queue = self.__config.SIMPLEBUS_MESSAGE_ERROR_QUEUE
        message_def.expires = self.__config.SIMPLEBUS_MESSAGE_EXPIRES
        message_def.concurrency = self.__config.SIMPLEBUS_MESSAGE_CONCURRENCY
        message_def.prefetch_count = self.__config.SIMPLEBUS_MESSAGE_PREFETCH_COUNT
        message_def.compressor = self.__config.SIMPLEBUS_MESSAGE_COMPRESSOR
        message_def.serializer = self.__config.SIMPLEBUS_MESSAGE_SERIALIZER
        message_def.purge_on_subscribe = self.__config.SIMPLEBUS_PURGE_ON_SUBSCRIBE

        if message_def.is_command():
            message_def.destination = self.__config.SIMPLEBUS_COMMAND_DESTINATION
        else:
            message_def.destination = self.__config.SIMPLEBUS_EVENT_DESTINATION

        message_def.update(self.__config.SIMPLEBUS_MESSAGES.get(message_name, {}))
        message_def.update(options)

        self.__messages_by_cls[message_cls] = message_def
        self.__messages_by_name[message_name] = message_def

        messages = self.__messages_by_destination.get(message_def.destination)
        if messages is None:
            messages = self.__messages_by_destination[message_def.destination] = []
        messages.append(message_def)

    def get_by_cls(self, cls):
        return self.__messages_by_cls.get(cls)

    def get_by_name(self, name):
        return self.__messages_by_name.get(name)

    def get_by_destination(self, destination):
        return self.__messages_by_destination.get(destination)
