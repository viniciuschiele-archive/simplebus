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


def dict_to_message(message_cls, data):
    return message_cls.__dict_to_message__(data)


def message_to_dict(message):
    return type(message).__message_to_dict__(message)


def get_message_name(message_cls):
    return message_cls.__message_name__


def get_message_options(message_cls):
    return message_cls.__message_options__


def is_command(message_cls):
    return message_cls.__message_type__ == 0


def is_event(message_cls):
    return message_cls.__message_type__ != 0


def setup_message_class(message_cls, name, type, destination, error_queue, expires, concurrency, prefetch_count,
                        compressor, serializer, purge, endpoint):
    message_cls.__message_name__ = name or message_cls.__name__
    message_cls.__message_type__ = type
    message_cls.__message_options__ = {}
    message_cls.__dict_to_message__ = _generate_dict_to_message(message_cls)
    message_cls.__message_to_dict__ = _generate_message_to_dict(message_cls)

    if destination:
        message_cls.__message_options__['destination'] = destination

    if error_queue:
        message_cls.__message_options__['error_queue'] = error_queue

    if expires:
        message_cls.__message_options__['expires'] = expires

    if concurrency:
        message_cls.__message_options__['concurrency'] = concurrency

    if prefetch_count:
        message_cls.__message_options__['prefetch_count'] = prefetch_count

    if compressor:
        message_cls.__message_options__['compressor'] = compressor

    if serializer:
        message_cls.__message_options__['serializer'] = serializer

    if purge:
        message_cls.__message_options__['purge'] = purge

    if endpoint:
        message_cls.__message_options__['endpoint'] = endpoint


def _generate_dict_to_message(message_cls):
    def set_dict(data):
        message = message_cls()
        message.__dict__.update(data)

    args = inspect.getargspec(message_cls.__init__)[0]
    if len(args) > 1:
        return lambda data: message_cls(**data)
    elif hasattr(message_cls, '__dict__'):
        return set_dict

    raise TypeError('Message \'%s\' has no __init__ to be instantiated.' % str(message_cls))


def _generate_message_to_dict(message_cls):
    if hasattr(message_cls, 'as_dict'):
        return lambda message: message.as_dict()
    if isinstance(message_cls, dict):
        return lambda message: message
    if hasattr(message_cls, '__dict__'):
        return lambda message: message.__dict__
    raise TypeError('Message \'%s\' cannot be converted to dict' % str(message_cls))


class MessageRegistry(object):
    def __init__(self, config):
        self.__config = config
        self.__messages_by_destination = {}
        self.__messages_by_name = {}
        self.__message_options = {}

    def add(self, message_cls):
        name = get_message_name(message_cls)
        if self.__messages_by_name.get(name) is not None:
            raise SimpleBusError('Message \'%s\' already subscribed.' % str(message_cls))
        self.__messages_by_name[type] = message_cls

        options = self.get_options(message_cls)
        destination = options['destination']
        messages = self.__messages_by_destination.get(destination)
        if messages is None:
            messages = self.__messages_by_destination[destination] = []
        messages.append(message_cls)
        return options

    def get_by_destination(self, destination):
        return self.__messages_by_destination.get(destination)

    def get_by_name(self, name):
        return self.__messages_by_name.get(name)

    def get_options(self, message_cls):
        """Gets the options for the specified message class."""

        options = self.__message_options.get(message_cls)
        if options:
            return options

        options = {
            'endpoint': self.__config.SIMPLEBUS_MESSAGE_ENDPOINT,
            'error_queue': self.__config.SIMPLEBUS_MESSAGE_ERROR_QUEUE,
            'expires': self.__config.SIMPLEBUS_MESSAGE_EXPIRES,
            'concurrency': self.__config.SIMPLEBUS_MESSAGE_CONCURRENCY,
            'prefetch_count': self.__config.SIMPLEBUS_MESSAGE_PREFETCH_COUNT,
            'compressor': self.__config.SIMPLEBUS_MESSAGE_COMPRESSOR,
            'serializer': self.__config.SIMPLEBUS_MESSAGE_SERIALIZER,
            'purge': self.__config.SIMPLEBUS_PURGE_ON_STARTUP,
        }

        options2 = self.__config.SIMPLEBUS_MESSAGES.get(get_message_name(message_cls))

        if options2:
            options.update(options2)

        if is_command(message_cls):
            options['destination'] = self.__config.SIMPLEBUS_COMMAND_DESTINATION
        else:
            options['destination'] = self.__config.SIMPLEBUS_EVENT_DESTINATION

        options.update(get_message_options(message_cls))

        self.__message_options[message_cls] = options

        return options
