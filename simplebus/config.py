# Copyright 2015 Vinicius Chiele. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Implements the configuration related objects."""


class Config(object):
    """
    SimpleBus configuration object. Default values are defined as
    class attributes.
    """

    # Default endpoints.
    SIMPLEBUS_ENDPOINTS = {'default': 'amqp://guest:guest@localhost'}

    # Default options for the messages.
    SIMPLEBUS_MESSAGE_ENDPOINT = 'default'
    SIMPLEBUS_MESSAGE_CONCURRENCY = 1
    SIMPLEBUS_MESSAGE_PREFETCH_COUNT = 10
    SIMPLEBUS_MESSAGE_COMPRESSOR = None
    SIMPLEBUS_MESSAGE_EXPIRES = None
    SIMPLEBUS_MESSAGE_SERIALIZER = 'json'
    SIMPLEBUS_MESSAGE_ERROR_QUEUE = 'simplebus.errors'
    SIMPLEBUS_COMMAND_DESTINATION = 'simplebus.commands'
    SIMPLEBUS_EVENT_DESTINATION = 'simplebus.events'

    # Options per message.
    SIMPLEBUS_MESSAGES = {}

    # Subscribes the messages when the bus starts.
    SIMPLEBUS_AUTO_SUBSCRIBE = True

    # Purge the messages before subscribe it.
    SIMPLEBUS_PURGE_ON_SUBSCRIBE = False

    #: A sequence of modules to import when the bus starts.
    SIMPLEBUS_IMPORTS = []

    def from_object(self, obj):
        """Loads the values from an object."""

        for key in dir(obj):
            if key.isupper() and key.startswith('SIMPLEBUS_'):
                setattr(self, key, getattr(obj, key))
