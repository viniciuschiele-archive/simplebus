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

import copy

from .utils import merge_dict


class Config(object):
    """
    SimpleBus configuration object. Default values are defined as
    class attributes.
    """

    #: Default endpoints.
    SIMPLEBUS_ENDPOINTS = {'default': 'amqp://guest:guest@localhost/'}

    #: Default options for the queues.
    #: '*' is the default configuration for all queues.
    SIMPLEBUS_QUEUES = {
        '*': {
            'endpoint': None,
            'dead_letter': True,
            'retry': False,
            'max_retries': 3,
            'retry_delay': 1000,
            'expiration': None,
            'max_concurrency': 1,
            'prefetch_count': 10,
            'compression': None,
            'serializer': 'json',
        }
    }

    #: Default options for the topics.
    #: '*' is the default configuration for all topics.
    SIMPLEBUS_TOPICS = {
        '*': {
            'endpoint': None,
            'expiration': None,
            'max_concurrency': 1,
            'prefetch_count': 10,
            'compression': None,
            'serializer': 'json',
        }
    }

    #: A sequence of modules to import when the bus starts.
    SIMPLEBUS_IMPORTS = []

    #: If enabled the bus will try to reconnect if the connection goes down.
    SIMPLEBUS_RECOVERY = True

    #: The minimum number of seconds to wait before retrying connect to the broker.
    SIMPLEBUS_RECOVERY_MIN_DELAY = 0

    #: The interval in seconds to use to exponentially increment the retry interval by.
    SIMPLEBUS_RECOVERY_DELTA_DELAY = 1

    #: The maximum number of seconds to wait before retrying connect to the broker.
    SIMPLEBUS_RECOVERY_MAX_DELAY = 8

    def from_object(self, obj):
        """Load values from an object."""

        for key in dir(obj):
            if key.isupper() and key.startswith('SIMPLEBUS_'):
                value = getattr(obj, key)
                self.__setattr(key, value)

    def __setattr(self, key, value):
        """Sets the value for the specified key whether it exists."""

        if key == 'SIMPLEBUS_QUEUES':
            self.SIMPLEBUS_QUEUES = copy.deepcopy(Config.SIMPLEBUS_QUEUES)
            merge_dict(self.SIMPLEBUS_QUEUES, value)
        elif key == 'SIMPLEBUS_TOPICS':
            self.SIMPLEBUS_TOPICS = copy.deepcopy(Config.SIMPLEBUS_TOPICS)
            merge_dict(self.SIMPLEBUS_TOPICS, value)
        elif hasattr(self.__class__, key):
            setattr(self, key, value)
