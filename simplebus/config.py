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
import multiprocessing

from simplebus.serialization import JsonSerializer
from simplebus.utils import merge_dict


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
            'retry': True,
            'max_retries': 3,
            'retry_delay': 0,
            'expiration': None,
            'max_concurrency': multiprocessing.cpu_count(),
            'prefetch_count': 10,
            'serializer': 'json',
        }
    }

    #: Default options for the topics.
    #: '*' is the default configuration for all topics.
    SIMPLEBUS_TOPICS = {
        '*': {
            'endpoint': None,
            'expiration': None,
            'max_concurrency': multiprocessing.cpu_count(),
            'prefetch_count': 10,
            'serializer': 'json',
        }
    }

    #: If enabled the bus will try to reconnect if the connection goes down.
    SIMPLEBUS_RECOVERY = True

    #: Number of seconds between retries of reconnecting.
    #: Default is 3 seconds.
    SIMPLEBUS_RECOVERY_DELAY = 3

    #: Serializers supported.
    SIMPLEBUS_SERIALIZERS = {
        'json': JsonSerializer()
    }

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
