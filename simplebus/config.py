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

from simplebus.utils import ImmutableDict


class Config(object):
    """
    SimpleBus configuration object. Default values are defined as
    class attributes. Additional attributes may be added by extensions.
    """

    DEFAULT_QUEUE = ImmutableDict({
        'dead_letter_enabled': True,
        'expiration': None,
        'max_retry_count': 3,
        'retry_delay': 1000,
        'prefetch_count': 10,
        'endpoint': None
    })

    DEFAULT_TOPIC = ImmutableDict({
        'expiration': None,
        'prefetch_count': 10,
        'endpoint': None
    })

    SIMPLEBUS_ENDPOINTS = {'default': 'amqp://guest:guest@localhost/'}
    SIMPLEBUS_QUEUES = {}
    SIMPLEBUS_TOPICS = {}

    SIMPLEBUS_RECOVERY = True
    SIMPLEBUS_RECOVERY_DELAY = 3  # 3 seconds

    def __init__(self):
        self.__queues_cached = {}
        self.__topics_cached = {}

    def from_object(self, obj):
        """Load values from an object."""

        for key in dir(obj):
            if key.isupper() and key.startswith('SIMPLEBUS_'):
                value = getattr(obj, key)
                self.__setattr(key, value)

    def get_queue_options(self, queue, override_options):
        return self.__get_options(
            self.__queues_cached,
            queue,
            self.DEFAULT_QUEUE,
            override_options,
            self.SIMPLEBUS_QUEUES)

    def get_topic_options(self, topic, override_options):
        return self.__get_options(
            self.__topics_cached,
            topic,
            self.DEFAULT_TOPIC,
            override_options,
            self.SIMPLEBUS_TOPICS)

    @staticmethod
    def __get_options(cache, key, default_options, override_options, pool_options):
        options = cache.get(key)

        if not options:
            options = default_options.copy()
            options_form_config = pool_options.get('*')

            if options_form_config:
                for k, v in options_form_config.items():
                    options[k] = v

            options_form_config = pool_options.get(key)

            if options_form_config:
                for k, v in options_form_config.items():
                    options[k] = v

            cache[key] = options

        if override_options:
            options = options.copy()

            for k, v in override_options.items():
                options[k] = v

        return options

    def __setattr(self, key, value):
        if hasattr(self.__class__, key):
            setattr(self, key, value)
