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


class Config(dict):
    """
    SimpleBus configuration object. Default values are defined as
    class attributes. Additional attributes may be added by extensions.
    """

    BROKER_URL = 'amqp://guest:guest@localhost/'

    def from_object(self, obj):
        """Load values from an object."""
        for key in dir(obj):
            if key.isupper():
                value = getattr(obj, key)
                self.__setattr(key, value)

    def __setattr(self, key, value):
        """Sets an attribute if it exists, otherwise raises an exception"""
        if not hasattr(self.__class__, key):
            raise ValueError("Unknown config key: %s" % key)
        setattr(self, key, value)
