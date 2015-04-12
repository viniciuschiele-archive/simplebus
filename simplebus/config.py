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
    class attributes. Additional attributes may be added by extensions.
    """

    DEFAULT_ENDPOINTS = {'default': 'amqp://guest:guest@localhost/'}

    def __init__(self):
        self.endpoints = self.DEFAULT_ENDPOINTS.copy()

    def from_object(self, obj):
        """Load values from an object."""
        if not hasattr(obj, 'SIMPLEBUS'):
            raise RuntimeError('Configuration object must have SIMPLEBUS attribute.')

        config = getattr(obj, 'SIMPLEBUS')

        self.endpoints = config.get('endpoints')

        if not self.endpoints:
            raise RuntimeError('Configuration object must have at least one endpoint')
