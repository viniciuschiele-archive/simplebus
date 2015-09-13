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

"""Unit tests."""

from simplebus import Config
from unittest import TestCase


class TestConfig(TestCase):
    def test_default(self):
        config = Config()
        self.assertEqual('amqp://guest:guest@localhost', config.SIMPLEBUS_ENDPOINTS.get('default'))

    def test_from_object(self):
        config = Config()
        config.from_object(self.CustomConfig())
        self.assertEqual('amqp://test:test@localhost', config.SIMPLEBUS_ENDPOINTS.get('default'))

    class CustomConfig(object):
        SIMPLEBUS_ENDPOINTS = {
            'default': 'amqp://test:test@localhost'
        }
