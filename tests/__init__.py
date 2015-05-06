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

from simplebus import Bus


def create_bus():
    bus = Bus('unittests')
    bus.config.from_object(BusConfig())
    return bus


class BusConfig(object):
    SIMPLEBUS_ENDPOINTS = {
        'default': 'amqp://guest:guest@localhost?channel_limit=5'
    }

    SIMPLEBUS_QUEUES = {
        '*': {
            'dead_letter_enabled': False,
            'max_retries': 0
        }
    }
