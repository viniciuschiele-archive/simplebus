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

from simplebus.errors import LimitedExceeded
from simplebus.pools import ResourcePool
from simplebus.transports import amqp

from unittest import TestCase


class TestResourcePool(TestCase):
    def test_no_limit(self):
        pool = self.StringPool()
        for i in range(100):
            pool.acquire()

    def test_min_size(self):
        pool = self.StringPool(min_size=2)
        self.assertEqual('string1', pool.acquire())
        self.assertEqual('string2', pool.acquire())

        pool.release('string1')

        self.assertEqual('string1', pool.acquire())
        self.assertEqual('string3', pool.acquire())

        pool.release('string1')

        self.assertEqual('string4', pool.acquire())

    def test_max_size(self):
        pool = self.StringPool(min_size=0, max_size=2)
        self.assertEqual('string1', pool.acquire())
        self.assertEqual('string2', pool.acquire())
        self.assertRaises(LimitedExceeded, pool.acquire)

        pool.release('string1')

        self.assertEqual('string3', pool.acquire())
        self.assertRaises(LimitedExceeded, pool.acquire)

        pool.release('string2')
        pool.release('string1')

        self.assertEqual('string4', pool.acquire())
        self.assertEqual('string5', pool.acquire())

    def test_validate(self):
        pool = self.StringPool(max_size=1, invalidate_all=True)
        self.assertEqual('string1', pool.acquire())
        pool.release('string1')
        self.assertEqual('string2', pool.acquire())

    def test_amqp_transport(self):
        transport = amqp.Transport('amqp://guest:guest@localhost')
        self.assertEqual(5, transport.min_channels)
        self.assertEqual(20, transport.max_channels)

        transport = amqp.Transport('amqp://guest:guest@localhost?min_channels=2&max_channels=10')
        self.assertEqual(2, transport.min_channels)
        self.assertEqual(10, transport.max_channels)

    class StringPool(ResourcePool):
        def __init__(self, min_size=None, max_size=None, invalidate_all=False):
            super().__init__(min_size or 0, max_size)
            self.__count = 0
            self.__invalidate_all = invalidate_all

        def _create_resource(self):
            self.__count += 1
            return str('string' + str(self.__count))

        def _close_resource(self, resource):
            pass

        def _validate_resource(self, resource):
            return not self.__invalidate_all
