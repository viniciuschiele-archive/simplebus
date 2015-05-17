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

from simplebus import Bus
from simplebus.compression import Compressor


class CustomCompression(Compressor):
    @property
    def content_type(self):
        return 'application/custom'

    def compress(self, body):
        # your magic here
        pass

    def decompress(self, body):
        # your magic here
        pass


class Config(object):
    SIMPLEBUS_COMPRESSIONS = {
        'custom': CustomCompression()
    }


if __name__ == '__main__':
    bus = Bus(app_id='custom_compression')
    bus.config.from_object(Config())

    message = {
        'property1': 'value1',
        'property2': 2
    }

    bus.push('simple_queue', message, compression='custom')
