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
from simplebus.serialization import Serializer


class CustomSerializer(Serializer):
    @property
    def content_type(self):
        return 'application/custom'

    @property
    def content_encoding(self):
        return 'utf-8'

    def serialize(self, message):
        # your magic here
        pass

    def deserialize(self, buffer):
        # your magic here
        pass


bus = Bus(app_id='custom_serializer')
bus.config.SIMPLEBUS_SERIALIZERS['custom'] = CustomSerializer()


def send():
    message = {
        'property1': 'value1',
        'property2': 2
    }

    bus.push('simple_queue', message, serializer='custom')


if __name__ == '__main__':
    send()
