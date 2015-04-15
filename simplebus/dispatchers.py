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

import simplejson

from simplebus.state import set_transport_message
from simplebus.transports.core import MessageDispatcher


class DefaultDispatcher(MessageDispatcher):
    def __init__(self, consumer):
        self.__consumer = consumer

    def dispatch(self, transport_message):
        if transport_message.delivery_count > self.__consumer.max_delivery_count:
            transport_message.complete()

        content = simplejson.loads(transport_message.body)

        set_transport_message(transport_message)

        try:
            self.__consumer.handle(content)
            transport_message.complete()
        except:
            transport_message.defer()

        set_transport_message(None)

