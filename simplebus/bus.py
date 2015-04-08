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

from simplebus.config import Config
from simplebus.transports import create_transport
from simplebus.transports.core import Message
from simplebus.utils import has_self


class Bus(object):
    def __init__(self):
        self.__transport = None
        self.__consumers_by_queue = {}
        self.config = Config()

    def push(self, queue, content, delivery_mode=None, expiration=None):
        self.__ensure_transport()

        message = Message(content, delivery_mode, expiration)
        self.__transport.push(queue, message)

    def pull(self, queue, callback):
        self.__ensure_transport()

        use_self = has_self(callback)

        def on_message(message):
            try:
                if use_self:
                    callback(self, message.body)
                else:
                    callback(message.body)
                message.complete()
            except Exception:
                message.reject()

        return self.__transport.pull(queue, on_message)

    def shutdown(self):
        if self.__transport:
            self.__transport.close()
            self.__transport = None

    def __ensure_transport(self):
        if self.__transport:
            return

        self.__transport = create_transport(self.config.BROKER_URL)
