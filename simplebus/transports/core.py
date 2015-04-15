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


class Transport(object):
    abstract = True

    def open(self):
        raise NotImplementedError

    def close(self):
        raise NotImplementedError

    def create_message(self):
        raise NotImplementedError

    def send(self, queue, message):
        raise NotImplementedError

    def publish(self, topic, message):
        raise NotImplementedError

    def consume(self, queue, dispatcher):
        raise NotImplementedError

    def subscribe(self, topic, dispatcher):
        raise NotImplementedError


class Message(object):
    abstract = True

    @property
    def id(self):
        raise NotImplementedError

    @id.setter
    def id(self, value):
        raise NotImplementedError

    @property
    def body(self):
        raise NotImplementedError

    @body.setter
    def body(self, value):
        raise NotImplementedError

    @property
    def delivery_count(self):
        raise NotImplementedError

    @delivery_count.setter
    def delivery_count(self, value):
        raise NotImplementedError

    @property
    def expires(self):
        raise NotImplementedError

    @expires.setter
    def expires(self, value):
        raise NotImplementedError

    def complete(self):
        raise NotImplementedError

    def defer(self):
        raise NotImplementedError


class Cancellable(object):
    abstract = True

    def cancel(self):
        raise NotImplementedError


class Dispatcher(object):
    abstract = True

    def dispatch(self, message):
        raise NotImplementedError

