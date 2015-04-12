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


class Producer(object):
    abstract = True

    bus = None
    endpoint = None
    queue = None
    topic = None
    expires = None

    def publish(self, message):
        if not self.bus:
            raise RuntimeError('Producer must have a bus associated. Maybe you forgot to use @bus.producer')

        self.before_publish(message)

        if self.queue:
            self.bus.send(self.queue, message, self.expires, self.endpoint)

        if self.topic:
            self.bus.publish(self.topic, message, self.expires, self.endpoint)

        self.after_publish(message)

    def before_publish(self, message):
        pass

    def after_publish(self, message):
        pass