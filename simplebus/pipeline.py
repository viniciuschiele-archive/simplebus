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

from abc import ABCMeta, abstractmethod
from .errors import SimpleBusError


class Pipeline(object):
    def __init__(self):
        self.__started = False
        self.__steps = []

    @property
    def steps(self):
        return self.__steps.copy()

    def start(self):
        self.__started = True

    def stop(self):
        self.__started = False

    def add_step(self, step, before_step_id=None, after_step_id=None):
        if self.__started:
            raise SimpleBusError('Step cannot be added when the pipeline is started.')

        if not step.id:
            raise SimpleBusError('Step id must have a non empty value.')

        if before_step_id:
            for i in range(len(self.__steps)):
                if self.__steps[i].id == before_step_id:
                    self.__steps.insert(i, step)
                    return
            raise SimpleBusError('Step %s not found.' % before_step_id)

        if after_step_id:
            for i in range(len(self.__steps)):
                if self.__steps[i].id == after_step_id:
                    self.__steps.insert(i + 1, step)
                    return
            raise SimpleBusError('Step %s not found.' % after_step_id)

        self.__steps.append(step)

    def remove_step(self, step_id):
        for i in range(len(self.__steps)):
            if self.__steps[i].id == step_id:
                self.__steps.pop(i)

    def execute(self, context):
        if not self.__started:
            raise SimpleBusError('Pipeline cannot be executed when it is stopped.')

        self.__execute_step(context, 0)

    def __execute_step(self, context, step_index):
        if step_index < len(self.__steps):
            self.__steps[step_index].execute(context, lambda: self.__execute_step(context, step_index+1))


class PipelineStep(metaclass=ABCMeta):
    id = None

    @abstractmethod
    def execute(self, context, next_step):
        pass


class PipelineContext(object):
    def __getattr__(self, item):
        return self.__dict__.get(item)


class IncomingContext(PipelineContext):
    def __init__(self, transport_message, callback, options):
        self.transport_message = transport_message
        self.callback = callback
        self.options = options

        self.headers = transport_message.headers
        self.content_type = transport_message.content_type
        self.content_encoding = transport_message.content_encoding
        self.body = transport_message.body


class OutgoingContext(PipelineContext):
    def __init__(self, destination, publishing, body, options):
        self.destination = destination
        self.publishing = publishing
        self.body = body
        self.options = options
        self.headers = {}
