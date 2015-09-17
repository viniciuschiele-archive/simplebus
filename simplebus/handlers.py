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

"""
Message Handler is used to process a message received.
"""

from .errors import SimpleBusError
from .pipeline import PipelineStep


class InvokeHandlerStep(PipelineStep):
    id = 'InvokeHandler'

    def __init__(self, handlers):
        self.__handlers = handlers

    def execute(self, context, next_step):
        handler = self.__handlers.get(context.message_def.message_cls)

        if not handler:
            raise SimpleBusError('No handler found to the message \'%s\'.' %
                                 str(type(context.message_def.message_cls)))

        handler(context.body)
        next_step()
