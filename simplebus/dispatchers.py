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

"""
Dispatchers are used to process all messages received and
dispatch them to the message handlers.
"""


import logging

from .pipeline import PipelineStep
from .transports.base import TransportMessage
from .utils import create_random_id, get_transport


LOGGER = logging.getLogger(__name__)


class DispatchMessageStep(PipelineStep):
    id = 'DispatchMessage'

    def __init__(self, transports):
        self.__transports = transports

    def invoke(self, context, next_step):
        transport_message = TransportMessage(context.app_id,
                                             create_random_id(),
                                             context.content_type,
                                             context.content_encoding,
                                             context.message,
                                             context.options.get('expiration'))

        if context.headers:
            transport_message.headers.update(context.headers)

        transport = get_transport(self.__transports, context.options.get('endpoint'))

        if context.queue:
            transport.push(context.queue, transport_message, context.options)
        else:
            transport.publish(context.topic, transport_message, context.options)

        next_step()
