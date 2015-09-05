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

    def __init__(self, app_id, transports):
        self.__app_id = app_id
        self.__transports = transports

    def execute(self, context, next_step):
        transport_message = TransportMessage()
        transport_message.app_id = self.__app_id
        transport_message.message_id = create_random_id()
        transport_message.content_type = context.content_type
        transport_message.content_encoding = context.content_encoding
        transport_message.body = context.body
        transport_message.expiration = context.options.get('expiration')
        transport_message.headers.update(context.headers)
        context.transport_message = transport_message

        transport = get_transport(self.__transports, context.options.get('endpoint'))

        if context.publishing:
            transport.publish(context.destination, transport_message, context.options)
        else:
            transport.push(context.destination, transport_message, context.options)

        next_step()
