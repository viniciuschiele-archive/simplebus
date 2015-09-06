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

from .messages import is_command, get_message_name
from .pipeline import PipelineStep
from .transports import get_transport, TransportMessage
from .utils import create_random_id


LOGGER = logging.getLogger(__name__)


class DispatchMessageStep(PipelineStep):
    id = 'DispatchMessage'

    def __init__(self, app_id, transports):
        self.__app_id = app_id
        self.__transports = transports

    def execute(self, context, next_step):
        options = context.options
        transport_message = TransportMessage()
        transport_message.app_id = self.__app_id
        transport_message.message_id = create_random_id()
        transport_message.expiration = options.get('expires')
        transport_message.content_type = context.content_type
        transport_message.content_encoding = context.content_encoding
        transport_message.body = context.body
        transport_message.type = get_message_name(context.message_cls)

        address = options.get('address')

        transport = get_transport(self.__transports, options.get('endpoint'))

        if is_command(context.message_cls):
            dispatcher = transport.create_sender(address)
        else:
            dispatcher = transport.create_publisher(address)

        dispatcher.dispatch(transport_message)

        next_step()
