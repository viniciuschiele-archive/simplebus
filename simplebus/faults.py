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

from .pipeline import PipelineStep
from .transports import get_transport


class MoveFaultsToDeadLetterStep(PipelineStep):
    id = 'MoveFaultsToDeadLetter'

    def __init__(self, transports):
        self.__transports = transports

    def execute(self, context, next_step):
        try:
            next_step()
        except AssertionError:
            raise
        except Exception as e:
            error_queue = context.message_def.error_queue

            if not error_queue:
                raise

            transport_message = context.transport_message
            transport_message.headers['x-death-reason'] = str(e)
            transport_message.headers['x-failed-destination'] = context.message_def.destination
            transport_message.expiration = None

            transport = get_transport(self.__transports, context.message_def.endpoint)
            sender = transport.create_sender(error_queue)
            sender.dispatch(transport_message)
