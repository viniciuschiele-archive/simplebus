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

from .errors import MaxRetriesExceeded
from .pipeline import PipelineStep


class MoveFaultsToDeadLetterStep(PipelineStep):
    id = 'MoveFaultsToDeadLetter'

    def execute(self, context, next_step):
        try:
            next_step()
        except AssertionError:
            raise
        except Exception as e:
            context.transport_message.dead_letter(str(e))


class RetryFaultsStep(PipelineStep):
    id = 'RetryFaults'

    def execute(self, context, next_step):
        try:
            next_step()
        except AssertionError:
            raise
        except Exception as e:
            try:
                context.transport_message.retry()
            except MaxRetriesExceeded:
                raise e
