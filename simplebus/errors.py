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

"""This module contains all exceptions used by the SimpleBus API."""


class SimpleBusError(Exception):
    pass


class NoRetryError(SimpleBusError):
    """Notifies to the Bus to do not retry the current message."""
    pass


class SerializationError(SimpleBusError):
    """Serialize or deserialize has failed."""
    pass


class SerializerNotFoundError(SimpleBusError):
    """Serializer not found."""
    pass


class CompressionError(SimpleBusError):
    """Compress or decompress has failed."""
    pass


class CompressionNotFoundError(SimpleBusError):
    """Compression not found."""
    pass


class LimitedExceeded(SimpleBusError):
    """Resources were exceeded."""
    pass


class MaxRetriesExceeded(SimpleBusError):
    """Maximum of retries exceeded."""
    pass
