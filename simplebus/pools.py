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

"""Base classes to implement a pool of resources."""

from abc import ABCMeta
from abc import abstractmethod
from queue import Empty
from queue import LifoQueue


class ResourcePool(metaclass=ABCMeta):
    """Base class that provides a pool of resources."""

    def __init__(self, limit=None):
        self.limit = limit
        self.__closed = False
        self.__resources_in = LifoQueue()
        self.__resources_out = set()

    def acquire(self):
        """Acquires a resource."""

        if self.__closed:
            raise RuntimeError('Acquire on closed pool')

        if not self.limit:
            return self._create_resource()

        resource = None

        try:
            while True:
                resource = self.__resources_in.get_nowait()
                if self._validate_resource(resource):
                    break
        except Empty:
            resource = None

        if not resource:
            if self.limit and len(self.__resources_out) >= self.limit:
                raise RuntimeError('Limit of channels exceeded.')
            resource = self._create_resource()

        self.__resources_out.add(resource)
        return resource

    def release(self, resource):
        """Release resource so it can be used by another thread."""

        if self.limit:
            if self._validate_resource(resource):
                self.__resources_in.put_nowait(resource)
            else:
                self._close_resource(resource)
            self.__resources_out.discard(resource)
        else:
            self._close_resource(resource)

    def close(self):
        """Close and remove all resources in the pool (also those in use)."""

        self.__closed = True

        while 1:
            try:
                resource = self.__resources_out.pop()
            except KeyError:
                break
            try:
                self._close_resource(resource)
            except AttributeError:
                pass

        while 1:
            try:
                resource = self.__resources_in.queue.pop()
            except IndexError:
                break
            try:
                self._close_resource(resource)
            except AttributeError:
                pass

    @abstractmethod
    def _create_resource(self):
        """Creates a new resource."""
        pass

    @abstractmethod
    def _close_resource(self, resource):
        """Closes the specified resource."""
        pass

    @abstractmethod
    def _validate_resource(self, resource):
        """Validates whether resource is still valid to be used."""
        pass
