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

import threading

from abc import ABCMeta
from abc import abstractmethod
from queue import Empty
from queue import LifoQueue
from .errors import LimitedExceeded

class ResourcePool(metaclass=ABCMeta):
    """Base class that provides a pool of resources."""

    def __init__(self, min_size, max_size=None):
        if not isinstance(min_size, int):
            raise TypeError('min_size should be int.')

        if max_size and not isinstance(max_size, int):
            raise TypeError('max_size should be int.')

        if min_size < 0:
            raise ValueError('min_size should be greater than 0.')

        if max_size and max_size < min_size:
            raise ValueError('max_size should be greater or equal to min_size.')

        self.__min_size = min_size
        self.__max_size = max_size or -1
        self.__size = 0
        self.__size_lock = threading.Lock()
        self.__closed = False
        self.__pool = LifoQueue()

    @property
    def max_size(self):
        """Gets the maximum number of resources."""
        return self.__max_size

    @property
    def min_size(self):
        """Gets the minimum number of resources."""
        return self.__min_size

    def acquire(self):
        """Acquires a resource."""

        if self.__closed:
            raise RuntimeError('Acquire on closed pool')

        try:
            return self.__pool.get_nowait()
        except Empty:
            if not self.__inc_size():
                raise LimitedExceeded('Limit of channels exceeded.')

            try:
                return self._create_resource()
            except:
                self.__dec_size()
                raise

    def release(self, resource):
        """Release resource so it can be used by another thread."""

        if self.__size > self.__min_size:
            self.__dec_size()
            self._close_resource(resource)
        else:
            if self._validate_resource(resource):
                self.__pool.put_nowait(resource)
            else:
                self.__dec_size()
                self._close_resource(resource)

    def close(self):
        """Closes and removes all resources in the pool."""

        self.__closed = True

        while 1:
            try:
                resource = self.__pool.get_nowait()
                self._close_resource(resource)
            except Empty:
                break

        self.__size = 0

    def __inc_size(self):
        """Increments the number of resources alive."""

        if self.__max_size == -1:
            self.__size += 1
            return True
        with self.__size_lock:
            if self.__size < self.__max_size:
                self.__size += 1
                return True
            else:
                return False

    def __dec_size(self):
        """Decrements the number of resources alive."""

        if self.__max_size == -1:
            self.__size -= 1
            return True
        with self.__size_lock:
            self.__size -= 1
            return True

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
