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

import threading
import uuid

from importlib import import_module


def create_random_id():
    """Creates a random uuid without hyphens."""

    return str(uuid.uuid4()).replace('-', '')


def import_string(dotted_path):
    """Imports a class from the its full path."""

    try:
        module_path, class_name = dotted_path.rsplit('.', 1)
    except ValueError:
        msg = "%s doesn't look like a module path" % dotted_path
        raise LookupError(msg)

    module = import_module(module_path)

    try:
        return getattr(module, class_name)
    except AttributeError:
        msg = 'Module "%s" does not define a "%s" attribute/class' % (dotted_path, class_name)
        raise LookupError(msg)


class Loop(object):
    """A simple class that block the current thread."""
    def __init__(self):
        self.__event = threading.Event()

    def start(self):
        """Blocks the current thread."""
        self.__event.wait()

    def stop(self):
        """Releases the blocked thread."""
        self.__event.set()
