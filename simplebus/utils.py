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

import inspect
import sys


def import_string(import_name):
    # force the import name to automatically convert to strings
    # __import__ is not able to handle unicode strings in the fromlist
    # if the module is a package
    import_name = str(import_name).replace(':', '.')
    try:
        __import__(import_name)
    except ImportError:
        if '.' not in import_name:
            raise
    else:
        return sys.modules[import_name]

    module_name, obj_name = import_name.rsplit('.', 1)
    try:
        module = __import__(module_name, None, None, [obj_name])
    except ImportError:
        # support importing modules not yet set up by the parent module
        # (or package for that matter)
        module = import_string(module_name)

    try:
        return getattr(module, obj_name)
    except AttributeError as e:
        raise ImportError(e)


def has_self(func):
    args = inspect.getargspec(func)

    if len(args) == 0:
        return False

    return args[0] == 'self'


class EventHandler(object):
    """A simple event handling class, which manages callbacks to be
    executed.
    """
    def __init__(self):
        self.callbacks = []

    def __call__(self, *args):
        """Executes all callbacks.

        Executes all connected callbacks in the order of addition,
        passing the sender of the EventHandler as first argument and the
        optional args as second, third, ... argument to them.
        """
        return [callback(*args) for callback in self.callbacks]

    def __iadd__(self, callback):
        """Adds a callback to the EventHandler."""
        self.add(callback)
        return self

    def __isub__(self, callback):
        """Removes a callback from the EventHandler."""
        self.remove(callback)
        return self

    def __len__(self):
        """Gets the amount of callbacks connected to the EventHandler."""
        return len(self.callbacks)

    def __getitem__(self, index):
        return self.callbacks[index]

    def __setitem__(self, index, value):
        self.callbacks[index] = value

    def __delitem__(self, index):
        del self.callbacks[index]

    def add(self, callback):
        """Adds a callback to the EventHandler."""
        if not callable(callback):
            raise TypeError("callback mus be callable")
        self.callbacks.append(callback)

    def remove(self, callback):
        """Removes a callback from the EventHandler."""
        self.callbacks.remove(callback)