import threading

from simplebus.local import Proxy

__bus = None
__local = threading.local()


def get_current_bus():
    return __bus


def get_current_message():
    if hasattr(__local, 'current_message'):
        return __local.current_message
    return None


def set_current_bus(bus):
    global __bus
    __bus = bus


def set_current_message(message):
    if message:
        __local.current_message = message
    else:
        del __local.current_message


current_bus = Proxy(get_current_bus)
current_message = Proxy(get_current_message)
