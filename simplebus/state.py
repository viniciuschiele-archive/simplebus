import threading

from simplebus.local import Proxy

__local = threading.local()


def set_transport_message(transport_message):
    if transport_message:
        __local.transport_message = transport_message
    else:
        del __local.transport_message


def get_transport_message():
    if hasattr(__local, 'transport_message'):
        return __local.transport_message
    return None

transport_message = Proxy(get_transport_message)
