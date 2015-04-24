from abc import ABCMeta
from abc import abstractmethod


class Cancellable(metaclass=ABCMeta):
    @abstractmethod
    def cancel(self):
        pass


class Cancellation(Cancellable):
    def __init__(self, id, transport):
        self.__id = id
        self.__transport = transport

    def cancel(self):
        self.__transport.cancel(self.__id)


class Subscription(Cancellable):
    def __init__(self, id, transport):
        self.__id = id
        self.__transport = transport

    def cancel(self):
        self.__transport.unsubscribe(self.__id)