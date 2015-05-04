from abc import ABCMeta
from abc import abstractmethod


class Cancellable(metaclass=ABCMeta):
    """Interface that provides methods to cancel the receipt of messages."""

    @abstractmethod
    def cancel(self):
        """Cancels the receipt of messages."""
        pass


class Cancellation(Cancellable):
    """Class responsible for canceling the receipt of messages from queues."""

    def __init__(self, id, transport):
        self.__id = id
        self.__transport = transport

    def cancel(self):
        """Cancels the receipt of messages."""

        self.__transport.cancel(self.__id)


class Subscription(Cancellable):
    """Class responsible for canceling the receipt of messages from topics."""

    def __init__(self, id, transport):
        self.__id = id
        self.__transport = transport

    def cancel(self):
        """Cancels the receipt of messages."""

        self.__transport.unsubscribe(self.__id)