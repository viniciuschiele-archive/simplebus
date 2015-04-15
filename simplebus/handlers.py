class MessageHandler(object):
    abstract = True

    def handle(self, message):
        raise NotImplementedError


class CallbackHandler(MessageHandler):
    def __init__(self, callback):
        self.__callback = callback

    def handle(self, message):
        self.__callback(message)