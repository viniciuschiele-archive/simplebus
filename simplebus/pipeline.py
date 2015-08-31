from abc import ABCMeta, abstractmethod
from .errors import SimpleBusError


class Pipeline(object):
    def __init__(self):
        self.__started = False
        self.__steps = []

    @property
    def steps(self):
        return self.__steps.copy()

    def start(self):
        self.__started = True

    def stop(self):
        self.__started = False

    def add_step(self, step, before_step_id=None, after_step_id=None):
        if self.__started:
            raise SimpleBusError('Step cannot be added when the pipeline is started.')

        if not step.id:
            raise SimpleBusError('Step id must have a non empty value.')

        if before_step_id:
            for i in range(len(self.__steps)):
                if self.__steps[i].id == before_step_id:
                    self.__steps.insert(i, step)
                    return
            raise SimpleBusError('Step %s not found.' % before_step_id)

        if after_step_id:
            for i in range(len(self.__steps)):
                if self.__steps[i].id == after_step_id:
                    self.__steps.insert(i + 1, step)
                    return
            raise SimpleBusError('Step %s not found.' % after_step_id)

        self.__steps.append(step)

    def remove_step(self, step_id):
        for i in range(len(self.__steps)):
            if self.__steps[i].id == step_id:
                self.__steps.pop(i)

    def invoke(self, context):
        if not self.__started:
            raise SimpleBusError('Pipeline cannot be invoked when it is stopped.')

        self.__invoke_step(context, 0)

    def __invoke_step(self, context, index):
        if index < len(self.__steps):
            self.__steps[index].invoke(context, lambda: self.__invoke_step(context, index+1))


class PipelineStep(metaclass=ABCMeta):
    id = None

    @abstractmethod
    def invoke(self, context, next_step):
        pass


class PipelineContext(object):
    def __getattr__(self, item):
        return self.__dict__.get(item)
