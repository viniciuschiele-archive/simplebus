class Pipeline(object):
    def __init__(self):
        self.steps = []

    def invoke(self, context):
        for step in self.steps:
            step.invoke(context)


class PipelineStep(object):
    def invoke(self, context):
        pass


class PipelineContext(object):
    def __getattr__(self, item):
        return self.__dict__.get(item)
