class Pipeline(object):
    def __init__(self):
        self.steps = []

    def invoke(self, context):
        self.__invoke_step(context, 0)

    def __invoke_step(self, context, index):
        if index < len(self.steps):
            self.steps[index].invoke(context, lambda: self.__invoke_step(context, index+1))


class PipelineStep(object):
    def invoke(self, context, next_step):
        pass


class PipelineContext(object):
    def __getattr__(self, item):
        return self.__dict__.get(item)
