from .errors import MaxRetriesExceeded
from .pipeline import PipelineStep


class RetryMessageStep(PipelineStep):
    id = 'RetryStep'

    def invoke(self, context, next_step):
        try:
            next_step()
        except Exception as e:
            try:
                context.transport_message.retry()
            except MaxRetriesExceeded:
                context.transport_message.dead_letter(str(e))
