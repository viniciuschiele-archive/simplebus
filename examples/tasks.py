from simplebus import SimpleBus

bus = SimpleBus(app_id='tasks')


@bus.command(destination='tasks', concurrency=4, error_queue='tasks.error')
class ConvertAudioCommand(object):
    def __init__(self, audio_id):
        self.audio_id = audio_id


@bus.event(destination='tasks.converted', concurrency=4)
class AudioConvertedEvent(object):
    def __init__(self, audio_id):
        self.audio_id = audio_id


@bus.handle(ConvertAudioCommand)
def convert_audio(command):
    bus.publish(AudioConvertedEvent(command.audio_id))


@bus.handle(AudioConvertedEvent)
def audio_converted(event):
    print('Audio converted: ' + str(event.audio_id))

if __name__ == '__main__':
    bus.publish(ConvertAudioCommand(1))
    bus.loop.start()
