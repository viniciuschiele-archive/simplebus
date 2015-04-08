from distutils.core import setup

setup(
    name='simplebus',
    version='0.3.0',
    packages=[
        'simplebus',
        'simplebus.transports'],
    url='https://github.com/viniciuschiele/simplebus',
    license='Apache 2.0',
    author='Vinicius Chiele',
    author_email='vinicius.chiele@gmail.com',
    description='A simple message bus for python',
    install_requires=['amqp-storm']
)
