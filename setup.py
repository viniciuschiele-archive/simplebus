from setuptools import setup

setup(
    name='simplebus',
    version='0.6.0',
    packages=[
        'simplebus',
        'simplebus.transports'],
    url='https://github.com/viniciuschiele/simplebus',
    license='Apache 2.0',
    author='Vinicius Chiele',
    author_email='vinicius.chiele@gmail.com',
    description='A simple message bus for python 3',
    keywords=['simplebus', 'messagebus', 'message bus',
              'messaging', 'queue', 'topic', 'pubsub'],
    install_requires=['simplejson>=3.6.5'],
    classifiers=[
          'Development Status :: 5 - Production/Stable',
          'Intended Audience :: Developers',
          'Natural Language :: English',
          'Operating System :: OS Independent',
          'Programming Language :: Python :: 3',
          'Programming Language :: Python :: 3.2',
          'Programming Language :: Python :: 3.3',
          'Programming Language :: Python :: 3.4',
          'Programming Language :: Python :: Implementation :: CPython',
          'Topic :: Communications',
          'Topic :: Internet',
          'Topic :: Software Development :: Libraries',
          'Topic :: Software Development :: Libraries :: Python Modules',
          'Topic :: System :: Networking']
)
