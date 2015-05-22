=================================
SimpleBus
=================================
SimpleBus is a messaging library for Python 3. It has been designed to be simple and easy to use.
SimpleBus is still under heavy development so next versions might break compatibility.

|Version| |Downloads| |License|

Features
===============
- Auto recovery whether connection is dropped.
- Multiple transports, amqp implemented so far.
- Channel pooling for amqp transport.
- Retry logic for all messages received, it supports delay between retries.
- Concurrent threads receiving messages from a specified queue.
- Dead letter, messages that fail all retires are send to an dead letter queue.
- Custom serialization, json and msgpack implemented so far.
- Message compression, gzip implemented so far.
- ... more coming

Documentation
===============
Soon... take a look at the examples_ to see how it works.

Installation
===============
You can install SimpleBus via Python Package Index (PyPi_),::

    $ pip install simplebus

To use AMQP transport you need to install the amqp-storm_ library,::

    $ pip install amqp-storm

Feedback
===============
Please use the Issues_ for feature requests and troubleshooting usage.

.. |Version| image:: https://badge.fury.io/py/simplebus.svg?
   :target: http://badge.fury.io/py/simplebus

.. |Downloads| image:: https://pypip.in/d/simplebus/badge.svg?
   :target: https://pypi.python.org/pypi/simplebus
   
.. |License| image:: https://pypip.in/license/simplebus/badge.svg?
   :target: https://github.com/viniciuschiele/simplebus/blob/master/LICENSE

.. _amqp-storm: https://github.com/eandersson/amqp-storm

.. _examples: https://github.com/viniciuschiele/simplebus/tree/master/examples

.. _PyPi: https://pypi.python.org/pypi/simplebus

.. _Issues: https://github.com/viniciuschiele/simplebus/issues
