Flask-Melodramatiq
==================

**Flask-Melodramatiq** is a Flask extension that adds support for the
`dramatiq`_ task processing library.

`dramatiq`_ is a great library, and Flask-Melodramatiq tries hard not
to force you to change the way you interact with
it. Flask-Melodramatiq defines thin wrappers around the broker types
available in dramatiq, so that all the power of dramatiq's API remains
available to you.

For example::

  import dramatiq
  from flask import Flask
  from flask_melodramatiq import RabbitmqBroker

  app = Flask(__name__)
  broker = RabbitmqBroker(app)
  dramatiq.set_broker(broker)

  @dramatiq.actor
  def task():
      print('Snakes appreciate good theatrical preformace.')

or, if you prefer the Flask application factory pattern::

  import dramatiq
  from flask import Flask
  from flask_melodramatiq import RabbitmqBroker

  broker = RabbitmqBroker()
  dramatiq.set_broker(broker)

  @dramatiq.actor
  def task():
      print('Snakes appreciate good theatrical preformace.')

  def create_app():
      app = Flask(__name__)
      broker.init_app(app)
      return app

In those examples, the ``broker`` instance that we created (we call it
a "lazy broker") is a genuine `dramatiq`_ broker, and can be used
anywhere where a "native" broker can be used. (It has
``dramatiq.brokers.rabbitmq.RabbitmqBroker`` as a superclass!)  Lazy
brokers are thin wrappers which add several important features:

1. They honor the settings in the Flask application configuration.

2. ``init_app`` can be called on them *before or after* the
   actors have been defined.

3. The Flask application context is correctly set during the execution
   of the tasks.

4. They add few convenience methods.

You can change the configuration options for your broker by passing
keyword arguments to the constructor, or by setting corresponding
values for the ``DRAMATIQ_BROKER_*`` set of keys in the app
config. For example, you can do either::

   from flask_melodramatiq import RabbitmqBroker

   broker = RabbitmqBroker(
       url='amqp://mybroker:5672', confirm_delivery=True)

or you can put this in your app config::

   DRAMATIQ_BROKER_URL = 'amqp://mybroker:5672'
   DRAMATIQ_BROKER_CONFIRM_DELIVERY = True

You can even instantiate a broker of dynamically configurable type::

  from flask_melodramatiq import Broker

  broker = Broker()  # Broker's type is not fixed

and then specify the type in the app config::

   DRAMATIQ_BROKER_CLASS = 'StubBroker'

This feature can be quite useful when testing your code.


You can read the docs `here`_.


.. _here: https://flask-melodramatiq.readthedocs.io/en/latest/
.. _dramatiq: https://github.com/Bogdanp/dramatiq
