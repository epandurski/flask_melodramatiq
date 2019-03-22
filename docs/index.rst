.. Flask-Melodramatiq documentation master file, created by
   sphinx-quickstart on Wed Mar 20 16:06:49 2019.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to Flask-Melodramatiq's documentation!
==============================================

Release v\ |release|. (:doc:`installation`, :doc:`api`, `Source Code`_)

.. _Source Code: https://github.com/epandurski/flask_melodramatiq

.. image:: https://img.shields.io/badge/License-MIT-yellow.svg
   :target: https://opensource.org/licenses/MIT


**Flask-Melodramatiq** is a Flask extension that adds support for the
`dramatiq` task processing library.

`dramatiq` is a great library, and Flask-Melodramatiq tries hard not
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
a "lazy broker") is a genuine `dramatiq` broker, and can be used
anywhere where a "native" broker can be used. (It has
:class:`~dramatiq.brokers.rabbitmq.RabbitmqBroker` as a superclass!)
Lazy brokers are thin wrappers which add several important features:

1. They honor the settings in the Flask application configuration.

2. :meth:`init_app` can be called on them *before or after* the
   actors have been defined.

3. The Flask application context is correctly set during the execution
   of the tasks.

4. They add few convenience methods (see `api`).


Configuration
-------------

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

If the configuration values passed to the constructor are different
from the ones set in the app config, the later take precedence. You
can even set/override the type of the broker in the app config::

  from flask_melodramatiq import Broker

  broker = Broker()  # Broker's type is not specified here.

and instead, specify the type in the app config::

   DRAMATIQ_BROKER_CLASS = 'StubBroker'

This feature can be quite useful when testing your code.


Starting workers
----------------

With Flask-Melodramatiq you have the whole power of dramatiq's CLI on
on your disposal. For example, to start worker processes for your
broker instance you may run::

  $ dramatiq wsgi:broker

and in ``wsgi.py`` you may have something like this::

   from myapp import create_app, broker

   app = create_app()

You may have as many broker instances as you want, but you need to
start each one of them with a separate command.


Contents:

.. toctree::
   :maxdepth: 2

   installation
   api
   Source Code <https://github.com/epandurski/flask_melodramatiq>
