API Reference
=============

.. module:: flask_melodramatiq

.. autofunction:: create_broker_class

.. autoclass:: Broker
   :members: init_app, set_default, actor

.. autoclass:: RabbitmqBroker
   :members: init_app, set_default, actor, publish_message

.. autoclass:: RedisBroker
   :members: init_app, set_default, actor

.. autoclass:: StubBroker
   :members: init_app, set_default, actor
