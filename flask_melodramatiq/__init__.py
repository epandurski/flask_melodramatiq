import importlib
import functools
import dramatiq
from flask_melodramatiq.lazy_broker import (
    LAZY_BROKER_DOCSTRING_TEMPLATE,
    register_broker_class,
    LazyActor,
    LazyBrokerMixin,
    Broker,
)

__all__ = ['create_broker_class', 'Broker', 'RabbitmqBroker', 'RedisBroker', 'StubBroker']


def create_broker_class(classpath, *, classname=None, docstring=None, mixins=()):
    """Create a new lazy broker class that wraps an existing broker class.

    :param classpath: A module path to the existing broker class. For
      example: ``"dramatiq.brokers.rabbitmq:RabbitmqBroker"``.

    :param classname: Optional name for the new class. If not given,
      the class name specified in **classpath** will be used.

    :param docstring: Optional documentation string for the new class

    :param mixins: Optional additional mix-in classes
    :type mixins: tuple(type)

    :return: The created lazy broker class

    Example::

      from flask_melodramatiq import create_broker_class

      PostgresBroker = create_broker_class('dramatiq_pg:PostgresBroker')

    """

    modname, varname = classpath.split(':', maxsplit=1)
    classname = classname or varname
    try:
        module = importlib.import_module(modname)
    except ImportError as e:
        # We will raise this exact import error when the class is
        # instantiated by the user.
        raise_import_error = functools.partial(raise_error, e)
        broker_class = type(classname, (Broker,), dict(
            __init__=raise_import_error,
            __doc__=docstring,
            _dramatiq_broker_factory=raise_import_error,
        ))
    else:
        superclass = getattr(module, varname)
        broker_class = type(classname, mixins + (LazyBrokerMixin, superclass), dict(
            __doc__=docstring,
            _dramatiq_broker_factory=superclass,
        ))
    register_broker_class(broker_class)
    return broker_class


def raise_error(e, *args, **kwargs):
    raise e


# We change the default actor class used by the `dramatiq.actor`
# decorator to `LazyActor`. This should be safe because for regular
# brokers and "init_app"-ed lazy brokers `LazyActor` behaves exactly
# as `dramatiq.Actor`.
dramatiq.actor.__kwdefaults__['actor_class'] = LazyActor


class RabbitmqBrokerMixin:
    def publish(self, message, *, exchange=''):
        """Publishes a message on an exchange.

        :param message: The message. The routing key will be
          ``f"dramatiq.events.{message.actor_name}"`` if
          ``message.queue_name`` is `None`, and ``message.queue_name``
          otherwise.

        :type message: dramatiq.Message

        :param exchange: The name of the exchange on which to publish
          the message

        """

        import pika

        if message.queue_name is None:
            routing_key = 'dramatiq.events.' + message.actor_name
        else:
            routing_key = message.queue_name

        properties = pika.BasicProperties(
            delivery_mode=2,
            priority=message.options.get("broker_priority"),
        )

        attempts = 1
        while True:
            try:
                self.channel.publish(
                    exchange=exchange,
                    routing_key=routing_key,
                    body=message.encode(),
                    properties=properties,
                )
                return

            except (pika.exceptions.AMQPConnectionError,
                    pika.exceptions.AMQPChannelError) as e:
                del self.channel
                del self.connection

                attempts += 1
                if attempts > 6:
                    raise dramatiq.ConnectionClosed(e) from None


RabbitmqBroker = create_broker_class(
    classpath='dramatiq.brokers.rabbitmq:RabbitmqBroker',
    docstring=LAZY_BROKER_DOCSTRING_TEMPLATE.format(
        description='A lazy broker wrapping a :class:`~dramatiq.brokers.rabbitmq.RabbitmqBroker`.\n',
    ),
    mixins=(RabbitmqBrokerMixin,)
)


RedisBroker = create_broker_class(
    classpath='dramatiq.brokers.redis:RedisBroker',
    docstring=LAZY_BROKER_DOCSTRING_TEMPLATE.format(
        description='A lazy broker wrapping a :class:`~dramatiq.brokers.redis.RedisBroker`.\n',
    ),
)


StubBroker = create_broker_class(
    classpath='dramatiq.brokers.stub:StubBroker',
    docstring=LAZY_BROKER_DOCSTRING_TEMPLATE.format(
        description='A lazy broker wrapping a :class:`~dramatiq.brokers.stub.StubBroker`.\n',
    ),
)
