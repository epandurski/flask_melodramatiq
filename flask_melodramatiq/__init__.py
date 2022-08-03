import importlib
import functools
import dramatiq
from flask_melodramatiq.lazy_broker import (
    LAZY_BROKER_DOCSTRING_TEMPLATE,
    register_broker_class,
    LazyActor,
    LazyBrokerMixin,
    Broker,
    missing,
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
        broker_class = type(classname, mixins + (Broker,), dict(
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


RabbitmqBroker = create_broker_class(
    classpath='dramatiq.brokers.rabbitmq:RabbitmqBroker',
    docstring=LAZY_BROKER_DOCSTRING_TEMPLATE.format(
        description='A lazy broker wrapping a :class:`~dramatiq.brokers.rabbitmq.RabbitmqBroker`.\n',
    ),
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
