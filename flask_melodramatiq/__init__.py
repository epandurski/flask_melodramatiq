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

__all__ = ['Broker', 'RabbitmqBroker', 'RedisBroker', 'StubBroker']


def create_broker_class(module_name, class_name, docstring=None):
    try:
        module = importlib.import_module(module_name)
    except ImportError as e:
        # We will raise this exact import error when the class is
        # instantiated by the user.
        raise_import_error = functools.partial(raise_error, e)
        broker_class = type(class_name, (Broker,), dict(
            __init__=raise_import_error,
            __doc__=docstring,
            _dramatiq_broker_factory=raise_import_error,
        ))
    else:
        superclass = getattr(module, class_name)
        broker_class = type(class_name, (LazyBrokerMixin, superclass), dict(
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
    module_name='dramatiq.brokers.rabbitmq',
    class_name='RabbitmqBroker',
    docstring=LAZY_BROKER_DOCSTRING_TEMPLATE.format(
        description='A lazy broker wrapping :class:`~dramatiq.brokers.rabbitmq.RabbitmqBroker`.\n',
    ),
)


RedisBroker = create_broker_class(
    module_name='dramatiq.brokers.redis',
    class_name='RedisBroker',
    docstring=LAZY_BROKER_DOCSTRING_TEMPLATE.format(
        description='A lazy broker wrapping :class:`~dramatiq.brokers.redis.RedisBroker`.\n',
    ),
)


StubBroker = create_broker_class(
    module_name='dramatiq.brokers.stub',
    class_name='StubBroker',
    docstring=LAZY_BROKER_DOCSTRING_TEMPLATE.format(
        description='A lazy broker wrapping :class:`~dramatiq.brokers.stub.StubBroker`.\n',
    ),
)
