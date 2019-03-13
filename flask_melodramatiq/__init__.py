import threading
import importlib
import functools
import dramatiq
from dramatiq.brokers import stub


__all__ = ['LazyActor', 'RabbitmqBroker', 'RedisBroker', 'StubBroker']


def _raise_error(e, *args, **kwargs):
    raise e


def _create_broker(module_name, class_name, default_url='', broker_factory=None):
    try:
        module = importlib.import_module(module_name)
    except ImportError as e:
        return type(class_name, (), dict(
            __init__=functools.partial(_raise_error, e),
        ))
    class_ = getattr(module, class_name)
    return type(class_name, (_LazyBrokerMixin, class_), dict(
        _LazyBrokerMixin__broker_default_url=default_url,
        _LazyBrokerMixin__broker_factory=staticmethod(broker_factory) if broker_factory else class_,
    ))


class _ProxiedInstanceMixin:
    """Delegates attribute access to a lazily created instance.

    The lazily created instance is held in `self._proxied_instance`.
    To avoid infinite recursion, the `clear_proxied_instance()` method
    should be the first thing called in the constructor. Setting
    `self._proxied_instance` to anything different than `None`
    prevents any further attribute assignments on `self` (which will
    go to the proxied instance instead).

    """

    def clear_proxied_instance(self):
        object.__setattr__(self, '_proxied_instance', None)

    def __str__(self):
        if self._proxied_instance is None:
            return object.__str__(self)
        return str(self._proxied_instance)

    def __repr__(self):
        if self._proxied_instance:
            return repr(self._proxied_instance)
        return object.__repr__(self)

    def __getattr__(self, name):
        if self._proxied_instance is None:
            raise RuntimeError('The init_app() method must be called on brokers before use.')
        return getattr(self._proxied_instance, name)

    def __setattr__(self, name, value):
        if self._proxied_instance is None:
            return object.__setattr__(self, name, value)
        return setattr(self._proxied_instance, name, value)

    def __delattr__(self, name):
        if self._proxied_instance is None:
            return object.__delattr__(self, name)
        return delattr(self._proxied_instance, name)


class _LazyBrokerMixin(_ProxiedInstanceMixin):
    __registered_config_prefixes = set()

    def __init__(self, app=None, config_prefix='DRAMATIQ_BROKER', **options):
        self.clear_proxied_instance()
        self._unregistered_lazy_actors = []
        if config_prefix in self.__registered_config_prefixes:
            raise RuntimeError(
                'Can not create a second broker with config prefix "{}". '
                'Did you forget to pass the "config_prefix" argument when '
                'creating the broker?'.format(config_prefix)
            )
        self.__registered_config_prefixes.add(config_prefix)
        self.__config_prefix = config_prefix
        self.__options = options
        self.__broker_url = None
        self.__app = None
        self.__stub = stub.StubBroker(middleware=options.get('middleware'))
        if config_prefix == 'DRAMATIQ_BROKER':
            dramatiq.set_broker(self)
        if app is not None:
            self.init_app(app)

    def init_app(self, app):
        options = {'url': self.__read_url_from_config(app)}
        options.update(self.__options)
        broker_url = options['url']
        if self.__stub:
            self.__stub.close()
            self.__stub = None
            broker = self.__broker_factory(**options)
            broker.add_middleware(AppContextMiddleware(app))
            for actor in self._unregistered_lazy_actors:
                actor.register(broker=broker)
            self._unregistered_lazy_actors = None
            self.__broker_url = broker_url
            self.__app = app
            self._proxied_instance = broker  # `self` is sealed from now on.
        if broker_url != self.__broker_url:
            raise RuntimeError(
                '{app} tried to start a broker with '
                '{config_prefix}_URL={new_url}, '
                'but another app already has started that broker with '
                '{config_prefix}_URL={old_url}.'.format(
                    app=app,
                    config_prefix=self.__config_prefix,
                    new_url=broker_url,
                    old_url=self.__broker_url,
                )
            )
        if app is not self.__app:
            broker.add_middleware(MultipleAppsWarningMiddleware())
        if not hasattr(app, 'extensions'):
            app.extensions = {}
        app.extensions[self.__config_prefix.lower()] = self

    def actor(self, fn=None, **kw):
        for kwarg in ['broker', 'actor_class']:
            if kwarg in kw:
                raise TypeError("actor() got an unexpected keyword argument '{}'".format(kwarg))

        decorator = dramatiq.actor(actor_class=LazyActor, broker=self, **kw)
        if fn is None:
            return decorator
        return decorator(fn)

    @property
    def actor_options(self):
        return (self._proxied_instance or self.__stub).actor_options

    def __read_url_from_config(self, app):
        return (
            app.config.get('{0}_URL'.format(self.__config_prefix))
            or self.__broker_default_url
        )


class LazyActor(_ProxiedInstanceMixin, dramatiq.Actor):
    def __init__(self, fn, *, broker, **kw):
        self.clear_proxied_instance()
        self.__fn = fn
        self.__kw = kw
        if broker._unregistered_lazy_actors is None:
            self.register(broker)
        else:
            broker._unregistered_lazy_actors.append(self)

    def register(self, broker):
        self._proxied_instance = dramatiq.Actor(self.__fn, broker=broker, **self.__kw)

    def __call__(self, *args, **kwargs):
        if self._proxied_instance:
            return self._proxied_instance(*args, **kwargs)
        return self.__fn(*args, **kwargs)


class AppContextMiddleware(dramatiq.Middleware):
    state = threading.local()

    def __init__(self, app):
        self.app = app

    def before_process_message(self, broker, message):
        context = self.app.app_context()
        context.push()
        self.state.context = context

    def after_process_message(self, broker, message, *, result=None, exception=None):
        try:
            context = self.state.context
            context.pop(exception)
            del self.state.context
        except AttributeError:
            pass

    after_skip_message = after_process_message


class MultipleAppsWarningMiddleware(dramatiq.Middleware):
    def after_process_boot(self, broker):
        broker.logger.warning(
            "%s is used by more than one flask application. "
            "Actor's application context may be set incorrectly." % broker
        )


RabbitmqBroker = _create_broker(
    module_name='dramatiq.brokers.rabbitmq',
    class_name='RabbitmqBroker',
    default_url='amqp://127.0.0.1:5672',
)


RedisBroker = _create_broker(
    module_name='dramatiq.brokers.redis',
    class_name='RedisBroker',
    default_url='redis://127.0.0.1:6379/0',
)


StubBroker = _create_broker(
    module_name='dramatiq.brokers.stub',
    class_name='StubBroker',
    broker_factory=lambda url, **kw: stub.StubBroker(**kw),
)
